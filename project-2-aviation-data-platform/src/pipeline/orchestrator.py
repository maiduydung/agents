"""Main pipeline orchestrator — coordinates bronze, silver, gold layers."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from src.config import PlatformConfig, get_config
from src.observability.logger import get_logger
from src.observability.metrics import MetricsCollector
from src.pipeline.bronze import BronzeLayer
from src.pipeline.gold import GoldLayer
from src.pipeline.silver import SilverLayer
from src.quality.report import print_report
from src.quality.validator import QualityValidator

log = get_logger(__name__)


class PipelineOrchestrator:
    """Orchestrates the full medallion architecture pipeline."""

    def __init__(self, config: PlatformConfig | None = None) -> None:
        self._config = config or get_config()
        self._config.ensure_dirs()
        self._conn = duckdb.connect(self._config.db_path)
        self._metrics = MetricsCollector(self._conn)
        self._bronze = BronzeLayer(self._conn, self._config, self._metrics)
        self._silver = SilverLayer(self._conn, self._config, self._metrics)
        self._gold = GoldLayer(self._conn, self._config, self._metrics)
        self._validator = QualityValidator(self._conn)

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def run_bronze(self) -> dict[str, int]:
        """Run bronze layer ingestion."""
        log.info("starting_bronze_layer")
        counts = self._bronze.ingest_all()
        log.info("bronze_layer_complete", counts=counts)
        return counts

    def run_silver(self, batch_id: str | None = None) -> dict[str, dict[str, int]]:
        """Run silver layer processing."""
        bid = batch_id or self._bronze.batch_id
        log.info("starting_silver_layer", batch_id=bid)
        results = self._silver.process_all(bid)
        log.info("silver_layer_complete", results=results)
        return results

    def run_gold(self, batch_id: str | None = None) -> dict[str, int]:
        """Run gold layer transformations."""
        bid = batch_id or self._bronze.batch_id
        log.info("starting_gold_layer", batch_id=bid)
        counts = self._gold.build_all(bid)
        log.info("gold_layer_complete", counts=counts)
        return counts

    def run_full(self) -> dict[str, Any]:
        """Run the complete pipeline: bronze -> silver -> gold."""
        log.info("starting_full_pipeline")

        bronze_counts = self.run_bronze()
        batch_id = self._bronze.batch_id

        silver_results = self.run_silver(batch_id)
        gold_counts = self.run_gold(batch_id)

        # Print gold table stats
        gold_stats = self._gold.get_table_stats()
        log.info("gold_table_stats", stats=gold_stats)

        summary = {
            "batch_id": batch_id,
            "bronze": bronze_counts,
            "silver": silver_results,
            "gold": gold_counts,
            "gold_totals": gold_stats,
        }
        log.info("full_pipeline_complete", summary=summary)
        return summary

    def run_quality(self, layer: str | None = None) -> list:
        """Run data quality checks and print report."""
        log.info("starting_quality_checks", layer=layer or "all")
        results = self._validator.run_all(layer)
        print_report(results)
        return results

    def backfill(self, start_date: date, end_date: date) -> dict[str, Any]:
        """Idempotent backfill — regenerate and reprocess data for a date range.

        This demonstrates the backfill pattern: drop-and-reload within
        the batch scope, ensuring no duplicates.
        """
        from generators.aviation_data import AviationDataGenerator

        log.info("starting_backfill", start_date=start_date.isoformat(), end_date=end_date.isoformat())

        # Generate data for the date range
        generator = AviationDataGenerator(self._config)
        gen_counts = generator.generate_all(num_records=500, start_date=start_date, end_date=end_date)

        # Run the full pipeline on the newly generated data
        result = self.run_full()
        result["generated"] = gen_counts
        log.info("backfill_complete", start_date=start_date.isoformat(), end_date=end_date.isoformat())
        return result

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
