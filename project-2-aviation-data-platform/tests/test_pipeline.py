"""Basic pipeline tests — validates end-to-end pipeline execution."""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date
from pathlib import Path

import pytest

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import PlatformConfig
from src.observability.logger import setup_logging


@pytest.fixture(autouse=True)
def _setup_logging() -> None:
    setup_logging("WARNING")


@pytest.fixture
def temp_config(tmp_path: Path) -> PlatformConfig:
    """Create a config pointing to a temporary directory."""
    config = PlatformConfig(
        project_root=tmp_path,
        db_path=str(tmp_path / "test.duckdb"),
        data_dir=str(tmp_path / "data"),
        sql_dir=str(PROJECT_ROOT / "sql"),
    )
    config.ensure_dirs()
    return config


class TestDataGeneration:
    """Test synthetic data generation."""

    def test_generate_all(self, temp_config: PlatformConfig) -> None:
        from generators.aviation_data import AviationDataGenerator

        gen = AviationDataGenerator(temp_config)
        counts = gen.generate_all(num_records=50)

        assert counts["flights"] == 50
        assert counts["reservations"] == 150  # 3x flights
        assert counts["crew"] == 50
        assert counts["weather"] == 50

        # Check files exist
        raw_dir = Path(temp_config.data_dir) / "raw"
        assert (raw_dir / "flights.jsonl").exists()
        assert (raw_dir / "reservations.jsonl").exists()
        assert (raw_dir / "crew.jsonl").exists()
        assert (raw_dir / "weather.jsonl").exists()


class TestBronzeLayer:
    """Test bronze layer ingestion."""

    def test_ingest_all(self, temp_config: PlatformConfig) -> None:
        from generators.aviation_data import AviationDataGenerator
        from src.pipeline.orchestrator import PipelineOrchestrator

        gen = AviationDataGenerator(temp_config)
        gen.generate_all(num_records=20)

        orchestrator = PipelineOrchestrator(temp_config)
        try:
            counts = orchestrator.run_bronze()
            assert counts["flights"] == 20
            assert counts["reservations"] == 60
            assert counts["crew"] == 20
            assert counts["weather"] == 20
        finally:
            orchestrator.close()


class TestFullPipeline:
    """Test full pipeline execution."""

    def test_run_full(self, temp_config: PlatformConfig) -> None:
        from generators.aviation_data import AviationDataGenerator
        from src.pipeline.orchestrator import PipelineOrchestrator

        gen = AviationDataGenerator(temp_config)
        gen.generate_all(num_records=30)

        orchestrator = PipelineOrchestrator(temp_config)
        try:
            summary = orchestrator.run_full()

            assert "batch_id" in summary
            assert summary["bronze"]["flights"] == 30

            # Silver should have accepted most records
            assert summary["silver"]["flights"]["accepted"] > 0

            # Gold should have built tables
            gold_totals = summary["gold_totals"]
            assert gold_totals["dim_time"] > 0
            assert gold_totals["dim_routes"] > 0
            assert gold_totals["fact_flights"] > 0
        finally:
            orchestrator.close()


class TestQuality:
    """Test data quality checks."""

    def test_quality_after_pipeline(self, temp_config: PlatformConfig) -> None:
        from generators.aviation_data import AviationDataGenerator
        from src.pipeline.orchestrator import PipelineOrchestrator
        from src.quality.checks import CheckStatus

        gen = AviationDataGenerator(temp_config)
        gen.generate_all(num_records=30)

        orchestrator = PipelineOrchestrator(temp_config)
        try:
            orchestrator.run_full()
            results = orchestrator.run_quality()

            # At least some checks should pass
            passed = [r for r in results if r.status == CheckStatus.PASS]
            assert len(passed) > 0
        finally:
            orchestrator.close()


class TestIdempotency:
    """Test that pipeline is idempotent — running twice doesn't create duplicates in silver/gold."""

    def test_no_silver_duplicates(self, temp_config: PlatformConfig) -> None:
        from generators.aviation_data import AviationDataGenerator
        from src.pipeline.orchestrator import PipelineOrchestrator

        gen = AviationDataGenerator(temp_config)
        gen.generate_all(num_records=20)

        orchestrator = PipelineOrchestrator(temp_config)
        try:
            # Run twice
            orchestrator.run_full()
            summary2 = orchestrator.run_full()

            # Gold should not have duplicated — new rows should be 0 on second run
            # (bronze appends, but silver deduplicates, gold uses NOT IN)
            conn = orchestrator.conn
            dup_count = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT flight_id, COUNT(*) AS cnt
                    FROM silver_flights
                    GROUP BY flight_id
                    HAVING cnt > 1
                )
            """).fetchone()[0]
            assert dup_count == 0
        finally:
            orchestrator.close()
