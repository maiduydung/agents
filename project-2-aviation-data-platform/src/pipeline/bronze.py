"""Bronze layer — raw data landing with metadata. Append-only, no transformations."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb

from src.config import PlatformConfig
from src.ingestion.crew import load_raw_crew
from src.ingestion.flight_ops import load_raw_flights
from src.ingestion.reservations import load_raw_reservations
from src.ingestion.weather import load_raw_weather
from src.models.bronze import BRONZE_DDL
from src.observability.logger import get_logger
from src.observability.metrics import MetricsCollector

log = get_logger(__name__)


class BronzeLayer:
    """Ingests raw data into DuckDB bronze tables with metadata columns."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: PlatformConfig, metrics: MetricsCollector) -> None:
        self._conn = conn
        self._config = config
        self._metrics = metrics
        self._batch_id = f"batch-{uuid.uuid4().hex[:8]}"

    def initialize(self) -> None:
        """Create bronze tables if they do not exist."""
        for table_name, ddl in BRONZE_DDL.items():
            self._conn.execute(ddl)
            log.debug("bronze_table_ready", table=table_name)

    def ingest_all(self) -> dict[str, int]:
        """Run full bronze ingestion for all sources. Returns row counts."""
        self.initialize()
        counts: dict[str, int] = {}

        counts["flights"] = self._ingest_flights()
        counts["reservations"] = self._ingest_reservations()
        counts["crew"] = self._ingest_crew()
        counts["weather"] = self._ingest_weather()

        log.info("bronze_ingestion_complete", batch_id=self._batch_id, counts=counts)
        return counts

    @property
    def batch_id(self) -> str:
        return self._batch_id

    def _ingest_flights(self) -> int:
        """Ingest flight operations data into bronze_flights."""
        records = load_raw_flights(self._config.data_dir)
        with self._metrics.track("ingest_flights", "bronze", self._batch_id) as m:
            m.rows_in = len(records)
            ingested_at = datetime.now(timezone.utc)
            for rec in records:
                self._conn.execute(
                    """
                    INSERT INTO bronze_flights VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        rec.get("flight_id"),
                        rec.get("flight_number"),
                        rec.get("airline_code"),
                        rec.get("aircraft_registration"),
                        rec.get("aircraft_type"),
                        rec.get("origin"),
                        rec.get("destination"),
                        rec.get("scheduled_departure"),
                        rec.get("scheduled_arrival"),
                        rec.get("actual_departure"),
                        rec.get("actual_arrival"),
                        rec.get("status"),
                        rec.get("delay_minutes"),
                        rec.get("pax_count"),
                        rec.get("fuel_kg"),
                        "flight_ops_system",
                        ingested_at,
                        self._batch_id,
                    ],
                )
            m.rows_out = len(records)
        return len(records)

    def _ingest_reservations(self) -> int:
        """Ingest reservation data into bronze_reservations."""
        records = load_raw_reservations(self._config.data_dir)
        with self._metrics.track("ingest_reservations", "bronze", self._batch_id) as m:
            m.rows_in = len(records)
            ingested_at = datetime.now(timezone.utc)
            for rec in records:
                self._conn.execute(
                    """
                    INSERT INTO bronze_reservations VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        rec.get("reservation_id"),
                        rec.get("flight_number"),
                        rec.get("airline_code"),
                        rec.get("passenger_name"),
                        rec.get("booking_date"),
                        rec.get("departure_date"),
                        rec.get("origin"),
                        rec.get("destination"),
                        rec.get("fare_class"),
                        rec.get("fare_amount"),
                        rec.get("currency"),
                        rec.get("booking_channel"),
                        rec.get("pnr"),
                        rec.get("status"),
                        "reservation_system",
                        ingested_at,
                        self._batch_id,
                    ],
                )
            m.rows_out = len(records)
        return len(records)

    def _ingest_crew(self) -> int:
        """Ingest crew data into bronze_crew."""
        records = load_raw_crew(self._config.data_dir)
        with self._metrics.track("ingest_crew", "bronze", self._batch_id) as m:
            m.rows_in = len(records)
            ingested_at = datetime.now(timezone.utc)
            for rec in records:
                self._conn.execute(
                    """
                    INSERT INTO bronze_crew VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        rec.get("crew_id"),
                        rec.get("employee_id"),
                        rec.get("crew_name"),
                        rec.get("role"),
                        rec.get("airline_code"),
                        rec.get("flight_number"),
                        rec.get("flight_date"),
                        rec.get("license_type"),
                        rec.get("base_airport"),
                        rec.get("status"),
                        "crew_management_system",
                        ingested_at,
                        self._batch_id,
                    ],
                )
            m.rows_out = len(records)
        return len(records)

    def _ingest_weather(self) -> int:
        """Ingest weather data into bronze_weather."""
        records = load_raw_weather(self._config.data_dir)
        with self._metrics.track("ingest_weather", "bronze", self._batch_id) as m:
            m.rows_in = len(records)
            ingested_at = datetime.now(timezone.utc)
            for rec in records:
                self._conn.execute(
                    """
                    INSERT INTO bronze_weather VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        rec.get("observation_id"),
                        rec.get("airport_code"),
                        rec.get("observed_at"),
                        rec.get("temperature_c"),
                        rec.get("wind_speed_kts"),
                        rec.get("wind_direction"),
                        rec.get("visibility_km"),
                        rec.get("condition"),
                        rec.get("ceiling_ft"),
                        "weather_service",
                        ingested_at,
                        self._batch_id,
                    ],
                )
            m.rows_out = len(records)
        return len(records)
