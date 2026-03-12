"""Pipeline metrics collector — tracks rows processed, duration, errors."""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Generator

import duckdb

from src.observability.logger import get_logger

log = get_logger(__name__)


@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage execution."""

    stage: str
    layer: str
    batch_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    duration_ms: float = 0.0
    status: str = "RUNNING"
    error_message: str | None = None


class MetricsCollector:
    """Collects and persists pipeline execution metrics."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the pipeline_runs tracking table if it doesn't exist."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id          VARCHAR PRIMARY KEY,
                batch_id        VARCHAR NOT NULL,
                stage           VARCHAR NOT NULL,
                layer           VARCHAR NOT NULL,
                started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
                finished_at     TIMESTAMP WITH TIME ZONE,
                rows_in         INTEGER DEFAULT 0,
                rows_out        INTEGER DEFAULT 0,
                rows_rejected   INTEGER DEFAULT 0,
                duration_ms     DOUBLE DEFAULT 0,
                status          VARCHAR DEFAULT 'RUNNING',
                error_message   VARCHAR
            )
        """)

    @contextmanager
    def track(self, stage: str, layer: str, batch_id: str) -> Generator[StageMetrics, None, None]:
        """Context manager that tracks metrics for a pipeline stage."""
        metrics = StageMetrics(stage=stage, layer=layer, batch_id=batch_id)
        run_id = str(uuid.uuid4())
        start = time.perf_counter()

        try:
            yield metrics
            metrics.status = "SUCCESS"
        except Exception as exc:
            metrics.status = "FAILED"
            metrics.error_message = str(exc)[:500]
            raise
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            metrics.duration_ms = round(elapsed, 2)
            metrics.finished_at = datetime.now(timezone.utc)
            self._persist(run_id, metrics)
            self._log_metrics(metrics)

    def _persist(self, run_id: str, m: StageMetrics) -> None:
        """Write metrics to the pipeline_runs table."""
        self._conn.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, batch_id, stage, layer, started_at, finished_at,
                 rows_in, rows_out, rows_rejected, duration_ms, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                m.batch_id,
                m.stage,
                m.layer,
                m.started_at,
                m.finished_at,
                m.rows_in,
                m.rows_out,
                m.rows_rejected,
                m.duration_ms,
                m.status,
                m.error_message,
            ],
        )

    def _log_metrics(self, m: StageMetrics) -> None:
        """Emit structured log for the stage."""
        rejection_rate = m.rows_rejected / max(m.rows_in, 1)
        log.info(
            "pipeline_stage_complete",
            stage=m.stage,
            layer=m.layer,
            batch_id=m.batch_id,
            rows_in=m.rows_in,
            rows_out=m.rows_out,
            rows_rejected=m.rows_rejected,
            rejection_rate=round(rejection_rate, 4),
            duration_ms=m.duration_ms,
            status=m.status,
        )
        if rejection_rate > 0.05:
            log.warning(
                "high_rejection_rate",
                stage=m.stage,
                rate=round(rejection_rate, 4),
                threshold=0.05,
            )

    def get_latest_runs(self, limit: int = 20) -> list[dict]:
        """Return the most recent pipeline runs."""
        result = self._conn.execute(
            """
            SELECT * FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        columns = [
            "run_id", "batch_id", "stage", "layer", "started_at", "finished_at",
            "rows_in", "rows_out", "rows_rejected", "duration_ms", "status", "error_message",
        ]
        return [dict(zip(columns, row)) for row in result]
