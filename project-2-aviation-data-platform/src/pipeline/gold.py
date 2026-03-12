"""Gold layer — star schema dimensional models built from silver data via SQL."""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.config import PlatformConfig
from src.models.gold import GOLD_DDL
from src.observability.logger import get_logger
from src.observability.metrics import MetricsCollector

log = get_logger(__name__)

# Order matters: dimensions before facts
SQL_EXECUTION_ORDER = [
    "gold_dim_time.sql",
    "gold_dim_aircraft.sql",
    "gold_dim_routes.sql",
    "gold_fact_flights.sql",
    "gold_fact_bookings.sql",
]


class GoldLayer:
    """Builds star schema serving layer from silver data using SQL transformations."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: PlatformConfig, metrics: MetricsCollector) -> None:
        self._conn = conn
        self._config = config
        self._metrics = metrics
        self._sql_dir = Path(config.sql_dir)

    def initialize(self) -> None:
        """Create gold tables if they do not exist."""
        for table_name, ddl in GOLD_DDL.items():
            self._conn.execute(ddl)
            log.debug("gold_table_ready", table=table_name)

    def build_all(self, batch_id: str) -> dict[str, int]:
        """Execute all gold SQL transformations in order. Returns row counts per table."""
        self.initialize()
        counts: dict[str, int] = {}

        for sql_file in SQL_EXECUTION_ORDER:
            table_name = sql_file.replace(".sql", "").replace("gold_", "")
            counts[table_name] = self._execute_sql(sql_file, table_name, batch_id)

        log.info("gold_build_complete", batch_id=batch_id, counts=counts)
        return counts

    def _execute_sql(self, sql_file: str, table_name: str, batch_id: str) -> int:
        """Execute a single SQL file and return the number of rows affected."""
        sql_path = self._sql_dir / sql_file
        if not sql_path.exists():
            log.error("sql_file_not_found", file=str(sql_path))
            return 0

        sql = sql_path.read_text()

        # Get row count before
        full_table = f"{'dim' if 'dim' in table_name else 'fact'}_{table_name.replace('dim_', '').replace('fact_', '')}"
        # Derive the actual DuckDB table name
        if table_name.startswith("dim_") or table_name.startswith("fact_"):
            full_table = table_name
        else:
            full_table = table_name

        # Map sql file names to actual table names
        table_map = {
            "dim_time": "dim_time",
            "dim_aircraft": "dim_aircraft",
            "dim_routes": "dim_routes",
            "fact_flights": "fact_flights",
            "fact_bookings": "fact_bookings",
        }
        actual_table = table_map.get(table_name, table_name)

        with self._metrics.track(f"build_{table_name}", "gold", batch_id) as m:
            before_count = self._conn.execute(f"SELECT COUNT(*) FROM {actual_table}").fetchone()[0]

            self._conn.execute(sql)

            after_count = self._conn.execute(f"SELECT COUNT(*) FROM {actual_table}").fetchone()[0]
            new_rows = after_count - before_count
            m.rows_in = before_count
            m.rows_out = new_rows

        log.info("gold_sql_executed", file=sql_file, table=actual_table, new_rows=new_rows, total_rows=after_count)
        return new_rows

    def get_table_stats(self) -> dict[str, int]:
        """Return row counts for all gold tables."""
        stats: dict[str, int] = {}
        for table_name in GOLD_DDL:
            try:
                count = self._conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                stats[table_name] = count
            except Exception:
                stats[table_name] = 0
        return stats
