"""Data quality check definitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class CheckSeverity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class CheckStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"


@dataclass
class QualityCheck:
    """Definition of a single data quality check."""

    name: str
    description: str
    layer: str
    table: str
    sql: str
    severity: CheckSeverity = CheckSeverity.ERROR
    threshold: float | None = None  # For percentage-based checks


@dataclass
class CheckResult:
    """Result of executing a quality check."""

    check: QualityCheck
    status: CheckStatus
    actual_value: Any
    message: str


# ---------------------------------------------------------------------------
# Bronze layer checks
# ---------------------------------------------------------------------------
BRONZE_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="bronze_flights_not_empty",
        description="Bronze flights table has records",
        layer="bronze",
        table="bronze_flights",
        sql="SELECT COUNT(*) FROM bronze_flights",
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="bronze_reservations_not_empty",
        description="Bronze reservations table has records",
        layer="bronze",
        table="bronze_reservations",
        sql="SELECT COUNT(*) FROM bronze_reservations",
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="bronze_flights_null_flight_id",
        description="No null flight_id in bronze flights",
        layer="bronze",
        table="bronze_flights",
        sql="SELECT COUNT(*) FROM bronze_flights WHERE flight_id IS NULL",
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
]

# ---------------------------------------------------------------------------
# Silver layer checks
# ---------------------------------------------------------------------------
SILVER_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="silver_flights_valid_status",
        description="All silver flights have valid status values",
        layer="silver",
        table="silver_flights",
        sql="""
            SELECT COUNT(*) FROM silver_flights
            WHERE status NOT IN ('SCHEDULED', 'DEPARTED', 'ARRIVED', 'CANCELLED', 'DIVERTED', 'DELAYED')
        """,
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="silver_flights_positive_delay",
        description="No unrealistic negative delays in silver flights",
        layer="silver",
        table="silver_flights",
        sql="SELECT COUNT(*) FROM silver_flights WHERE delay_minutes < -30",
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="silver_reservations_positive_fare",
        description="All silver reservations have non-negative fares",
        layer="silver",
        table="silver_reservations",
        sql="SELECT COUNT(*) FROM silver_reservations WHERE fare_amount < 0",
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="silver_flights_null_rate_origin",
        description="Null rate for origin is below 5%",
        layer="silver",
        table="silver_flights",
        sql="""
            SELECT ROUND(
                CAST(SUM(CASE WHEN origin IS NULL OR origin = '' THEN 1 ELSE 0 END) AS DOUBLE)
                / NULLIF(COUNT(*), 0), 4
            ) FROM silver_flights
        """,
        severity=CheckSeverity.WARNING,
        threshold=0.05,
    ),
    QualityCheck(
        name="silver_crew_valid_role",
        description="All silver crew have valid roles",
        layer="silver",
        table="silver_crew",
        sql="""
            SELECT COUNT(*) FROM silver_crew
            WHERE role NOT IN ('CAPTAIN', 'FIRST_OFFICER', 'CABIN_CREW', 'PURSER', 'ENGINEER')
        """,
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="silver_flights_no_duplicates",
        description="No duplicate flight_id in silver flights",
        layer="silver",
        table="silver_flights",
        sql="""
            SELECT COUNT(*) FROM (
                SELECT flight_id, COUNT(*) AS cnt
                FROM silver_flights
                GROUP BY flight_id
                HAVING cnt > 1
            )
        """,
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
]

# ---------------------------------------------------------------------------
# Gold layer checks
# ---------------------------------------------------------------------------
GOLD_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="gold_fact_flights_referential_routes",
        description="All fact_flights have valid route_key references",
        layer="gold",
        table="fact_flights",
        sql="""
            SELECT COUNT(*) FROM fact_flights ff
            WHERE ff.route_key IS NOT NULL
              AND ff.route_key NOT IN (SELECT route_key FROM dim_routes)
        """,
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="gold_fact_flights_referential_aircraft",
        description="All fact_flights have valid aircraft_key references",
        layer="gold",
        table="fact_flights",
        sql="""
            SELECT COUNT(*) FROM fact_flights ff
            WHERE ff.aircraft_key IS NOT NULL
              AND ff.aircraft_key NOT IN (SELECT aircraft_key FROM dim_aircraft)
        """,
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="gold_fact_flights_referential_time",
        description="All fact_flights have valid date_key references",
        layer="gold",
        table="fact_flights",
        sql="""
            SELECT COUNT(*) FROM fact_flights ff
            WHERE ff.date_key IS NOT NULL
              AND ff.date_key NOT IN (SELECT date_key FROM dim_time)
        """,
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
    QualityCheck(
        name="gold_load_factor_range",
        description="Load factor is between 0 and 1.5",
        layer="gold",
        table="fact_flights",
        sql="""
            SELECT COUNT(*) FROM fact_flights
            WHERE load_factor < 0 OR load_factor > 1.5
        """,
        severity=CheckSeverity.WARNING,
        threshold=0,
    ),
    QualityCheck(
        name="gold_dim_time_coverage",
        description="dim_time has at least 1 date entry",
        layer="gold",
        table="dim_time",
        sql="SELECT COUNT(*) FROM dim_time",
        severity=CheckSeverity.ERROR,
        threshold=0,
    ),
]

ALL_CHECKS = BRONZE_CHECKS + SILVER_CHECKS + GOLD_CHECKS
