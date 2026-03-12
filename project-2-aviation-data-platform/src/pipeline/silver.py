"""Silver layer — clean, validate, deduplicate bronze data."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import duckdb
from pydantic import ValidationError

from src.config import PlatformConfig
from src.models.silver import (
    SILVER_DDL,
    SilverCrew,
    SilverFlight,
    SilverReservation,
    SilverWeather,
)
from src.observability.logger import get_logger
from src.observability.metrics import MetricsCollector

log = get_logger(__name__)


class SilverLayer:
    """Transforms bronze data into cleaned, validated silver records."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: PlatformConfig, metrics: MetricsCollector) -> None:
        self._conn = conn
        self._config = config
        self._metrics = metrics

    def initialize(self) -> None:
        """Create silver tables if they do not exist."""
        for table_name, ddl in SILVER_DDL.items():
            self._conn.execute(ddl)
            log.debug("silver_table_ready", table=table_name)

    def process_all(self, batch_id: str) -> dict[str, dict[str, int]]:
        """Process all bronze data into silver. Returns per-source stats."""
        self.initialize()
        results: dict[str, dict[str, int]] = {}

        results["flights"] = self._process_flights(batch_id)
        results["reservations"] = self._process_reservations(batch_id)
        results["crew"] = self._process_crew(batch_id)
        results["weather"] = self._process_weather(batch_id)

        log.info("silver_processing_complete", batch_id=batch_id, results=results)
        return results

    def _process_flights(self, batch_id: str) -> dict[str, int]:
        """Validate and deduplicate flights from bronze to silver."""
        with self._metrics.track("process_flights", "silver", batch_id) as m:
            rows = self._conn.execute(
                "SELECT * FROM bronze_flights WHERE _batch_id = ?", [batch_id]
            ).fetchall()
            columns = [
                "flight_id", "flight_number", "airline_code", "aircraft_registration",
                "aircraft_type", "origin", "destination", "scheduled_departure",
                "scheduled_arrival", "actual_departure", "actual_arrival", "status",
                "delay_minutes", "pax_count", "fuel_kg", "_source", "_ingested_at", "_batch_id",
            ]
            m.rows_in = len(rows)
            accepted = 0
            rejected = 0

            for row in rows:
                rec = dict(zip(columns, row))
                try:
                    validated = SilverFlight(
                        flight_id=rec["flight_id"],
                        flight_number=rec["flight_number"],
                        airline_code=rec["airline_code"],
                        aircraft_registration=rec["aircraft_registration"],
                        aircraft_type=rec["aircraft_type"],
                        origin=rec["origin"] or "XXX",
                        destination=rec["destination"] or "XXX",
                        scheduled_departure=rec["scheduled_departure"],
                        scheduled_arrival=rec["scheduled_arrival"],
                        actual_departure=rec["actual_departure"],
                        actual_arrival=rec["actual_arrival"],
                        status=rec["status"],
                        delay_minutes=rec["delay_minutes"] if rec["delay_minutes"] is not None else 0,
                        pax_count=rec["pax_count"] if rec["pax_count"] is not None else 0,
                        fuel_kg=rec["fuel_kg"],
                    )
                except (ValidationError, ValueError, TypeError) as e:
                    rejected += 1
                    log.debug("flight_validation_failed", flight_id=rec.get("flight_id"), error=str(e)[:200])
                    continue

                # Upsert — deduplicate on flight_id
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO silver_flights
                        (flight_id, flight_number, airline_code, aircraft_registration,
                         aircraft_type, origin, destination, scheduled_departure,
                         scheduled_arrival, actual_departure, actual_arrival, status,
                         delay_minutes, pax_count, fuel_kg, _bronze_batch_id, _processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        validated.flight_id, validated.flight_number, validated.airline_code,
                        validated.aircraft_registration, validated.aircraft_type,
                        validated.origin, validated.destination,
                        validated.scheduled_departure, validated.scheduled_arrival,
                        validated.actual_departure, validated.actual_arrival,
                        validated.status, validated.delay_minutes, validated.pax_count,
                        validated.fuel_kg, batch_id,
                    ],
                )
                accepted += 1

            m.rows_out = accepted
            m.rows_rejected = rejected
        return {"accepted": accepted, "rejected": rejected}

    def _process_reservations(self, batch_id: str) -> dict[str, int]:
        """Validate and deduplicate reservations from bronze to silver."""
        with self._metrics.track("process_reservations", "silver", batch_id) as m:
            rows = self._conn.execute(
                "SELECT * FROM bronze_reservations WHERE _batch_id = ?", [batch_id]
            ).fetchall()
            columns = [
                "reservation_id", "flight_number", "airline_code", "passenger_name",
                "booking_date", "departure_date", "origin", "destination",
                "fare_class", "fare_amount", "currency", "booking_channel",
                "pnr", "status", "_source", "_ingested_at", "_batch_id",
            ]
            m.rows_in = len(rows)
            accepted = 0
            rejected = 0

            for row in rows:
                rec = dict(zip(columns, row))
                try:
                    # Skip records with empty passenger names or negative fares
                    if not rec.get("passenger_name"):
                        raise ValueError("Empty passenger name")
                    fare = rec.get("fare_amount")
                    if fare is not None and fare < 0:
                        raise ValueError(f"Negative fare: {fare}")

                    validated = SilverReservation(
                        reservation_id=rec["reservation_id"],
                        flight_number=rec["flight_number"],
                        airline_code=rec["airline_code"],
                        passenger_name=rec["passenger_name"],
                        booking_date=rec["booking_date"],
                        departure_date=rec["departure_date"],
                        origin=rec["origin"] or "XXX",
                        destination=rec["destination"] or "XXX",
                        fare_class=rec["fare_class"],
                        fare_amount=max(float(fare or 0), 0),
                        currency=rec.get("currency", "VND"),
                        booking_channel=rec.get("booking_channel", "WEB"),
                        pnr=rec.get("pnr"),
                        status=rec.get("status", "CONFIRMED"),
                    )
                except (ValidationError, ValueError, TypeError) as e:
                    rejected += 1
                    log.debug("reservation_validation_failed", reservation_id=rec.get("reservation_id"), error=str(e)[:200])
                    continue

                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO silver_reservations
                        (reservation_id, flight_number, airline_code, passenger_name,
                         booking_date, departure_date, origin, destination,
                         fare_class, fare_amount, currency, booking_channel,
                         pnr, status, _bronze_batch_id, _processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        validated.reservation_id, validated.flight_number, validated.airline_code,
                        validated.passenger_name, validated.booking_date, validated.departure_date,
                        validated.origin, validated.destination, validated.fare_class,
                        validated.fare_amount, validated.currency, validated.booking_channel,
                        validated.pnr, validated.status, batch_id,
                    ],
                )
                accepted += 1

            m.rows_out = accepted
            m.rows_rejected = rejected
        return {"accepted": accepted, "rejected": rejected}

    def _process_crew(self, batch_id: str) -> dict[str, int]:
        """Validate and deduplicate crew records from bronze to silver."""
        with self._metrics.track("process_crew", "silver", batch_id) as m:
            rows = self._conn.execute(
                "SELECT * FROM bronze_crew WHERE _batch_id = ?", [batch_id]
            ).fetchall()
            columns = [
                "crew_id", "employee_id", "crew_name", "role", "airline_code",
                "flight_number", "flight_date", "license_type", "base_airport",
                "status", "_source", "_ingested_at", "_batch_id",
            ]
            m.rows_in = len(rows)
            accepted = 0
            rejected = 0

            for row in rows:
                rec = dict(zip(columns, row))
                try:
                    validated = SilverCrew(
                        crew_id=rec["crew_id"],
                        employee_id=rec["employee_id"],
                        crew_name=rec["crew_name"],
                        role=rec["role"],
                        airline_code=rec["airline_code"],
                        flight_number=rec["flight_number"],
                        flight_date=rec["flight_date"],
                        license_type=rec.get("license_type"),
                        base_airport=rec.get("base_airport"),
                        status=rec.get("status", "ACTIVE"),
                    )
                except (ValidationError, ValueError, TypeError) as e:
                    rejected += 1
                    log.debug("crew_validation_failed", crew_id=rec.get("crew_id"), error=str(e)[:200])
                    continue

                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO silver_crew
                        (crew_id, employee_id, crew_name, role, airline_code,
                         flight_number, flight_date, license_type, base_airport,
                         status, _bronze_batch_id, _processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        validated.crew_id, validated.employee_id, validated.crew_name,
                        validated.role, validated.airline_code, validated.flight_number,
                        validated.flight_date, validated.license_type, validated.base_airport,
                        validated.status, batch_id,
                    ],
                )
                accepted += 1

            m.rows_out = accepted
            m.rows_rejected = rejected
        return {"accepted": accepted, "rejected": rejected}

    def _process_weather(self, batch_id: str) -> dict[str, int]:
        """Validate and deduplicate weather records from bronze to silver."""
        with self._metrics.track("process_weather", "silver", batch_id) as m:
            rows = self._conn.execute(
                "SELECT * FROM bronze_weather WHERE _batch_id = ?", [batch_id]
            ).fetchall()
            columns = [
                "observation_id", "airport_code", "observed_at", "temperature_c",
                "wind_speed_kts", "wind_direction", "visibility_km", "condition",
                "ceiling_ft", "_source", "_ingested_at", "_batch_id",
            ]
            m.rows_in = len(rows)
            accepted = 0
            rejected = 0

            for row in rows:
                rec = dict(zip(columns, row))
                try:
                    validated = SilverWeather(
                        observation_id=rec["observation_id"],
                        airport_code=rec["airport_code"],
                        observed_at=rec["observed_at"],
                        temperature_c=rec.get("temperature_c"),
                        wind_speed_kts=rec.get("wind_speed_kts"),
                        wind_direction=rec.get("wind_direction"),
                        visibility_km=rec.get("visibility_km"),
                        condition=rec.get("condition"),
                        ceiling_ft=rec.get("ceiling_ft"),
                    )
                except (ValidationError, ValueError, TypeError) as e:
                    rejected += 1
                    log.debug("weather_validation_failed", observation_id=rec.get("observation_id"), error=str(e)[:200])
                    continue

                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO silver_weather
                        (observation_id, airport_code, observed_at, temperature_c,
                         wind_speed_kts, wind_direction, visibility_km, condition,
                         ceiling_ft, _bronze_batch_id, _processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        validated.observation_id, validated.airport_code, validated.observed_at,
                        validated.temperature_c, validated.wind_speed_kts, validated.wind_direction,
                        validated.visibility_km, validated.condition, validated.ceiling_ft,
                        batch_id,
                    ],
                )
                accepted += 1

            m.rows_out = accepted
            m.rows_rejected = rejected
        return {"accepted": accepted, "rejected": rejected}
