"""Bronze layer schemas — raw data with ingestion metadata."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class BronzeMetadata(BaseModel):
    """Metadata columns added to every bronze record."""

    source: str = Field(..., alias="_source", description="Source system identifier")
    ingested_at: datetime = Field(..., alias="_ingested_at", description="UTC timestamp of ingestion")
    batch_id: str = Field(..., alias="_batch_id", description="Batch identifier for lineage")


class BronzeReservation(BronzeMetadata):
    """Raw reservation record from the booking system."""

    reservation_id: str
    flight_number: str
    airline_code: str
    passenger_name: str
    booking_date: str
    departure_date: str
    origin: str
    destination: str
    fare_class: str
    fare_amount: float | None = None
    currency: str = "VND"
    booking_channel: str | None = None
    pnr: str | None = None
    status: str | None = None


class BronzeFlight(BronzeMetadata):
    """Raw flight operations record."""

    flight_id: str
    flight_number: str
    airline_code: str
    aircraft_registration: str
    aircraft_type: str
    origin: str
    destination: str
    scheduled_departure: str
    scheduled_arrival: str
    actual_departure: str | None = None
    actual_arrival: str | None = None
    status: str
    delay_minutes: int | None = None
    pax_count: int | None = None
    fuel_kg: float | None = None


class BronzeCrew(BronzeMetadata):
    """Raw crew assignment record."""

    crew_id: str
    employee_id: str
    crew_name: str
    role: str
    airline_code: str
    flight_number: str
    flight_date: str
    license_type: str | None = None
    base_airport: str | None = None
    status: str = "ACTIVE"


class BronzeWeather(BronzeMetadata):
    """Raw weather observation for an airport."""

    observation_id: str
    airport_code: str
    observed_at: str
    temperature_c: float | None = None
    wind_speed_kts: int | None = None
    wind_direction: int | None = None
    visibility_km: float | None = None
    condition: str | None = None
    ceiling_ft: int | None = None


# DuckDB DDL for bronze tables
BRONZE_DDL: dict[str, str] = {
    "bronze_reservations": """
        CREATE TABLE IF NOT EXISTS bronze_reservations (
            reservation_id      VARCHAR,
            flight_number       VARCHAR,
            airline_code        VARCHAR,
            passenger_name      VARCHAR,
            booking_date        VARCHAR,
            departure_date      VARCHAR,
            origin              VARCHAR,
            destination         VARCHAR,
            fare_class          VARCHAR,
            fare_amount         DOUBLE,
            currency            VARCHAR,
            booking_channel     VARCHAR,
            pnr                 VARCHAR,
            status              VARCHAR,
            _source             VARCHAR NOT NULL,
            _ingested_at        TIMESTAMP NOT NULL,
            _batch_id           VARCHAR NOT NULL
        )
    """,
    "bronze_flights": """
        CREATE TABLE IF NOT EXISTS bronze_flights (
            flight_id               VARCHAR,
            flight_number           VARCHAR,
            airline_code            VARCHAR,
            aircraft_registration   VARCHAR,
            aircraft_type           VARCHAR,
            origin                  VARCHAR,
            destination             VARCHAR,
            scheduled_departure     VARCHAR,
            scheduled_arrival       VARCHAR,
            actual_departure        VARCHAR,
            actual_arrival          VARCHAR,
            status                  VARCHAR,
            delay_minutes           INTEGER,
            pax_count               INTEGER,
            fuel_kg                 DOUBLE,
            _source                 VARCHAR NOT NULL,
            _ingested_at            TIMESTAMP NOT NULL,
            _batch_id               VARCHAR NOT NULL
        )
    """,
    "bronze_crew": """
        CREATE TABLE IF NOT EXISTS bronze_crew (
            crew_id         VARCHAR,
            employee_id     VARCHAR,
            crew_name       VARCHAR,
            role            VARCHAR,
            airline_code    VARCHAR,
            flight_number   VARCHAR,
            flight_date     VARCHAR,
            license_type    VARCHAR,
            base_airport    VARCHAR,
            status          VARCHAR,
            _source         VARCHAR NOT NULL,
            _ingested_at    TIMESTAMP NOT NULL,
            _batch_id       VARCHAR NOT NULL
        )
    """,
    "bronze_weather": """
        CREATE TABLE IF NOT EXISTS bronze_weather (
            observation_id  VARCHAR,
            airport_code    VARCHAR,
            observed_at     VARCHAR,
            temperature_c   DOUBLE,
            wind_speed_kts  INTEGER,
            wind_direction  INTEGER,
            visibility_km   DOUBLE,
            condition       VARCHAR,
            ceiling_ft      INTEGER,
            _source         VARCHAR NOT NULL,
            _ingested_at    TIMESTAMP NOT NULL,
            _batch_id       VARCHAR NOT NULL
        )
    """,
}
