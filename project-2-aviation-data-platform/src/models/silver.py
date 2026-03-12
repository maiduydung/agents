"""Silver layer schemas — cleaned, validated, deduplicated data."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


class SilverFlight(BaseModel):
    """Cleaned flight operations record."""

    flight_id: str
    flight_number: str
    airline_code: str
    aircraft_registration: str
    aircraft_type: str
    origin: str = Field(..., min_length=3, max_length=3)
    destination: str = Field(..., min_length=3, max_length=3)
    scheduled_departure: datetime
    scheduled_arrival: datetime
    actual_departure: datetime | None = None
    actual_arrival: datetime | None = None
    status: str
    delay_minutes: int = 0
    pax_count: int = 0
    fuel_kg: float | None = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {"SCHEDULED", "DEPARTED", "ARRIVED", "CANCELLED", "DIVERTED", "DELAYED"}
        v_upper = v.upper().strip()
        if v_upper not in allowed:
            raise ValueError(f"Invalid flight status: {v}")
        return v_upper

    @field_validator("delay_minutes")
    @classmethod
    def validate_delay(cls, v: int) -> int:
        if v < -30:
            raise ValueError(f"Unrealistic early arrival: {v} minutes")
        if v > 1440:
            raise ValueError(f"Delay exceeds 24 hours: {v} minutes")
        return v

    @field_validator("aircraft_type")
    @classmethod
    def validate_aircraft(cls, v: str) -> str:
        allowed = {"A320", "A321", "A350", "B787"}
        if v not in allowed:
            raise ValueError(f"Unknown aircraft type: {v}")
        return v


class SilverReservation(BaseModel):
    """Cleaned reservation record."""

    reservation_id: str
    flight_number: str
    airline_code: str
    passenger_name: str
    booking_date: date
    departure_date: date
    origin: str = Field(..., min_length=3, max_length=3)
    destination: str = Field(..., min_length=3, max_length=3)
    fare_class: str = Field(..., min_length=1, max_length=2)
    fare_amount: float = Field(..., ge=0)
    currency: str = "VND"
    booking_channel: str = "WEB"
    pnr: str | None = None
    status: str = "CONFIRMED"

    @field_validator("fare_amount")
    @classmethod
    def validate_fare(cls, v: float) -> float:
        if v > 100_000_000:
            raise ValueError(f"Fare amount unrealistically high: {v}")
        return v


class SilverCrew(BaseModel):
    """Cleaned crew assignment record."""

    crew_id: str
    employee_id: str
    crew_name: str
    role: str
    airline_code: str
    flight_number: str
    flight_date: date
    license_type: str | None = None
    base_airport: str | None = None
    status: str = "ACTIVE"

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        allowed = {"CAPTAIN", "FIRST_OFFICER", "CABIN_CREW", "PURSER", "ENGINEER"}
        v_upper = v.upper().strip()
        if v_upper not in allowed:
            raise ValueError(f"Invalid crew role: {v}")
        return v_upper


class SilverWeather(BaseModel):
    """Cleaned weather observation."""

    observation_id: str
    airport_code: str = Field(..., min_length=3, max_length=3)
    observed_at: datetime
    temperature_c: float | None = None
    wind_speed_kts: int | None = None
    wind_direction: int | None = None
    visibility_km: float | None = None
    condition: str | None = None
    ceiling_ft: int | None = None

    @field_validator("temperature_c")
    @classmethod
    def validate_temp(cls, v: float | None) -> float | None:
        if v is not None and (v < -50 or v > 60):
            raise ValueError(f"Temperature out of range: {v}")
        return v

    @field_validator("wind_speed_kts")
    @classmethod
    def validate_wind(cls, v: int | None) -> int | None:
        if v is not None and (v < 0 or v > 200):
            raise ValueError(f"Wind speed out of range: {v}")
        return v


# DuckDB DDL for silver tables
SILVER_DDL: dict[str, str] = {
    "silver_flights": """
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id               VARCHAR PRIMARY KEY,
            flight_number           VARCHAR NOT NULL,
            airline_code            VARCHAR NOT NULL,
            aircraft_registration   VARCHAR NOT NULL,
            aircraft_type           VARCHAR NOT NULL,
            origin                  VARCHAR(3) NOT NULL,
            destination             VARCHAR(3) NOT NULL,
            scheduled_departure     TIMESTAMP NOT NULL,
            scheduled_arrival       TIMESTAMP NOT NULL,
            actual_departure        TIMESTAMP,
            actual_arrival          TIMESTAMP,
            status                  VARCHAR NOT NULL,
            delay_minutes           INTEGER DEFAULT 0,
            pax_count               INTEGER DEFAULT 0,
            fuel_kg                 DOUBLE,
            _bronze_batch_id        VARCHAR,
            _processed_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "silver_reservations": """
        CREATE TABLE IF NOT EXISTS silver_reservations (
            reservation_id      VARCHAR PRIMARY KEY,
            flight_number       VARCHAR NOT NULL,
            airline_code        VARCHAR NOT NULL,
            passenger_name      VARCHAR NOT NULL,
            booking_date        DATE NOT NULL,
            departure_date      DATE NOT NULL,
            origin              VARCHAR(3) NOT NULL,
            destination         VARCHAR(3) NOT NULL,
            fare_class          VARCHAR(2) NOT NULL,
            fare_amount         DOUBLE NOT NULL,
            currency            VARCHAR DEFAULT 'VND',
            booking_channel     VARCHAR DEFAULT 'WEB',
            pnr                 VARCHAR,
            status              VARCHAR DEFAULT 'CONFIRMED',
            _bronze_batch_id    VARCHAR,
            _processed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "silver_crew": """
        CREATE TABLE IF NOT EXISTS silver_crew (
            crew_id         VARCHAR PRIMARY KEY,
            employee_id     VARCHAR NOT NULL,
            crew_name       VARCHAR NOT NULL,
            role            VARCHAR NOT NULL,
            airline_code    VARCHAR NOT NULL,
            flight_number   VARCHAR NOT NULL,
            flight_date     DATE NOT NULL,
            license_type    VARCHAR,
            base_airport    VARCHAR,
            status          VARCHAR DEFAULT 'ACTIVE',
            _bronze_batch_id VARCHAR,
            _processed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "silver_weather": """
        CREATE TABLE IF NOT EXISTS silver_weather (
            observation_id  VARCHAR PRIMARY KEY,
            airport_code    VARCHAR(3) NOT NULL,
            observed_at     TIMESTAMP NOT NULL,
            temperature_c   DOUBLE,
            wind_speed_kts  INTEGER,
            wind_direction  INTEGER,
            visibility_km   DOUBLE,
            condition       VARCHAR,
            ceiling_ft      INTEGER,
            _bronze_batch_id VARCHAR,
            _processed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
}
