"""Gold layer schemas — star schema dimensional models."""

from __future__ import annotations

# DuckDB DDL for gold (serving) layer tables — star schema
GOLD_DDL: dict[str, str] = {
    "dim_time": """
        CREATE TABLE IF NOT EXISTS dim_time (
            date_key        INTEGER PRIMARY KEY,
            full_date       DATE NOT NULL,
            year            INTEGER NOT NULL,
            quarter         INTEGER NOT NULL,
            month           INTEGER NOT NULL,
            month_name      VARCHAR NOT NULL,
            week_of_year    INTEGER NOT NULL,
            day_of_month    INTEGER NOT NULL,
            day_of_week     INTEGER NOT NULL,
            day_name        VARCHAR NOT NULL,
            is_weekend      BOOLEAN NOT NULL,
            is_holiday      BOOLEAN DEFAULT FALSE
        )
    """,
    "dim_aircraft": """
        CREATE TABLE IF NOT EXISTS dim_aircraft (
            aircraft_key            INTEGER PRIMARY KEY,
            aircraft_registration   VARCHAR NOT NULL,
            aircraft_type           VARCHAR NOT NULL,
            airline_code            VARCHAR NOT NULL,
            seat_capacity           INTEGER NOT NULL,
            effective_from          DATE NOT NULL,
            effective_to            DATE DEFAULT '9999-12-31',
            is_current              BOOLEAN DEFAULT TRUE
        )
    """,
    "dim_routes": """
        CREATE TABLE IF NOT EXISTS dim_routes (
            route_key       INTEGER PRIMARY KEY,
            origin          VARCHAR(3) NOT NULL,
            destination     VARCHAR(3) NOT NULL,
            airline_code    VARCHAR NOT NULL,
            route_code      VARCHAR NOT NULL,
            distance_km     INTEGER,
            domestic        BOOLEAN DEFAULT TRUE
        )
    """,
    "dim_crew": """
        CREATE TABLE IF NOT EXISTS dim_crew (
            crew_key        INTEGER PRIMARY KEY,
            employee_id     VARCHAR NOT NULL,
            crew_name       VARCHAR NOT NULL,
            role            VARCHAR NOT NULL,
            airline_code    VARCHAR NOT NULL,
            license_type    VARCHAR,
            base_airport    VARCHAR,
            status          VARCHAR DEFAULT 'ACTIVE',
            effective_from  DATE NOT NULL,
            effective_to    DATE DEFAULT '9999-12-31',
            is_current      BOOLEAN DEFAULT TRUE
        )
    """,
    "fact_flights": """
        CREATE TABLE IF NOT EXISTS fact_flights (
            flight_key          INTEGER PRIMARY KEY,
            flight_id           VARCHAR NOT NULL,
            flight_number       VARCHAR NOT NULL,
            date_key            INTEGER REFERENCES dim_time(date_key),
            route_key           INTEGER REFERENCES dim_routes(route_key),
            aircraft_key        INTEGER REFERENCES dim_aircraft(aircraft_key),
            scheduled_departure TIMESTAMP NOT NULL,
            actual_departure    TIMESTAMP,
            status              VARCHAR NOT NULL,
            delay_minutes       INTEGER DEFAULT 0,
            pax_count           INTEGER DEFAULT 0,
            load_factor         DOUBLE DEFAULT 0.0,
            fuel_kg             DOUBLE,
            _processed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "fact_bookings": """
        CREATE TABLE IF NOT EXISTS fact_bookings (
            booking_key     INTEGER PRIMARY KEY,
            reservation_id  VARCHAR NOT NULL,
            flight_number   VARCHAR NOT NULL,
            date_key        INTEGER REFERENCES dim_time(date_key),
            route_key       INTEGER REFERENCES dim_routes(route_key),
            airline_code    VARCHAR NOT NULL,
            fare_class      VARCHAR NOT NULL,
            fare_amount     DOUBLE NOT NULL,
            currency        VARCHAR DEFAULT 'VND',
            booking_channel VARCHAR,
            status          VARCHAR DEFAULT 'CONFIRMED',
            _processed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
}
