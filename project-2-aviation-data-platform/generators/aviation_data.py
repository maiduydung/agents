"""Synthetic aviation data generator — produces realistic Vietnamese airline data."""

from __future__ import annotations

import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from faker import Faker

from src.config import PlatformConfig, get_config
from src.observability.logger import get_logger

log = get_logger(__name__)

# Vietnamese locale for realistic names
fake = Faker(["vi_VN"])
Faker.seed(42)
random.seed(42)

# Vietnamese first/last names for crew
VIETNAMESE_LAST_NAMES = [
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Vo", "Dang",
    "Bui", "Do", "Ho", "Ngo", "Duong", "Ly", "Huynh",
]
VIETNAMESE_FIRST_NAMES = [
    "Anh", "Binh", "Cuong", "Dung", "Hanh", "Hoa", "Hung", "Khanh",
    "Linh", "Minh", "Nam", "Phuong", "Quang", "Son", "Thanh",
    "Thao", "Tuan", "Van", "Yen", "Duc",
]

# Approximate distances between Vietnamese airports (km)
ROUTE_DISTANCES: dict[tuple[str, str], int] = {
    ("SGN", "HAN"): 1180, ("SGN", "DAD"): 610, ("SGN", "CXR"): 320,
    ("SGN", "PQC"): 310, ("SGN", "HPH"): 1200, ("SGN", "VII"): 780,
    ("SGN", "VDO"): 1500, ("SGN", "UIH"): 480, ("SGN", "BMV"): 280,
    ("HAN", "DAD"): 620, ("HAN", "CXR"): 1100, ("HAN", "PQC"): 1500,
    ("HAN", "HPH"): 100, ("HAN", "VII"): 310, ("HAN", "VDO"): 200,
    ("DAD", "CXR"): 400, ("DAD", "PQC"): 800,
}

WEATHER_CONDITIONS = ["CLEAR", "FEW_CLOUDS", "SCATTERED", "OVERCAST", "RAIN", "THUNDERSTORM", "FOG"]


def _vn_name() -> str:
    last = random.choice(VIETNAMESE_LAST_NAMES)
    first = random.choice(VIETNAMESE_FIRST_NAMES)
    middle = random.choice(VIETNAMESE_FIRST_NAMES)
    return f"{last} {middle} {first}"


def _route_distance(origin: str, destination: str) -> int:
    """Lookup or estimate route distance."""
    key = (origin, destination)
    rev = (destination, origin)
    return ROUTE_DISTANCES.get(key, ROUTE_DISTANCES.get(rev, random.randint(200, 1500)))


def _generate_pnr() -> str:
    """Generate a 6-character PNR code."""
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choices(chars, k=6))


class AviationDataGenerator:
    """Generates synthetic aviation datasets and writes them as JSON lines."""

    def __init__(self, config: PlatformConfig | None = None) -> None:
        self.config = config or get_config()
        self.config.ensure_dirs()
        self._raw_dir = Path(self.config.data_dir) / "raw"

    def generate_all(
        self,
        num_records: int = 1000,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, int]:
        """Generate all source datasets and return record counts."""
        start = start_date or date.today() - timedelta(days=30)
        end = end_date or date.today()

        counts: dict[str, int] = {}
        counts["flights"] = self._generate_flights(num_records, start, end)
        counts["reservations"] = self._generate_reservations(num_records * 3, start, end)
        counts["crew"] = self._generate_crew(num_records, start, end)
        counts["weather"] = self._generate_weather(num_records, start, end)

        log.info("data_generation_complete", counts=counts)
        return counts

    def _generate_flights(self, count: int, start: date, end: date) -> int:
        """Generate flight operations data."""
        records: list[dict[str, Any]] = []
        days = (end - start).days or 1

        for i in range(count):
            airline = random.choice(self.config.airlines)
            origin, destination = random.sample(list(self.config.airports), 2)
            flight_day = start + timedelta(days=random.randint(0, days))
            dep_hour = random.randint(5, 23)
            dep_minute = random.choice([0, 15, 30, 45])
            sched_dep = datetime(flight_day.year, flight_day.month, flight_day.day, dep_hour, dep_minute)

            flight_hours = _route_distance(origin, destination) / 750
            sched_arr = sched_dep + timedelta(hours=flight_hours)

            status = random.choices(
                self.config.flight_statuses,
                weights=[30, 20, 30, 5, 2, 13],
                k=1,
            )[0]

            delay = 0
            actual_dep = None
            actual_arr = None
            if status in ("DEPARTED", "ARRIVED", "DELAYED"):
                delay = random.choices(
                    [0, random.randint(5, 30), random.randint(31, 120), random.randint(121, 360)],
                    weights=[50, 30, 15, 5],
                    k=1,
                )[0]
                actual_dep = sched_dep + timedelta(minutes=delay)
                actual_arr = sched_arr + timedelta(minutes=delay)

            aircraft_type = random.choice(airline.fleet)
            capacity = self.config.seat_capacity[aircraft_type]
            pax = random.randint(int(capacity * 0.5), capacity) if status != "CANCELLED" else 0

            record = {
                "flight_id": f"FL-{uuid.uuid4().hex[:12].upper()}",
                "flight_number": f"{airline.code}{random.randint(100, 999)}",
                "airline_code": airline.code,
                "aircraft_registration": f"VN-{chr(65 + random.randint(0, 25))}{random.randint(100, 999)}",
                "aircraft_type": aircraft_type,
                "origin": origin,
                "destination": destination,
                "scheduled_departure": sched_dep.isoformat(),
                "scheduled_arrival": sched_arr.isoformat(),
                "actual_departure": actual_dep.isoformat() if actual_dep else None,
                "actual_arrival": actual_arr.isoformat() if actual_arr else None,
                "status": status,
                "delay_minutes": delay,
                "pax_count": pax,
                "fuel_kg": round(random.uniform(3000, 12000), 1),
            }

            # Inject occasional bad data (~2%)
            if random.random() < 0.02:
                record["aircraft_type"] = random.choice(["UNKNOWN", "", "B737"])
            if random.random() < 0.01:
                record["delay_minutes"] = -999
            if random.random() < 0.02:
                record["origin"] = ""

            records.append(record)

        self._write_jsonl(records, "flights")
        return len(records)

    def _generate_reservations(self, count: int, start: date, end: date) -> int:
        """Generate reservation/booking data."""
        records: list[dict[str, Any]] = []
        days = (end - start).days or 1

        for _ in range(count):
            airline = random.choice(self.config.airlines)
            origin, destination = random.sample(list(self.config.airports), 2)
            dep_date = start + timedelta(days=random.randint(0, days))
            book_date = dep_date - timedelta(days=random.randint(1, 90))

            fare_class = random.choice(self.config.fare_classes)
            # Fare amount in VND — business class costs more
            base_fare = random.randint(500_000, 5_000_000)
            if fare_class in ("J", "C"):
                base_fare *= 3

            channel = random.choice(self.config.booking_channels)
            status = random.choices(
                ["CONFIRMED", "CANCELLED", "CHECKED_IN", "BOARDED", "NO_SHOW"],
                weights=[60, 10, 15, 10, 5],
                k=1,
            )[0]

            record = {
                "reservation_id": f"RES-{uuid.uuid4().hex[:12].upper()}",
                "flight_number": f"{airline.code}{random.randint(100, 999)}",
                "airline_code": airline.code,
                "passenger_name": _vn_name(),
                "booking_date": book_date.isoformat(),
                "departure_date": dep_date.isoformat(),
                "origin": origin,
                "destination": destination,
                "fare_class": fare_class,
                "fare_amount": float(base_fare),
                "currency": "VND",
                "booking_channel": channel,
                "pnr": _generate_pnr(),
                "status": status,
            }

            # Occasional bad data
            if random.random() < 0.01:
                record["fare_amount"] = -100.0
            if random.random() < 0.02:
                record["passenger_name"] = ""

            records.append(record)

        self._write_jsonl(records, "reservations")
        return len(records)

    def _generate_crew(self, count: int, start: date, end: date) -> int:
        """Generate crew assignment data."""
        records: list[dict[str, Any]] = []
        days = (end - start).days or 1

        roles = ["CAPTAIN", "FIRST_OFFICER", "CABIN_CREW", "PURSER"]
        role_weights = [15, 15, 55, 15]

        for _ in range(count):
            airline = random.choice(self.config.airlines)
            flight_day = start + timedelta(days=random.randint(0, days))
            role = random.choices(roles, weights=role_weights, k=1)[0]

            license_type = None
            if role in ("CAPTAIN", "FIRST_OFFICER"):
                license_type = random.choice(["ATPL", "CPL"])

            record = {
                "crew_id": f"CRW-{uuid.uuid4().hex[:12].upper()}",
                "employee_id": f"EMP-{random.randint(10000, 99999)}",
                "crew_name": _vn_name(),
                "role": role,
                "airline_code": airline.code,
                "flight_number": f"{airline.code}{random.randint(100, 999)}",
                "flight_date": flight_day.isoformat(),
                "license_type": license_type,
                "base_airport": random.choice(["SGN", "HAN", "DAD"]),
                "status": random.choices(["ACTIVE", "ON_LEAVE", "TRAINING"], weights=[85, 10, 5], k=1)[0],
            }

            # Occasional bad data
            if random.random() < 0.01:
                record["role"] = "INVALID_ROLE"

            records.append(record)

        self._write_jsonl(records, "crew")
        return len(records)

    def _generate_weather(self, count: int, start: date, end: date) -> int:
        """Generate weather observation data."""
        records: list[dict[str, Any]] = []
        days = (end - start).days or 1

        for _ in range(count):
            airport = random.choice(self.config.airports)
            obs_day = start + timedelta(days=random.randint(0, days))
            obs_hour = random.randint(0, 23)
            obs_time = datetime(obs_day.year, obs_day.month, obs_day.day, obs_hour, 0)

            # Temperature varies by region — South Vietnam is hotter
            base_temp = 32.0 if airport in ("SGN", "PQC", "CXR", "BMV") else 26.0
            temp = round(base_temp + random.uniform(-8, 8), 1)

            record = {
                "observation_id": f"WX-{uuid.uuid4().hex[:12].upper()}",
                "airport_code": airport,
                "observed_at": obs_time.isoformat(),
                "temperature_c": temp,
                "wind_speed_kts": random.randint(0, 35),
                "wind_direction": random.randint(0, 360),
                "visibility_km": round(random.uniform(1.0, 15.0), 1),
                "condition": random.choice(WEATHER_CONDITIONS),
                "ceiling_ft": random.choice([500, 1000, 2000, 5000, 10000, 25000]),
            }

            # Occasional bad data
            if random.random() < 0.01:
                record["temperature_c"] = 999.0
            if random.random() < 0.01:
                record["wind_speed_kts"] = -5

            records.append(record)

        self._write_jsonl(records, "weather")
        return len(records)

    def _write_jsonl(self, records: list[dict[str, Any]], source_name: str) -> None:
        """Write records as JSON lines to the raw data directory."""
        filepath = self._raw_dir / f"{source_name}.jsonl"
        with open(filepath, "w") as f:
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
        log.info("raw_data_written", source=source_name, file=str(filepath), records=len(records))
