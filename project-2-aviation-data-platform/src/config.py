"""Configuration for the Aviation Data Platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class AirlineConfig:
    """Airline-specific configuration."""

    code: str
    name: str
    fleet: list[str]


@dataclass(frozen=True)
class PlatformConfig:
    """Central configuration for the aviation data platform."""

    # Paths
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    db_path: str = ""
    data_dir: str = ""
    sql_dir: str = ""

    # Airlines operating in Vietnam
    airlines: tuple[AirlineConfig, ...] = (
        AirlineConfig("VJ", "VietJet Air", ["A320", "A321"]),
        AirlineConfig("VN", "Vietnam Airlines", ["A321", "B787", "A350"]),
        AirlineConfig("QH", "Bamboo Airways", ["A320", "A321", "B787"]),
    )

    # Vietnamese airport codes
    airports: tuple[str, ...] = ("SGN", "HAN", "DAD", "CXR", "PQC", "HPH", "VII", "VDO", "UIH", "BMV")

    # Fare classes
    fare_classes: tuple[str, ...] = ("Y", "M", "B", "H", "K", "L", "V", "W", "J", "C")

    # Flight statuses
    flight_statuses: tuple[str, ...] = ("SCHEDULED", "DEPARTED", "ARRIVED", "CANCELLED", "DIVERTED", "DELAYED")

    # Booking channels
    booking_channels: tuple[str, ...] = ("WEB", "MOBILE", "AGENT", "OTA", "CORPORATE", "CALL_CENTER")

    # Quality thresholds
    max_null_rate: float = 0.05
    max_rejection_rate: float = 0.05
    freshness_hours: int = 24

    # Aircraft seat capacities
    seat_capacity: dict[str, int] = field(default_factory=lambda: {
        "A320": 180,
        "A321": 220,
        "A350": 305,
        "B787": 274,
    })

    def __post_init__(self) -> None:
        root = self.project_root
        if not self.db_path:
            object.__setattr__(self, "db_path", str(root / "data" / "aviation.duckdb"))
        if not self.data_dir:
            object.__setattr__(self, "data_dir", str(root / "data"))
        if not self.sql_dir:
            object.__setattr__(self, "sql_dir", str(root / "sql"))

    def ensure_dirs(self) -> None:
        """Create necessary directories."""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        for sub in ("raw", "staging", "logs"):
            (Path(self.data_dir) / sub).mkdir(exist_ok=True)


def get_config() -> PlatformConfig:
    """Return the platform configuration singleton."""
    return PlatformConfig()
