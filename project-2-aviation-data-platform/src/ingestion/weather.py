"""Weather data ingestion."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.observability.logger import get_logger

log = get_logger(__name__)


def load_raw_weather(data_dir: str) -> list[dict[str, Any]]:
    """Load raw weather observation records from JSONL file."""
    filepath = Path(data_dir) / "raw" / "weather.jsonl"
    if not filepath.exists():
        log.warning("weather_file_not_found", path=str(filepath))
        return []

    records: list[dict[str, Any]] = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    log.info("weather_loaded", count=len(records))
    return records
