"""Crew management data ingestion."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.observability.logger import get_logger

log = get_logger(__name__)


def load_raw_crew(data_dir: str) -> list[dict[str, Any]]:
    """Load raw crew assignment records from JSONL file."""
    filepath = Path(data_dir) / "raw" / "crew.jsonl"
    if not filepath.exists():
        log.warning("crew_file_not_found", path=str(filepath))
        return []

    records: list[dict[str, Any]] = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    log.info("crew_loaded", count=len(records))
    return records
