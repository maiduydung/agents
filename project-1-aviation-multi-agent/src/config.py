"""Configuration for the Aviation Multi-Agent system."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
DOCS_DIR: Path = DATA_DIR / "docs"
FLIGHTS_JSON: Path = DATA_DIR / "flights.json"
BOOKINGS_JSON: Path = DATA_DIR / "bookings.json"
CREW_JSON: Path = DATA_DIR / "crew.json"
CHROMA_PERSIST_DIR: Path = PROJECT_ROOT / ".chromadb"

# ---------------------------------------------------------------------------
# LLM (Anthropic Claude)
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
LLM_MODEL: str = os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001")
LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0"))

# ---------------------------------------------------------------------------
# Embeddings (local HuggingFace — no API key needed)
# ---------------------------------------------------------------------------
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

# ---------------------------------------------------------------------------
# ChromaDB
# ---------------------------------------------------------------------------
CHROMA_COLLECTION_NAME: str = "aviation_policies"

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
