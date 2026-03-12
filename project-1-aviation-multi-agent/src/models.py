"""Pydantic data models and LangGraph state definitions."""

from __future__ import annotations

import operator
from typing import Annotated, Any, Literal, Sequence

from langchain_core.messages import BaseMessage
from pydantic import BaseModel, Field
from typing_extensions import TypedDict


# ---------------------------------------------------------------------------
# Graph State
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    """Shared state that flows through the LangGraph workflow.

    Attributes:
        messages: The full conversation history (append-only via operator.add).
        current_agent: Which specialist agent is currently handling the query.
        metadata: Arbitrary metadata attached to the current invocation.
    """

    messages: Annotated[Sequence[BaseMessage], operator.add]
    current_agent: str
    metadata: dict[str, Any]


# ---------------------------------------------------------------------------
# Domain Models — Flight
# ---------------------------------------------------------------------------

class FlightInfo(BaseModel):
    """A single flight record."""

    flight_id: str = Field(..., description="IATA flight designator, e.g. VJ101")
    airline: str
    origin: str = Field(..., description="3-letter IATA airport code")
    destination: str = Field(..., description="3-letter IATA airport code")
    scheduled_departure: str
    scheduled_arrival: str
    actual_departure: str | None = None
    actual_arrival: str | None = None
    status: Literal[
        "scheduled", "on_time", "delayed", "boarding", "departed", "arrived", "cancelled"
    ]
    gate: str | None = None
    terminal: str | None = None
    aircraft_type: str | None = None
    delay_minutes: int = 0
    delay_reason: str | None = None


# ---------------------------------------------------------------------------
# Domain Models — Booking / Revenue
# ---------------------------------------------------------------------------

class BookingClassInfo(BaseModel):
    """Booking details for a single cabin class."""

    seats: int
    booked: int
    avg_fare: float


class BookingInfo(BaseModel):
    """Aggregated booking data for a flight."""

    flight_id: str
    total_seats: int
    booked_seats: int
    load_factor: float = Field(..., ge=0, le=1)
    revenue_usd: float
    avg_fare_usd: float
    booking_classes: dict[str, BookingClassInfo]
    ancillary_revenue_usd: float = 0
    no_show_rate: float = 0
    cancellation_rate: float = 0
    booking_date_distribution: dict[str, float] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Domain Models — Crew
# ---------------------------------------------------------------------------

class CrewMember(BaseModel):
    """A flight- or cabin-crew member."""

    crew_id: str
    name: str
    role: Literal["captain", "first_officer", "cabin_crew_lead", "cabin_crew"]
    airline: str
    base: str
    license_type: str
    aircraft_ratings: list[str]
    total_flight_hours: int
    hours_last_30_days: int
    hours_last_7_days: int
    last_duty_end: str
    next_duty_start: str
    rest_hours_current: float
    min_rest_required: float
    rest_compliant: bool
    assigned_flights: list[str]
    status: str
    fatigue_risk_score: float = Field(..., ge=0, le=1)


# ---------------------------------------------------------------------------
# API request / response
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    """Incoming API request."""

    query: str = Field(..., min_length=1, description="Natural-language question")
    session_id: str | None = Field(
        default=None, description="Optional session identifier for conversation continuity"
    )


class QueryResponse(BaseModel):
    """API response."""

    answer: str
    agent_used: str = Field(..., description="Which specialist agent handled the query")
    metadata: dict[str, Any] = Field(default_factory=dict)
