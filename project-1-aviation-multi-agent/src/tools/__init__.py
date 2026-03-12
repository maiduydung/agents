"""Tool definitions for specialist agents."""

from src.tools.booking_tools import get_booking_stats, get_load_factor, get_revenue_summary
from src.tools.crew_tools import (
    check_crew_rest_compliance,
    get_crew_assignments,
    get_crew_by_aircraft_type,
)
from src.tools.flight_tools import get_flight_status, get_gate_info, search_flights

__all__ = [
    "get_flight_status",
    "get_gate_info",
    "search_flights",
    "get_booking_stats",
    "get_load_factor",
    "get_revenue_summary",
    "check_crew_rest_compliance",
    "get_crew_assignments",
    "get_crew_by_aircraft_type",
]
