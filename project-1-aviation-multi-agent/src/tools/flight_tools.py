"""Flight operations tools — simulated data backed by flights.json."""

from __future__ import annotations

import json
from typing import Any

from langchain_core.tools import tool

from src.config import FLIGHTS_JSON


def _load_flights() -> list[dict[str, Any]]:
    """Load flight records from the JSON data file."""
    with open(FLIGHTS_JSON, encoding="utf-8") as fh:
        return json.load(fh)


@tool
def get_flight_status(flight_id: str) -> str:
    """Look up the current status of a specific flight by its flight ID (e.g. VJ101, VN302).

    Returns departure/arrival times, delay information, gate, and current status.
    """
    flights = _load_flights()
    flight_id_upper = flight_id.strip().upper()

    for flight in flights:
        if flight["flight_id"] == flight_id_upper:
            parts = [
                f"Flight: {flight['flight_id']} ({flight['airline']})",
                f"Route: {flight['origin']} -> {flight['destination']}",
                f"Status: {flight['status'].upper()}",
                f"Scheduled Departure: {flight['scheduled_departure']}",
                f"Scheduled Arrival: {flight['scheduled_arrival']}",
            ]
            if flight.get("actual_departure"):
                parts.append(f"Actual Departure: {flight['actual_departure']}")
            if flight.get("actual_arrival"):
                parts.append(f"Actual Arrival: {flight['actual_arrival']}")
            if flight["delay_minutes"] > 0:
                parts.append(f"Delay: {flight['delay_minutes']} minutes")
                parts.append(f"Delay Reason: {flight.get('delay_reason', 'unknown')}")
            parts.append(f"Aircraft: {flight.get('aircraft_type', 'N/A')}")
            parts.append(f"Gate: {flight.get('gate', 'N/A')} | Terminal: {flight.get('terminal', 'N/A')}")
            return "\n".join(parts)

    return f"No flight found with ID '{flight_id_upper}'."


@tool
def get_gate_info(flight_id: str) -> str:
    """Get the gate and terminal assignment for a specific flight."""
    flights = _load_flights()
    flight_id_upper = flight_id.strip().upper()

    for flight in flights:
        if flight["flight_id"] == flight_id_upper:
            return (
                f"Flight {flight['flight_id']}: "
                f"Gate {flight.get('gate', 'TBD')} | "
                f"Terminal {flight.get('terminal', 'TBD')} | "
                f"Status: {flight['status']}"
            )

    return f"No gate information found for flight '{flight_id_upper}'."


@tool
def search_flights(
    origin: str | None = None,
    destination: str | None = None,
    status: str | None = None,
) -> str:
    """Search for flights by origin airport, destination airport, and/or status.

    All parameters are optional — provide any combination to filter.
    Airport codes should be 3-letter IATA codes (e.g. SGN, HAN, DAD).
    Status options: scheduled, on_time, delayed, boarding, departed, arrived, cancelled.
    """
    flights = _load_flights()
    results: list[dict[str, Any]] = []

    for flight in flights:
        if origin and flight["origin"].upper() != origin.strip().upper():
            continue
        if destination and flight["destination"].upper() != destination.strip().upper():
            continue
        if status and flight["status"].lower() != status.strip().lower():
            continue
        results.append(flight)

    if not results:
        filters = []
        if origin:
            filters.append(f"origin={origin}")
        if destination:
            filters.append(f"destination={destination}")
        if status:
            filters.append(f"status={status}")
        return f"No flights found matching filters: {', '.join(filters) or 'none'}."

    lines: list[str] = [f"Found {len(results)} flight(s):\n"]
    for f in results:
        delay_info = f" (delayed {f['delay_minutes']}min)" if f["delay_minutes"] > 0 else ""
        lines.append(
            f"  {f['flight_id']} | {f['origin']}->{f['destination']} | "
            f"{f['status'].upper()}{delay_info} | "
            f"Dep: {f['scheduled_departure']} | Gate: {f.get('gate', 'TBD')}"
        )
    return "\n".join(lines)
