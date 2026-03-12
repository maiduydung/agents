"""Crew management tools — backed by crew.json."""

from __future__ import annotations

import json
from typing import Any

from langchain_core.tools import tool

from src.config import CREW_JSON


def _load_crew() -> list[dict[str, Any]]:
    """Load crew records from the JSON data file."""
    with open(CREW_JSON, encoding="utf-8") as fh:
        return json.load(fh)


@tool
def check_crew_rest_compliance(crew_id: str | None = None) -> str:
    """Check rest compliance for a specific crew member (by crew_id) or all crew.

    If crew_id is not provided, returns compliance status for every crew member.
    Flags violations where rest_hours_current < min_rest_required.
    """
    crew_members = _load_crew()

    if crew_id:
        crew_id_upper = crew_id.strip().upper()
        for member in crew_members:
            if member["crew_id"].upper() == crew_id_upper:
                status = "COMPLIANT" if member["rest_compliant"] else "VIOLATION"
                lines = [
                    f"Rest Compliance for {member['name']} ({member['crew_id']}):",
                    f"  Role: {member['role'].replace('_', ' ').title()}",
                    f"  Status: {status}",
                    f"  Current Rest: {member['rest_hours_current']} hours",
                    f"  Minimum Required: {member['min_rest_required']} hours",
                    f"  Last Duty End: {member['last_duty_end']}",
                    f"  Next Duty Start: {member['next_duty_start']}",
                    f"  Fatigue Risk Score: {member['fatigue_risk_score']:.2f}",
                ]
                if not member["rest_compliant"]:
                    deficit = member["min_rest_required"] - member["rest_hours_current"]
                    lines.append(f"  REST DEFICIT: {deficit:.1f} hours — crew member must NOT fly")
                return "\n".join(lines)
        return f"No crew member found with ID '{crew_id_upper}'."

    # All crew
    compliant = [m for m in crew_members if m["rest_compliant"]]
    violations = [m for m in crew_members if not m["rest_compliant"]]

    lines = [
        f"=== Crew Rest Compliance Overview ===",
        f"Total Crew: {len(crew_members)}",
        f"Compliant: {len(compliant)}",
        f"Violations: {len(violations)}",
    ]

    if violations:
        lines.append("\nREST VIOLATIONS:")
        for m in violations:
            deficit = m["min_rest_required"] - m["rest_hours_current"]
            lines.append(
                f"  {m['crew_id']} {m['name']} ({m['role']}) — "
                f"rest: {m['rest_hours_current']}h / required: {m['min_rest_required']}h "
                f"(deficit: {deficit:.1f}h) — fatigue risk: {m['fatigue_risk_score']:.2f}"
            )

    return "\n".join(lines)


@tool
def get_crew_assignments(flight_id: str | None = None) -> str:
    """Get crew assigned to a specific flight, or list all crew with their assignments.

    If flight_id is provided, returns only crew assigned to that flight.
    """
    crew_members = _load_crew()

    if flight_id:
        flight_id_upper = flight_id.strip().upper()
        assigned = [m for m in crew_members if flight_id_upper in m["assigned_flights"]]
        if not assigned:
            return f"No crew currently assigned to flight '{flight_id_upper}'."
        lines = [f"Crew assigned to {flight_id_upper}:\n"]
        for m in assigned:
            compliance = "COMPLIANT" if m["rest_compliant"] else "VIOLATION"
            lines.append(
                f"  {m['crew_id']} | {m['name']} | {m['role'].replace('_', ' ').title()} | "
                f"Rest: {compliance} | Fatigue: {m['fatigue_risk_score']:.2f} | "
                f"Flight Hours (30d): {m['hours_last_30_days']}h"
            )
        return "\n".join(lines)

    # All assignments
    lines = ["=== All Crew Assignments ===\n"]
    for m in crew_members:
        flights_str = ", ".join(m["assigned_flights"]) if m["assigned_flights"] else "None"
        compliance = "COMPLIANT" if m["rest_compliant"] else "VIOLATION"
        lines.append(
            f"  {m['crew_id']} | {m['name']} | {m['role'].replace('_', ' ').title()} | "
            f"Flights: {flights_str} | Status: {m['status']} | Rest: {compliance}"
        )
    return "\n".join(lines)


@tool
def get_crew_by_aircraft_type(aircraft_type: str) -> str:
    """Find crew members rated for a specific aircraft type (e.g. A320, A350, B787).

    Useful for finding replacement crew when reassignment is needed.
    """
    crew_members = _load_crew()
    aircraft_upper = aircraft_type.strip().upper()

    rated = [m for m in crew_members if aircraft_upper in m["aircraft_ratings"]]

    if not rated:
        return f"No crew members found rated for aircraft type '{aircraft_upper}'."

    available = [m for m in rated if m["rest_compliant"] and m["status"] == "available"]

    lines = [
        f"Crew rated for {aircraft_upper}: {len(rated)} total, {len(available)} available\n",
        "All rated crew:",
    ]
    for m in rated:
        compliance = "COMPLIANT" if m["rest_compliant"] else "VIOLATION"
        lines.append(
            f"  {m['crew_id']} | {m['name']} | {m['role'].replace('_', ' ').title()} | "
            f"Status: {m['status']} | Rest: {compliance} | "
            f"Base: {m['base']} | Hours (7d): {m['hours_last_7_days']}h"
        )

    return "\n".join(lines)
