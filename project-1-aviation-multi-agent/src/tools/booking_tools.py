"""Booking and revenue management tools — backed by bookings.json."""

from __future__ import annotations

import json
from typing import Any

from langchain_core.tools import tool

from src.config import BOOKINGS_JSON


def _load_bookings() -> list[dict[str, Any]]:
    """Load booking records from the JSON data file."""
    with open(BOOKINGS_JSON, encoding="utf-8") as fh:
        return json.load(fh)


@tool
def get_load_factor(flight_id: str) -> str:
    """Get the load factor and seat availability for a specific flight.

    Returns booked vs. total seats and load factor percentage.
    """
    bookings = _load_bookings()
    flight_id_upper = flight_id.strip().upper()

    for booking in bookings:
        if booking["flight_id"] == flight_id_upper:
            lf_pct = booking["load_factor"] * 100
            available = booking["total_seats"] - booking["booked_seats"]
            lines = [
                f"Flight {booking['flight_id']} Load Factor: {lf_pct:.1f}%",
                f"  Total Seats: {booking['total_seats']}",
                f"  Booked: {booking['booked_seats']}",
                f"  Available: {available}",
                "",
                "  Breakdown by class:",
            ]
            for cls_name, cls_data in booking["booking_classes"].items():
                cls_avail = cls_data["seats"] - cls_data["booked"]
                lines.append(
                    f"    {cls_name.replace('_', ' ').title()}: "
                    f"{cls_data['booked']}/{cls_data['seats']} seats "
                    f"({cls_avail} available, avg fare ${cls_data['avg_fare']:.0f})"
                )
            return "\n".join(lines)

    return f"No booking data found for flight '{flight_id_upper}'."


@tool
def get_booking_stats(flight_id: str) -> str:
    """Get detailed booking statistics for a flight including no-show rate,
    cancellation rate, ancillary revenue, and booking date distribution.
    """
    bookings = _load_bookings()
    flight_id_upper = flight_id.strip().upper()

    for booking in bookings:
        if booking["flight_id"] == flight_id_upper:
            dist = booking.get("booking_date_distribution", {})
            lines = [
                f"Booking Statistics for {booking['flight_id']}:",
                f"  No-show Rate: {booking['no_show_rate'] * 100:.1f}%",
                f"  Cancellation Rate: {booking['cancellation_rate'] * 100:.1f}%",
                f"  Ancillary Revenue: ${booking['ancillary_revenue_usd']:,.0f}",
                f"  Average Fare: ${booking['avg_fare_usd']:.2f}",
                "",
                "  Booking Window Distribution:",
            ]
            for window, pct in dist.items():
                label = window.replace("_", " ").replace("plus", "+")
                lines.append(f"    {label} days before departure: {pct * 100:.0f}%")
            return "\n".join(lines)

    return f"No booking statistics found for flight '{flight_id_upper}'."


@tool
def get_revenue_summary() -> str:
    """Get an overall revenue summary across all flights in the dataset.

    Returns total revenue, average load factor, and highlights flights
    that need attention (low load factor or high no-show rates).
    """
    bookings = _load_bookings()

    total_revenue = sum(b["revenue_usd"] for b in bookings)
    total_ancillary = sum(b["ancillary_revenue_usd"] for b in bookings)
    avg_load_factor = sum(b["load_factor"] for b in bookings) / len(bookings) if bookings else 0
    avg_no_show = sum(b["no_show_rate"] for b in bookings) / len(bookings) if bookings else 0

    low_lf_flights = [b for b in bookings if b["load_factor"] < 0.75]
    high_ns_flights = [b for b in bookings if b["no_show_rate"] >= 0.05]

    lines = [
        "=== Revenue Summary ===",
        f"Total Flights: {len(bookings)}",
        f"Total Ticket Revenue: ${total_revenue:,.0f}",
        f"Total Ancillary Revenue: ${total_ancillary:,.0f}",
        f"Combined Revenue: ${total_revenue + total_ancillary:,.0f}",
        f"Average Load Factor: {avg_load_factor * 100:.1f}%",
        f"Average No-show Rate: {avg_no_show * 100:.1f}%",
    ]

    if low_lf_flights:
        lines.append("\nFlights with LOW load factor (<75%):")
        for b in low_lf_flights:
            lines.append(f"  {b['flight_id']}: {b['load_factor'] * 100:.1f}% — needs yield management attention")

    if high_ns_flights:
        lines.append("\nFlights with HIGH no-show rate (>=5%):")
        for b in high_ns_flights:
            lines.append(f"  {b['flight_id']}: {b['no_show_rate'] * 100:.1f}%")

    return "\n".join(lines)
