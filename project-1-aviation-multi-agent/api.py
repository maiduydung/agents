"""FastAPI application — REST endpoint for the multi-agent system."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from langchain_core.messages import HumanMessage

from src.config import API_HOST, API_PORT
from src.graph import graph
from src.models import QueryRequest, QueryResponse

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    logger.info("Aviation Multi-Agent API starting up …")
    yield
    logger.info("Aviation Multi-Agent API shutting down.")


app = FastAPI(
    title="Aviation Multi-Agent Operations Assistant",
    description=(
        "A multi-agent system powered by LangGraph that routes airline operations "
        "queries to specialist agents for flight ops, revenue management, and crew scheduling."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Simple health-check endpoint."""
    return {"status": "healthy", "service": "aviation-multi-agent"}


@app.post("/query", response_model=QueryResponse)
async def handle_query(request: QueryRequest) -> QueryResponse:
    """Process a natural-language query through the multi-agent graph.

    The supervisor agent classifies the query and routes it to the appropriate
    specialist agent (flight_ops, revenue, or crew).  The specialist invokes
    its tools and returns a synthesised answer.
    """
    start = time.time()

    try:
        initial_state = {
            "messages": [HumanMessage(content=request.query)],
            "current_agent": "",
            "metadata": {},
        }

        result = graph.invoke(initial_state)

        final_messages = result.get("messages", [])
        answer = final_messages[-1].content if final_messages else "No response generated."
        agent_used = result.get("current_agent", "unknown")
        metadata = result.get("metadata", {})
        metadata["latency_seconds"] = round(time.time() - start, 3)

        return QueryResponse(
            answer=answer,
            agent_used=agent_used,
            metadata=metadata,
        )

    except Exception as exc:
        logger.exception("Error processing query: %s", request.query)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/agents")
async def list_agents() -> dict[str, list[dict[str, str]]]:
    """List the available specialist agents and their capabilities."""
    return {
        "agents": [
            {
                "name": "flight_ops",
                "description": "Flight status, delays, gate assignments, schedule queries",
                "tools": "get_flight_status, get_gate_info, search_flights",
            },
            {
                "name": "revenue",
                "description": "Booking analytics, load factors, pricing, policy-based recommendations",
                "tools": "get_load_factor, get_booking_stats, get_revenue_summary, search_aviation_policies",
            },
            {
                "name": "crew",
                "description": "Crew scheduling, rest compliance, fatigue management, crew assignments",
                "tools": "check_crew_rest_compliance, get_crew_assignments, get_crew_by_aircraft_type, search_aviation_policies",
            },
        ]
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=True)
