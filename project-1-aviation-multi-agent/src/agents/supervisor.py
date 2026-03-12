"""Supervisor / Router Agent — classifies incoming queries and routes to specialists."""

from __future__ import annotations

import json
import logging
import time
from typing import Literal

from langchain_core.messages import AIMessage, SystemMessage
from langchain_anthropic import ChatAnthropic

from src.config import LLM_MODEL, LLM_TEMPERATURE, ANTHROPIC_API_KEY
from src.models import AgentState

logger = logging.getLogger("aviation.supervisor")

SUPERVISOR_SYSTEM_PROMPT = """\
You are the Supervisor Agent for an airline operations control center.
Your only job is to classify the user's query and route it to the correct
specialist agent. You do NOT answer the question yourself.

The available specialist agents are:

1. **flight_ops** — handles questions about:
   - Flight status, delays, cancellations
   - Gate and terminal assignments
   - Flight schedules and routes
   - Aircraft type information

2. **revenue** — handles questions about:
   - Booking data and load factors
   - Revenue and fare analysis
   - Pricing policy and overbooking rules
   - Ancillary revenue
   - Revenue management policies

3. **crew** — handles questions about:
   - Crew scheduling and assignments
   - Rest compliance and fatigue management
   - Crew qualifications and aircraft ratings
   - Flight time limitations
   - Crew management policies

Respond with ONLY a JSON object in this exact format:
{"agent": "<agent_name>", "reason": "<one-sentence justification>"}

Where <agent_name> is one of: flight_ops, revenue, crew.
Do NOT include any other text outside the JSON object.
"""

AgentName = Literal["flight_ops", "revenue", "crew"]


def _get_supervisor_llm() -> ChatAnthropic:
    """Build the supervisor LLM (no tools — routing only)."""
    return ChatAnthropic(
        model=LLM_MODEL,
        temperature=0,
        max_tokens=1024,
        api_key=ANTHROPIC_API_KEY,
    )


def _parse_routing_decision(content: str) -> tuple[AgentName, str]:
    """Extract the agent name and reason from the supervisor's JSON response.

    Falls back to ``flight_ops`` if parsing fails.
    """
    try:
        # Strip markdown code fences if the model wraps its output.
        cleaned = content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned)
        agent = data.get("agent", "flight_ops")
        reason = data.get("reason", "")
        if agent not in ("flight_ops", "revenue", "crew"):
            agent = "flight_ops"
        return agent, reason  # type: ignore[return-value]
    except (json.JSONDecodeError, AttributeError, KeyError):
        return "flight_ops", "Could not parse routing decision — defaulting to flight_ops."


def supervisor_node(state: AgentState) -> dict:
    """LangGraph node — classify the query and set ``current_agent``."""
    logger.info("🧠 Supervisor agent activated — classifying query …")
    t0 = time.perf_counter()

    llm = _get_supervisor_llm()
    messages = [SystemMessage(content=SUPERVISOR_SYSTEM_PROMPT)] + list(state["messages"])

    logger.debug("📤 Sending classification request to LLM (%s)", LLM_MODEL)
    response = llm.invoke(messages)
    elapsed = time.perf_counter() - t0

    agent_name, reason = _parse_routing_decision(response.content)

    logger.info("🎯 Routing to [%s] agent (%.2fs) │ Reason: %s", agent_name, elapsed, reason)

    routing_msg = AIMessage(
        content=f"[Supervisor] Routing to **{agent_name}** agent. Reason: {reason}"
    )

    return {
        "messages": [routing_msg],
        "current_agent": agent_name,
        "metadata": {"routing_reason": reason, "routed_to": agent_name},
    }
