"""Flight Operations Agent — handles flight status, gates, schedules."""

from __future__ import annotations

import logging
import time

from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langchain_anthropic import ChatAnthropic

from src.config import LLM_MODEL, LLM_TEMPERATURE, ANTHROPIC_API_KEY
from src.models import AgentState
from src.tools.flight_tools import get_flight_status, get_gate_info, search_flights

logger = logging.getLogger("aviation.flight_ops")

FLIGHT_OPS_TOOLS = [get_flight_status, get_gate_info, search_flights]

FLIGHT_OPS_SYSTEM_PROMPT = """\
You are a Flight Operations Specialist for a Vietnamese airline group.
Your job is to help operations teams with real-time flight information.

You have access to the following tools:
- get_flight_status: look up a specific flight by its ID
- get_gate_info: check gate/terminal assignments
- search_flights: search flights by origin, destination, or status

When answering:
- Always cite the flight ID and specific data points.
- If a flight is delayed, mention the delay reason and duration.
- Provide actionable recommendations when relevant (e.g. gate changes, rebooking).
- Use clear, concise language appropriate for an operations control center.
"""


def _get_flight_ops_llm() -> ChatAnthropic:
    """Build the LLM with flight ops tools bound."""
    llm = ChatAnthropic(
        model=LLM_MODEL,
        temperature=LLM_TEMPERATURE,
        max_tokens=2048,
        api_key=ANTHROPIC_API_KEY,
    )
    return llm.bind_tools(FLIGHT_OPS_TOOLS)


def flight_ops_node(state: AgentState) -> dict:
    """LangGraph node — invoke the Flight Operations agent."""
    logger.info("✈️  Flight Ops agent activated")
    t0 = time.perf_counter()

    llm = _get_flight_ops_llm()
    messages = [SystemMessage(content=FLIGHT_OPS_SYSTEM_PROMPT)] + list(state["messages"])

    logger.debug("📤 Sending request to LLM with %d tools bound", len(FLIGHT_OPS_TOOLS))
    response = llm.invoke(messages)

    # If the LLM wants to call tools, execute them and re-invoke.
    if response.tool_calls:
        tool_map = {t.name: t for t in FLIGHT_OPS_TOOLS}
        tool_messages = []
        for tc in response.tool_calls:
            logger.info("🔧 Tool call: %s(%s)", tc["name"], tc["args"])
            t_tool = time.perf_counter()
            tool_fn = tool_map[tc["name"]]
            result = tool_fn.invoke(tc["args"])
            logger.debug("📥 Tool [%s] returned in %.3fs", tc["name"], time.perf_counter() - t_tool)
            tool_messages.append(
                ToolMessage(content=str(result), tool_call_id=tc["id"])
            )

        # Second pass — let the LLM synthesise a human-readable answer.
        logger.debug("📤 Sending tool results back to LLM for synthesis …")
        followup = llm.invoke(messages + [response] + tool_messages)
        elapsed = time.perf_counter() - t0
        logger.info("✅ Flight Ops complete in %.2fs", elapsed)
        return {
            "messages": [response] + tool_messages + [followup],
            "current_agent": "flight_ops",
        }

    elapsed = time.perf_counter() - t0
    logger.info("✅ Flight Ops complete in %.2fs (no tool calls)", elapsed)
    return {"messages": [response], "current_agent": "flight_ops"}
