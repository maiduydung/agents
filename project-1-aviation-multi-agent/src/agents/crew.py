"""Crew Management Agent — scheduling, rest compliance, crew assignments."""

from __future__ import annotations

import logging
import time

from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langchain_anthropic import ChatAnthropic

from src.config import LLM_MODEL, LLM_TEMPERATURE, ANTHROPIC_API_KEY
from src.models import AgentState
from src.rag.retriever import search_aviation_policies
from src.tools.crew_tools import (
    check_crew_rest_compliance,
    get_crew_assignments,
    get_crew_by_aircraft_type,
)

logger = logging.getLogger("aviation.crew")

CREW_TOOLS = [
    check_crew_rest_compliance,
    get_crew_assignments,
    get_crew_by_aircraft_type,
    search_aviation_policies,
]

CREW_SYSTEM_PROMPT = """\
You are a Crew Management Specialist for a Vietnamese airline group.
Your role is to handle crew scheduling, rest compliance, and assignment queries.

You have access to the following tools:
- check_crew_rest_compliance: check whether crew members meet minimum rest requirements
- get_crew_assignments: see which crew are assigned to which flights
- get_crew_by_aircraft_type: find crew rated for a specific aircraft type
- search_aviation_policies: semantic search over airline policy documents
  (rest requirements, fatigue risk thresholds, flight-time limitations, etc.)

When answering:
- Always flag rest violations clearly — these are safety-critical.
- Reference fatigue risk scores and their meaning (low/moderate/high/critical).
- Suggest replacement crew when violations are found.
- Cite specific policy thresholds (e.g. "minimum 10h rest between duties").
- Prioritise safety over schedule adherence in all recommendations.
"""


def _get_crew_llm() -> ChatAnthropic:
    """Build the LLM with crew tools bound."""
    llm = ChatAnthropic(
        model=LLM_MODEL,
        temperature=LLM_TEMPERATURE,
        max_tokens=2048,
        api_key=ANTHROPIC_API_KEY,
    )
    return llm.bind_tools(CREW_TOOLS)


def crew_node(state: AgentState) -> dict:
    """LangGraph node — invoke the Crew Management agent."""
    logger.info("👨‍✈️ Crew agent activated")
    t0 = time.perf_counter()

    llm = _get_crew_llm()
    messages = [SystemMessage(content=CREW_SYSTEM_PROMPT)] + list(state["messages"])

    logger.debug("📤 Sending request to LLM with %d tools bound", len(CREW_TOOLS))
    response = llm.invoke(messages)

    if response.tool_calls:
        tool_map = {t.name: t for t in CREW_TOOLS}
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

        logger.debug("📤 Sending tool results back to LLM for synthesis …")
        followup = llm.invoke(messages + [response] + tool_messages)
        elapsed = time.perf_counter() - t0
        logger.info("✅ Crew complete in %.2fs", elapsed)
        return {
            "messages": [response] + tool_messages + [followup],
            "current_agent": "crew",
        }

    elapsed = time.perf_counter() - t0
    logger.info("✅ Crew complete in %.2fs (no tool calls)", elapsed)
    return {"messages": [response], "current_agent": "crew"}
