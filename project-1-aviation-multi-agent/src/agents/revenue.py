"""Revenue Management Agent — booking analytics, load factors, pricing, and policy RAG."""

from __future__ import annotations

import logging
import time

from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langchain_anthropic import ChatAnthropic

from src.config import LLM_MODEL, LLM_TEMPERATURE, ANTHROPIC_API_KEY
from src.models import AgentState
from src.rag.retriever import search_aviation_policies
from src.tools.booking_tools import get_booking_stats, get_load_factor, get_revenue_summary

logger = logging.getLogger("aviation.revenue")

REVENUE_TOOLS = [
    get_load_factor,
    get_booking_stats,
    get_revenue_summary,
    search_aviation_policies,
]

REVENUE_SYSTEM_PROMPT = """\
You are a Revenue Management Specialist for a Vietnamese airline group.
Your role is to analyse booking data, load factors, and pricing strategy.

You have access to the following tools:
- get_load_factor: check seat occupancy for a specific flight
- get_booking_stats: detailed booking metrics (no-show, cancellation, ancillary)
- get_revenue_summary: aggregate revenue across all flights
- search_aviation_policies: semantic search over airline policy documents
  (pricing rules, overbooking policy, load-factor targets, etc.)

When answering:
- Quote exact numbers (load factor %, revenue, seat counts).
- Reference policy thresholds when applicable (e.g. "below 75% triggers review").
- Offer revenue optimisation recommendations backed by data and policy.
- Compare against targets from the policy documents when possible.
"""


def _get_revenue_llm() -> ChatAnthropic:
    """Build the LLM with revenue tools bound."""
    llm = ChatAnthropic(
        model=LLM_MODEL,
        temperature=LLM_TEMPERATURE,
        max_tokens=2048,
        api_key=ANTHROPIC_API_KEY,
    )
    return llm.bind_tools(REVENUE_TOOLS)


def revenue_node(state: AgentState) -> dict:
    """LangGraph node — invoke the Revenue Management agent."""
    logger.info("💰 Revenue agent activated")
    t0 = time.perf_counter()

    llm = _get_revenue_llm()
    messages = [SystemMessage(content=REVENUE_SYSTEM_PROMPT)] + list(state["messages"])

    logger.debug("📤 Sending request to LLM with %d tools bound", len(REVENUE_TOOLS))
    response = llm.invoke(messages)

    if response.tool_calls:
        tool_map = {t.name: t for t in REVENUE_TOOLS}
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
        logger.info("✅ Revenue complete in %.2fs", elapsed)
        return {
            "messages": [response] + tool_messages + [followup],
            "current_agent": "revenue",
        }

    elapsed = time.perf_counter() - t0
    logger.info("✅ Revenue complete in %.2fs (no tool calls)", elapsed)
    return {"messages": [response], "current_agent": "revenue"}
