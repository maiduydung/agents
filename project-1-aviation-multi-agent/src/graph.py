"""LangGraph workflow definition — multi-agent routing graph."""

from __future__ import annotations

from langgraph.graph import END, StateGraph

from src.agents.crew import crew_node
from src.agents.flight_ops import flight_ops_node
from src.agents.revenue import revenue_node
from src.agents.supervisor import supervisor_node
from src.models import AgentState


def _route_to_specialist(state: AgentState) -> str:
    """Conditional edge: read ``current_agent`` set by the supervisor and
    route to the matching specialist node."""
    return state.get("current_agent", "flight_ops")


def build_graph() -> StateGraph:
    """Construct and compile the multi-agent LangGraph workflow.

    The graph has the following topology::

        START -> supervisor -> (conditional) -> flight_ops | revenue | crew -> END

    The supervisor classifies the user query and writes the target agent name
    into ``state["current_agent"]``.  A conditional edge then forwards
    execution to the chosen specialist, which invokes its tools and produces
    the final answer.

    Returns:
        A compiled :class:`StateGraph` ready to be invoked.
    """
    workflow = StateGraph(AgentState)

    # --- Nodes ---
    workflow.add_node("supervisor", supervisor_node)
    workflow.add_node("flight_ops", flight_ops_node)
    workflow.add_node("revenue", revenue_node)
    workflow.add_node("crew", crew_node)

    # --- Edges ---
    # Entry point.
    workflow.set_entry_point("supervisor")

    # Supervisor routes to one of the specialists.
    workflow.add_conditional_edges(
        "supervisor",
        _route_to_specialist,
        {
            "flight_ops": "flight_ops",
            "revenue": "revenue",
            "crew": "crew",
        },
    )

    # Each specialist terminates the graph after producing its answer.
    workflow.add_edge("flight_ops", END)
    workflow.add_edge("revenue", END)
    workflow.add_edge("crew", END)

    return workflow.compile()


# Pre-built compiled graph for convenience.
graph = build_graph()
