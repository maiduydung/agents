#!/usr/bin/env python3
"""CLI entry point for the Aviation Multi-Agent Operations Assistant."""

from __future__ import annotations

import logging
import sys
import time

from langchain_core.messages import HumanMessage

from src.graph import graph

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s │ %(levelname)-7s │ %(name)-30s │ %(message)s",
    datefmt="%H:%M:%S",
)
# Silence noisy third-party loggers
for noisy in (
    "httpx", "httpcore", "urllib3", "openai", "anthropic",
    "chromadb", "sentence_transformers", "langchain", "langchain_core",
    "langchain_anthropic", "langchain_community", "langchain_text_splitters",
    "huggingface_hub", "transformers",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger("aviation.cli")


def run_query(query: str, history: list | None = None) -> tuple[str, list]:
    """Send a query through the multi-agent graph, preserving conversation history.

    Returns (answer_text, updated_history).
    """
    logger.info("📩 Received query: %s", query)
    t0 = time.perf_counter()

    messages = list(history) if history else []
    messages.append(HumanMessage(content=query))

    initial_state = {
        "messages": messages,
        "current_agent": "",
        "metadata": {},
    }

    logger.debug("🚀 Invoking LangGraph multi-agent workflow … (history: %d msgs)", len(messages) - 1)
    result = graph.invoke(initial_state)
    elapsed = time.perf_counter() - t0

    agent_used = result.get("current_agent", "unknown")
    all_messages = result.get("messages", [])
    logger.info("✅ Pipeline complete in %.2fs │ agent=%s │ messages=%d", elapsed, agent_used, len(all_messages))

    answer = all_messages[-1].content if all_messages else "No response generated."
    return answer, all_messages


def interactive_loop() -> None:
    """Run an interactive REPL with conversation history."""
    print("=" * 60)
    print("  ✈️  Aviation Multi-Agent Operations Assistant")
    print("  Type 'quit' or 'exit' to stop.")
    print("  Type 'clear' to reset conversation history.")
    print("=" * 60)

    history: list = []

    while True:
        try:
            query = input("\n[You] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 Goodbye.")
            break

        if not query:
            continue
        if query.lower() in {"quit", "exit", "q"}:
            print("👋 Goodbye.")
            break
        if query.lower() == "clear":
            history = []
            logger.info("🗑️  Conversation history cleared")
            print("History cleared.")
            continue

        print()
        try:
            answer, history = run_query(query, history)
            print(f"\n{'─' * 60}")
            print(f"[Assistant]\n{answer}")
            print(f"{'─' * 60}")
            logger.debug("💬 History now has %d messages", len(history))
        except Exception as exc:
            logger.exception("💥 Error processing query")
            print(f"\n[Error] {exc}")


def main() -> None:
    """Entry point — accepts an optional query as a CLI argument."""
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        answer, _ = run_query(query)
        print(f"\n{'─' * 60}")
        print(answer)
        print(f"{'─' * 60}")
    else:
        interactive_loop()


if __name__ == "__main__":
    main()
