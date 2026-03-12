"""Retrieval chain — wraps ChromaDB retriever as a LangChain tool."""

from __future__ import annotations

import logging

from langchain_core.tools import tool

from src.rag.vectorstore import get_vectorstore

logger = logging.getLogger(__name__)

# Module-level cache so we don't rebuild the store on every call.
_vectorstore_cache = None


def _get_retriever():
    """Return a LangChain retriever backed by the ChromaDB vector store."""
    global _vectorstore_cache
    if _vectorstore_cache is None:
        _vectorstore_cache = get_vectorstore()
    return _vectorstore_cache.as_retriever(search_kwargs={"k": 4})


@tool
def search_aviation_policies(query: str) -> str:
    """Search the aviation operations policy documents using semantic search.

    Use this tool to find information about airline policies including:
    - Revenue management and pricing policies
    - Overbooking rules and compensation
    - Load factor targets
    - Flight delay management procedures
    - Crew rest requirements and flight time limitations
    - Fatigue risk management
    - Gate assignment priorities
    - Safety and compliance rules

    Args:
        query: A natural-language question about aviation policies.
    """
    retriever = _get_retriever()
    docs = retriever.invoke(query)

    if not docs:
        return "No relevant policy documents found for that query."

    sections: list[str] = []
    for i, doc in enumerate(docs, 1):
        source = doc.metadata.get("source", "unknown")
        sections.append(f"--- Policy Excerpt {i} (source: {source}) ---\n{doc.page_content}")

    return "\n\n".join(sections)
