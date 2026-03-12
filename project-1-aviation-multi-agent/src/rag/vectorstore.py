"""ChromaDB vector store — document ingestion and collection management."""

from __future__ import annotations

import logging
from pathlib import Path

from langchain_chroma import Chroma
from langchain_community.document_loaders import TextLoader
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.config import (
    CHROMA_COLLECTION_NAME,
    CHROMA_PERSIST_DIR,
    DOCS_DIR,
    EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)


def _get_embeddings() -> HuggingFaceEmbeddings:
    """Return a local HuggingFace embeddings model (no API key needed)."""
    return HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)


def get_vectorstore() -> Chroma:
    """Return (or create) the persistent ChromaDB vector store.

    If the collection already contains documents, it is returned as-is.
    Otherwise the documents in ``DOCS_DIR`` are ingested first.
    """
    embeddings = _get_embeddings()
    vectorstore = Chroma(
        collection_name=CHROMA_COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=str(CHROMA_PERSIST_DIR),
    )

    # Check whether documents have already been ingested.
    existing = vectorstore.get()
    if existing and existing.get("ids"):
        logger.info(
            "ChromaDB collection '%s' already has %d documents — skipping ingestion.",
            CHROMA_COLLECTION_NAME,
            len(existing["ids"]),
        )
        return vectorstore

    # Ingest documents from the docs directory.
    logger.info("Ingesting documents from %s into ChromaDB …", DOCS_DIR)
    vectorstore = ingest_documents(embeddings)
    return vectorstore


def ingest_documents(
    embeddings: HuggingFaceEmbeddings | None = None,
    docs_dir: Path | None = None,
) -> Chroma:
    """Load markdown files from *docs_dir*, split them, and add to ChromaDB.

    Returns the populated :class:`Chroma` vector store.
    """
    if embeddings is None:
        embeddings = _get_embeddings()
    if docs_dir is None:
        docs_dir = DOCS_DIR

    all_docs = []
    for md_file in sorted(docs_dir.glob("*.md")):
        logger.info("Loading %s", md_file.name)
        loader = TextLoader(str(md_file), encoding="utf-8")
        all_docs.extend(loader.load())

    if not all_docs:
        logger.warning("No markdown documents found in %s", docs_dir)
        return Chroma(
            collection_name=CHROMA_COLLECTION_NAME,
            embedding_function=embeddings,
            persist_directory=str(CHROMA_PERSIST_DIR),
        )

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=800,
        chunk_overlap=100,
        separators=["\n## ", "\n### ", "\n\n", "\n", " "],
    )
    chunks = splitter.split_documents(all_docs)
    logger.info("Split into %d chunks — inserting into ChromaDB.", len(chunks))

    vectorstore = Chroma.from_documents(
        documents=chunks,
        embedding=embeddings,
        collection_name=CHROMA_COLLECTION_NAME,
        persist_directory=str(CHROMA_PERSIST_DIR),
    )
    logger.info("Ingestion complete. Collection '%s' ready.", CHROMA_COLLECTION_NAME)
    return vectorstore
