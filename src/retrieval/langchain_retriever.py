"""LangChain wrapper for local investigation Chroma retrieval only."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.retrieval.build_vector_index import COLLECTION_NAME, MODEL_NAME
from src.utils.io import PROJECT_ROOT

try:
    from langchain_chroma import Chroma
except ImportError:  # pragma: no cover
    from langchain_community.vectorstores import Chroma  # type: ignore

try:
    from langchain_huggingface import HuggingFaceEmbeddings
except ImportError:  # pragma: no cover
    from langchain_community.embeddings import HuggingFaceEmbeddings  # type: ignore


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def run_langchain_retriever(
    query: str,
    *,
    top_k: int = 5,
    project_root: Path = PROJECT_ROOT,
) -> None:
    index_path = _resolve_path("data/retrieval/chroma_index", project_root)
    if not index_path.exists():
        raise FileNotFoundError(f"Chroma index path does not exist: {index_path}")

    embeddings = HuggingFaceEmbeddings(
        model_name=MODEL_NAME,
        encode_kwargs={"normalize_embeddings": False},
    )

    vectorstore = Chroma(
        collection_name=COLLECTION_NAME,
        persist_directory=str(index_path),
        embedding_function=embeddings,
    )
    retriever = vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": top_k},
    )
    docs = retriever.invoke(query)

    print("FinSignal Lakehouse — LangChain Retriever")
    print("-" * 72)
    print(f"query: {query}")
    print(f"top_k: {top_k}")
    print(f"index path: {index_path}")
    print("-" * 72)

    if not docs:
        print("No results.")
        return

    for rank, doc in enumerate(docs, start=1):
        md = doc.metadata or {}
        source_tables_raw = md.get("source_tables", "")
        try:
            source_tables = json.loads(source_tables_raw) if source_tables_raw else []
        except json.JSONDecodeError:
            source_tables = [source_tables_raw] if source_tables_raw else []

        print(f"rank: {rank}")
        print(f"document_type: {md.get('document_type', '')}")
        print(f"account_id: {md.get('account_id', '')}")
        print(f"security_id: {md.get('security_id', '')}")
        print(f"position_date: {md.get('position_date', '')}")
        print(f"event_date: {md.get('event_date', '')}")
        print(f"title: {md.get('title', '')}")
        print(f"text: {doc.page_content}")
        print(f"metadata: {md}")
        print(f"source_tables: {source_tables}")
        print("-" * 72)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Query Chroma index through LangChain retriever.")
    parser.add_argument("--query", required=True, help="Query text to retrieve evidence documents.")
    parser.add_argument("--top-k", type=int, default=5, help="Top-k nearest documents.")
    args = parser.parse_args(argv)
    run_langchain_retriever(query=args.query, top_k=args.top_k)


if __name__ == "__main__":
    main()

