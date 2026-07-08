"""Query local Chroma investigation corpus vector index."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from src.retrieval.build_vector_index import COLLECTION_NAME, MODEL_NAME
from src.utils.io import PROJECT_ROOT


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def run_query_vector_index(
    query: str,
    *,
    top_k: int = 5,
    project_root: Path = PROJECT_ROOT,
) -> None:
    index_path = _resolve_path("data/retrieval/chroma_index", project_root)
    if not index_path.exists():
        raise FileNotFoundError(f"Chroma index path does not exist: {index_path}")

    model = SentenceTransformer(MODEL_NAME)
    query_embedding = model.encode([query], normalize_embeddings=False).tolist()[0]

    client = chromadb.PersistentClient(path=str(index_path))
    collection = client.get_collection(COLLECTION_NAME)
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    ids = results.get("ids", [[]])[0]
    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    print("FinSignal Lakehouse — Vector Index Query")
    print("-" * 72)
    print(f"query: {query}")
    print(f"top_k: {top_k}")
    print(f"index path: {index_path}")
    print("-" * 72)

    if not ids:
        print("No results.")
        return

    for i, doc_id in enumerate(ids, start=1):
        metadata = metadatas[i - 1] if i - 1 < len(metadatas) else {}
        text = documents[i - 1] if i - 1 < len(documents) else ""
        distance = distances[i - 1] if i - 1 < len(distances) else None

        source_tables = metadata.get("source_tables", "")
        try:
            parsed_sources = json.loads(source_tables) if source_tables else []
        except json.JSONDecodeError:
            parsed_sources = [source_tables] if source_tables else []

        print(f"rank: {i}")
        print(f"distance: {distance}")
        print(f"document_id: {doc_id}")
        print(f"document_type: {metadata.get('document_type', '')}")
        print(f"account_id: {metadata.get('account_id', '')}")
        print(f"security_id: {metadata.get('security_id', '')}")
        print(f"position_date: {metadata.get('position_date', '')}")
        print(f"event_date: {metadata.get('event_date', '')}")
        print(f"title: {metadata.get('title', '')}")
        print(f"source_tables: {parsed_sources}")
        print(f"text: {text}")
        print("-" * 72)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Query Chroma index for investigation corpus.")
    parser.add_argument("--query", required=True, help="Query text to embed and search.")
    parser.add_argument("--top-k", type=int, default=5, help="Number of nearest results to return.")
    args = parser.parse_args(argv)

    run_query_vector_index(query=args.query, top_k=args.top_k)


if __name__ == "__main__":
    main()

