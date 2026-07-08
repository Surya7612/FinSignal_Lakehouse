"""
Build local Chroma vector index from investigation corpus.

No LLM calls, orchestration, or UI are included here.
"""

from __future__ import annotations

import argparse
import json
import shutil
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from src.utils.io import PROJECT_ROOT

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
COLLECTION_NAME = "investigation_corpus"


@dataclass
class VectorIndexResult:
    corpus_records_loaded: int
    embeddings_generated: int
    index_path: Path
    counts_by_document_type: list[tuple[str, int]]


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def _load_corpus(corpus_path: Path) -> list[dict]:
    records: list[dict] = []
    with corpus_path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            records.append(json.loads(line))
    records.sort(key=lambda row: row["document_id"])
    return records


def _to_chroma_metadata(record: dict) -> dict[str, str]:
    source_tables = record.get("source_tables", [])
    if isinstance(source_tables, list):
        source_tables_str = json.dumps(source_tables, separators=(",", ":"))
    else:
        source_tables_str = str(source_tables)

    return {
        "document_type": str(record.get("document_type", "")),
        "account_id": str(record.get("account_id", "")),
        "security_id": str(record.get("security_id", "")),
        "position_date": str(record.get("position_date", "")),
        "event_date": str(record.get("event_date", "")),
        "title": str(record.get("title", "")),
        "source_tables": source_tables_str,
    }


def run_build_vector_index(
    run_id: str,
    *,
    project_root: Path = PROJECT_ROOT,
) -> VectorIndexResult:
    corpus_path = _resolve_path(
        "data/retrieval/investigation_corpus/investigation_corpus.jsonl",
        project_root,
    )
    index_path = _resolve_path("data/retrieval/chroma_index", project_root)

    if not corpus_path.exists():
        raise FileNotFoundError(f"Corpus input not found: {corpus_path}")

    records = _load_corpus(corpus_path)
    texts = [str(row.get("text", "")) for row in records]
    ids = [str(row["document_id"]) for row in records]
    metadatas = [_to_chroma_metadata(row) for row in records]
    doc_types = Counter(str(row.get("document_type", "UNKNOWN")) for row in records)

    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=False,
        normalize_embeddings=False,
    ).tolist()

    if index_path.exists():
        shutil.rmtree(index_path)
    index_path.mkdir(parents=True, exist_ok=True)

    client = chromadb.PersistentClient(path=str(index_path))
    collection = client.create_collection(name=COLLECTION_NAME)
    collection.add(
        ids=ids,
        documents=texts,
        embeddings=embeddings,
        metadatas=metadatas,
    )

    _ = run_id
    return VectorIndexResult(
        corpus_records_loaded=len(records),
        embeddings_generated=len(embeddings),
        index_path=index_path,
        counts_by_document_type=sorted(doc_types.items(), key=lambda x: x[0]),
    )


def _print_summary(result: VectorIndexResult, run_id: str) -> None:
    print("FinSignal Lakehouse — Vector Index Build Complete")
    print(f"vector_run_id: {run_id}")
    print("-" * 72)
    print(f"corpus records loaded: {result.corpus_records_loaded}")
    print(f"embeddings generated:  {result.embeddings_generated}")
    print(f"vector index path:     {result.index_path}")
    print("counts by document_type:")
    if not result.counts_by_document_type:
        print("  none")
    for doc_type, count in result.counts_by_document_type:
        print(f"  {doc_type}: {count}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build Chroma vector index from investigation corpus JSONL.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Vector index build run identifier (default: generated UUID).",
    )
    args = parser.parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    result = run_build_vector_index(run_id=run_id)
    _print_summary(result, run_id)


if __name__ == "__main__":
    main()

