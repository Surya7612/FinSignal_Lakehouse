"""Deterministic LangGraph investigation workflow (no LLM calls)."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.retrieval.build_vector_index import COLLECTION_NAME, MODEL_NAME
from src.utils.io import PROJECT_ROOT, create_spark_session

try:
    from langchain_chroma import Chroma
except ImportError:  # pragma: no cover
    from langchain_community.vectorstores import Chroma  # type: ignore

try:
    from langchain_huggingface import HuggingFaceEmbeddings
except ImportError:  # pragma: no cover
    from langchain_community.embeddings import HuggingFaceEmbeddings  # type: ignore


class InvestigationState(TypedDict, total=False):
    query: str
    account_id: str | None
    security_id: str | None
    target_date: str | None
    structured_break: dict[str, Any] | None
    quality_flags: list[dict[str, Any]]
    event_context: list[dict[str, Any]]
    retrieved_evidence: list[dict[str, Any]]
    investigation_packet: dict[str, Any]


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def _row_to_dict(row: Any) -> dict[str, Any]:
    data = row.asDict(recursive=True)
    return {k: (str(v) if v is not None else None) for k, v in data.items()}


def _load_silver_events(session: SparkSession, project_root: Path) -> DataFrame:
    events_clean = _resolve_path("data/silver/events_clean", project_root)
    filing_events_clean = _resolve_path("data/silver/filing_events_clean", project_root)
    if events_clean.exists():
        return session.read.parquet(str(events_clean))
    if filing_events_clean.exists():
        return session.read.parquet(str(filing_events_clean))
    raise FileNotFoundError("Neither data/silver/events_clean nor data/silver/filing_events_clean exists.")


def run_investigation_graph(
    query: str,
    *,
    top_k: int = 5,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Investigation Graph")

    try:
        reconciliation_breaks = session.read.parquet(
            str(_resolve_path("data/gold/reconciliation_breaks", project_root))
        )
        trade_quality_flags = session.read.parquet(
            str(_resolve_path("data/silver/trade_quality_flags", project_root))
        )
        event_window_metrics = session.read.parquet(
            str(_resolve_path("data/gold/event_window_metrics", project_root))
        )
        events_clean = _load_silver_events(session, project_root).select(
            "event_id",
            "security_id",
            "event_date",
            "event_type",
            "event_summary",
        )
        _ = session.read.parquet(  # explicitly loaded per requirement input list
            str(_resolve_path("data/gold/position_reconstruction", project_root))
        )

        # ------------------------------------------------------------------
        # LangGraph nodes
        # ------------------------------------------------------------------
        def parse_query(state: InvestigationState) -> InvestigationState:
            q = state["query"]
            account_match = re.search(r"\b(ACC\d{3})\b", q, re.IGNORECASE)
            security_match = re.search(r"\b(SEC\d{3})\b", q, re.IGNORECASE)
            date_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", q)
            return {
                "account_id": account_match.group(1).upper() if account_match else None,
                "security_id": security_match.group(1).upper() if security_match else None,
                "target_date": date_match.group(1) if date_match else None,
            }

        def query_reconciliation_break(state: InvestigationState) -> InvestigationState:
            account_id = state.get("account_id")
            security_id = state.get("security_id")
            target_date = state.get("target_date")
            if not (account_id and security_id and target_date):
                return {"structured_break": None}

            row = (
                reconciliation_breaks.filter(
                    (F.col("account_id") == F.lit(account_id))
                    & (F.col("security_id") == F.lit(security_id))
                    & (F.col("position_date") == F.to_date(F.lit(target_date)))
                )
                .select(
                    "account_id",
                    "security_id",
                    "position_date",
                    "expected_position",
                    "reported_position",
                    "position_difference",
                    "break_reason_code",
                    "root_cause_reason_code",
                    "severity",
                )
                .limit(1)
                .collect()
            )
            return {"structured_break": _row_to_dict(row[0]) if row else None}

        def query_trade_quality_flags(state: InvestigationState) -> InvestigationState:
            flags_df = trade_quality_flags
            if state.get("account_id"):
                flags_df = flags_df.filter(F.col("account_id") == F.lit(state["account_id"]))
            if state.get("security_id"):
                flags_df = flags_df.filter(F.col("security_id") == F.lit(state["security_id"]))
            if state.get("target_date"):
                flags_df = flags_df.filter(F.col("flag_date") == F.to_date(F.lit(state["target_date"])))

            rows = (
                flags_df.select(
                    "flag_type",
                    "account_id",
                    "security_id",
                    "flag_date",
                    "severity",
                    "description",
                    "affected_entity",
                    "affected_record_id",
                )
                .orderBy("flag_type")
                .limit(20)
                .collect()
            )
            return {"quality_flags": [_row_to_dict(row) for row in rows]}

        def query_event_context(state: InvestigationState) -> InvestigationState:
            security_id = state.get("security_id")
            target_date = state.get("target_date")
            if not security_id:
                return {"event_context": []}

            event_df = (
                event_window_metrics.alias("m")
                .join(events_clean.alias("e"), on=["event_id", "security_id", "event_date"], how="left")
                .filter(F.col("m.security_id") == F.lit(security_id))
            )
            if target_date:
                event_df = event_df.filter(
                    F.abs(F.datediff(F.col("m.event_date"), F.to_date(F.lit(target_date)))) <= F.lit(5)
                )

            rows = (
                event_df.select(
                    F.col("m.event_id").alias("event_id"),
                    F.col("m.security_id").alias("security_id"),
                    F.col("m.event_date").alias("event_date"),
                    F.coalesce(F.col("e.event_type"), F.col("m.event_type")).alias("event_type"),
                    F.col("e.event_summary").alias("event_summary"),
                    "pre_event_return",
                    "post_event_return",
                    "event_day_return",
                    "position_change_around_event",
                    "exposure_change_around_event",
                    "is_full_window",
                )
                .orderBy("event_date")
                .limit(10)
                .collect()
            )
            return {"event_context": [_row_to_dict(row) for row in rows]}

        def retrieve_evidence(state: InvestigationState) -> InvestigationState:
            index_path = _resolve_path("data/retrieval/chroma_index", project_root)
            if not index_path.exists():
                return {"retrieved_evidence": []}

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
            docs = retriever.invoke(state["query"])

            evidence = []
            for doc in docs:
                md = doc.metadata or {}
                source_tables_raw = md.get("source_tables", "")
                try:
                    source_tables = json.loads(source_tables_raw) if source_tables_raw else []
                except json.JSONDecodeError:
                    source_tables = [source_tables_raw] if source_tables_raw else []
                evidence.append(
                    {
                        "document_type": md.get("document_type", ""),
                        "account_id": md.get("account_id", ""),
                        "security_id": md.get("security_id", ""),
                        "position_date": md.get("position_date", ""),
                        "event_date": md.get("event_date", ""),
                        "title": md.get("title", ""),
                        "text": doc.page_content,
                        "metadata": md,
                        "source_tables": source_tables,
                    }
                )
            return {"retrieved_evidence": evidence}

        def build_investigation_packet(state: InvestigationState) -> InvestigationState:
            structured_break = state.get("structured_break")
            quality_flags = state.get("quality_flags", [])
            event_context = state.get("event_context", [])
            retrieved = state.get("retrieved_evidence", [])

            missing_data_notes: list[str] = []
            if not state.get("account_id"):
                missing_data_notes.append("account_id not found in query; structured filters are partial.")
            if not state.get("security_id"):
                missing_data_notes.append("security_id not found in query; structured filters are partial.")
            if not state.get("target_date"):
                missing_data_notes.append("date not found in query; structured filters are partial.")
            if not structured_break:
                missing_data_notes.append("structured break not found for parsed account/security/date.")
            if not quality_flags:
                missing_data_notes.append("no matching trade quality flags found.")
            if not event_context:
                missing_data_notes.append("no nearby event-window context found for parsed security/date.")
            if not retrieved:
                missing_data_notes.append("retrieval returned no evidence documents.")

            if structured_break:
                summary = (
                    "Structured break found and used as primary evidence. "
                    f"For {structured_break.get('account_id')} / {structured_break.get('security_id')} on "
                    f"{structured_break.get('position_date')}, expected position was "
                    f"{structured_break.get('expected_position')}, reported position was "
                    f"{structured_break.get('reported_position')}, and difference was "
                    f"{structured_break.get('position_difference')}. "
                    f"Break reason is {structured_break.get('break_reason_code')} with root cause "
                    f"{structured_break.get('root_cause_reason_code')} and severity "
                    f"{structured_break.get('severity')}."
                )
            else:
                summary = (
                    "Structured break not found for parsed fields; investigation relies on retrieved evidence "
                    "and any available quality/event context."
                )

            confidence_notes = [
                "Deterministic workflow: regex parsing + direct parquet lookups + vector retrieval.",
                "No LLM generation used; output is template-based.",
            ]
            if structured_break:
                confidence_notes.append("Confidence higher because exact structured break row was found.")
            else:
                confidence_notes.append("Confidence lower because no exact structured break row was found.")

            packet = {
                "summary": summary,
                "structured_break": structured_break,
                "quality_flags": quality_flags,
                "event_context": event_context,
                "retrieved_evidence_titles": [row.get("title", "") for row in retrieved],
                "confidence_notes": confidence_notes,
                "missing_data_notes": missing_data_notes,
            }
            return {"investigation_packet": packet}

        graph = StateGraph(InvestigationState)
        graph.add_node("parse_query", parse_query)
        graph.add_node("query_reconciliation_break", query_reconciliation_break)
        graph.add_node("query_trade_quality_flags", query_trade_quality_flags)
        graph.add_node("query_event_context", query_event_context)
        graph.add_node("retrieve_evidence", retrieve_evidence)
        graph.add_node("build_investigation_packet", build_investigation_packet)

        graph.add_edge(START, "parse_query")
        graph.add_edge("parse_query", "query_reconciliation_break")
        graph.add_edge("query_reconciliation_break", "query_trade_quality_flags")
        graph.add_edge("query_trade_quality_flags", "query_event_context")
        graph.add_edge("query_event_context", "retrieve_evidence")
        graph.add_edge("retrieve_evidence", "build_investigation_packet")
        graph.add_edge("build_investigation_packet", END)

        compiled = graph.compile()
        final_state = compiled.invoke({"query": query})
        return final_state["investigation_packet"]
    finally:
        if owns_spark:
            session.stop()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run deterministic LangGraph investigation workflow.",
    )
    parser.add_argument("--query", required=True, help="Investigation query text.")
    parser.add_argument("--top-k", type=int, default=5, help="Top-k retrieved evidence docs.")
    args = parser.parse_args(argv)

    packet = run_investigation_graph(query=args.query, top_k=args.top_k)
    print(json.dumps(packet, indent=2))


if __name__ == "__main__":
    main()

