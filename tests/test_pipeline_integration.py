"""Integration tests for gold-layer outputs and investigation workflow."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.retrieval.evaluate_investigation import evaluate_investigation
from src.retrieval.investigation_graph import run_investigation_graph
from src.utils.io import read_table
from tests.golden import (
    GOLD_ROW_COUNTS,
    INVESTIGATION_QUERY,
    SPLIT_BREAK,
)


def _table_count(spark, project_root: Path, relative_path: str) -> int:
    return read_table(spark, project_root / relative_path).count()


def test_gold_row_counts(spark, project_root: Path) -> None:
    assert (
        _table_count(spark, project_root, "data/gold/position_reconstruction")
        == GOLD_ROW_COUNTS["position_reconstruction"]
    )
    assert (
        _table_count(spark, project_root, "data/gold/reconciliation_breaks")
        == GOLD_ROW_COUNTS["reconciliation_breaks"]
    )
    assert (
        _table_count(spark, project_root, "data/silver/trade_quality_flags")
        == GOLD_ROW_COUNTS["trade_quality_flags"]
    )
    assert (
        _table_count(spark, project_root, "data/gold/event_window_metrics")
        == GOLD_ROW_COUNTS["event_window_metrics"]
    )
    assert (
        _table_count(spark, project_root, "data/silver/corporate_actions_clean")
        == GOLD_ROW_COUNTS["corporate_actions_clean"]
    )


def test_split_adjustment_break(spark, project_root: Path) -> None:
    breaks_df = read_table(spark, project_root / "data/gold/reconciliation_breaks")
    row = (
        breaks_df.filter(
            (F.col("account_id") == F.lit(SPLIT_BREAK["account_id"]))
            & (F.col("security_id") == F.lit(SPLIT_BREAK["security_id"]))
            & (F.col("position_date") == F.to_date(F.lit(SPLIT_BREAK["position_date"])))
        )
        .collect()
    )
    assert len(row) == 1
    break_row = row[0]
    assert break_row.break_reason_code == SPLIT_BREAK["break_reason_code"]
    assert break_row.root_cause_reason_code == SPLIT_BREAK["break_reason_code"]
    assert float(break_row.expected_position) == SPLIT_BREAK["expected_position"]
    assert float(break_row.reported_position) == SPLIT_BREAK["reported_position"]
    assert float(break_row.position_difference) == SPLIT_BREAK["position_difference"]


def test_investigation_corpus_count(project_root: Path) -> None:
    corpus_path = project_root / "data/retrieval/investigation_corpus/investigation_corpus.jsonl"
    assert corpus_path.exists(), "Run scripts/run_all.sh before integration tests."
    lines = [line for line in corpus_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == GOLD_ROW_COUNTS["investigation_corpus"]


def test_investigation_packet_and_evaluation(spark, project_root: Path) -> None:
    packet = run_investigation_graph(
        INVESTIGATION_QUERY,
        project_root=project_root,
        spark=spark,
    )
    structured_break = packet["structured_break"]
    assert structured_break is not None
    assert structured_break["break_reason_code"] == SPLIT_BREAK["break_reason_code"]
    assert structured_break["account_id"] == SPLIT_BREAK["account_id"]
    assert structured_break["security_id"] == SPLIT_BREAK["security_id"]

    retrieved = packet["retrieved_evidence"]
    assert retrieved
    assert retrieved[0]["document_type"] == "RECONCILIATION_BREAK_SUMMARY"
    assert retrieved[0]["account_id"] == SPLIT_BREAK["account_id"]

    evaluation = evaluate_investigation(INVESTIGATION_QUERY, top_k=5)
    assert evaluation["checks"]["structured_break_found"]["passed"] is True
    assert evaluation["checks"]["exact_evidence_found"]["passed"] is True
    assert evaluation["checks"]["evidence_type_coverage"]["passed"] is True
    assert evaluation["unsupported_claim_risk"] == "LOW"
    assert evaluation["groundedness_score"] >= 0.8
