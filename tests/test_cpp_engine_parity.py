"""Parity tests for the C++ reference reconciliation engine."""

from __future__ import annotations

import pytest

from src.algorithms.engine_loader import (
    load_engine_inputs,
    normalize_engine_rows,
    run_cpp_engine,
    run_python_engine,
)


finsignal_engine = pytest.importorskip("finsignal_engine")


def test_cpp_engine_matches_python_reference(java_home_configured) -> None:
    inputs = load_engine_inputs()
    python_rows = run_python_engine(inputs)
    cpp_rows = run_cpp_engine(inputs)

    assert len(python_rows) == len(cpp_rows)
    assert normalize_engine_rows(python_rows) == normalize_engine_rows(cpp_rows)


def test_cpp_split_break_classification(java_home_configured) -> None:
    inputs = load_engine_inputs()
    cpp_rows = run_cpp_engine(inputs)
    split_rows = [
        row
        for row in cpp_rows
        if row["account_id"] == "ACC001"
        and row["security_id"] == "SEC002"
        and row["position_date"] == "2025-01-20"
    ]
    assert len(split_rows) == 1
    assert split_rows[0]["break_reason_code"] == "SPLIT_ADJUSTMENT_BREAK"
    assert split_rows[0]["reconciliation_status"] == "BREAK"
    assert round(float(split_rows[0]["expected_position"]), 6) == 382.0
    assert round(float(split_rows[0]["reported_position"]), 6) == 191.0
