"""Cross-engine consistency between Spark gold and Python/C++ reference engines."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.algorithms.engine_loader import (
    load_engine_inputs,
    normalize_engine_rows,
    run_cpp_engine,
    run_python_engine,
)
from src.utils.io import PROJECT_ROOT, create_spark_session, read_table


COMPARE_FIELDS = (
    "expected_position",
    "reported_position",
    "reconciliation_status",
    "break_reason_code",
)


def _spark_rows(spark: SparkSession, project_root: Path) -> list[dict[str, Any]]:
    df = read_table(spark, project_root / "data/gold/position_reconstruction")
    rows = df.select(
        "account_id",
        "security_id",
        "position_date",
        "expected_position",
        "reported_position",
        "reconciliation_status",
        "break_reason_code",
    ).collect()

    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "account_id": row.account_id,
                "security_id": row.security_id,
                "position_date": str(row.position_date)[:10],
                "expected_position": float(row.expected_position),
                "reported_position": None if row.reported_position is None else float(row.reported_position),
                "reconciliation_status": row.reconciliation_status,
                "break_reason_code": row.break_reason_code,
            }
        )
    return output


def _index_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    indexed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["account_id"], row["security_id"], row["position_date"])
        indexed[key] = row
    return indexed


def compare_cross_engine(
    *,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Cross Engine Consistency")

    try:
        spark_rows = _spark_rows(session, project_root)
        spark_index = _index_rows(spark_rows)

        inputs = load_engine_inputs(project_root=project_root, spark=session)
        python_index = _index_rows(run_python_engine(inputs))

        try:
            cpp_index = _index_rows(run_cpp_engine(inputs))
            cpp_available = True
        except ImportError:
            cpp_index = {}
            cpp_available = False

        spark_python_mismatches: list[dict[str, Any]] = []
        spark_cpp_mismatches: list[dict[str, Any]] = []

        for key, spark_row in spark_index.items():
            python_row = python_index.get(key)
            if python_row is None:
                spark_python_mismatches.append({"key": key, "reason": "missing_in_python"})
                continue
            for field in COMPARE_FIELDS:
                spark_value = spark_row[field]
                python_value = python_row[field]
                if field in {"expected_position", "reported_position"}:
                    if spark_value is None and python_value is None:
                        continue
                    if spark_value is None or python_value is None:
                        spark_python_mismatches.append(
                            {"key": key, "field": field, "spark": spark_value, "python": python_value}
                        )
                        break
                    if abs(float(spark_value) - float(python_value)) > 1e-6:
                        spark_python_mismatches.append(
                            {"key": key, "field": field, "spark": spark_value, "python": python_value}
                        )
                        break
                elif spark_value != python_value:
                    spark_python_mismatches.append(
                        {"key": key, "field": field, "spark": spark_value, "python": python_value}
                    )
                    break

            if cpp_available:
                cpp_row = cpp_index.get(key)
                if cpp_row is None:
                    spark_cpp_mismatches.append({"key": key, "reason": "missing_in_cpp"})
                    continue
                for field in COMPARE_FIELDS:
                    spark_value = spark_row[field]
                    cpp_value = cpp_row[field]
                    if field in {"expected_position", "reported_position"}:
                        if spark_value is None and cpp_value is None:
                            continue
                        if spark_value is None or cpp_value is None:
                            spark_cpp_mismatches.append(
                                {"key": key, "field": field, "spark": spark_value, "cpp": cpp_value}
                            )
                            break
                        if abs(float(spark_value) - float(cpp_value)) > 1e-6:
                            spark_cpp_mismatches.append(
                                {"key": key, "field": field, "spark": spark_value, "cpp": cpp_value}
                            )
                            break
                    elif spark_value != cpp_value:
                        spark_cpp_mismatches.append(
                            {"key": key, "field": field, "spark": spark_value, "cpp": cpp_value}
                        )
                        break

        python_cpp_match = normalize_engine_rows(list(python_index.values())) == normalize_engine_rows(
            list(cpp_index.values())
        )

        return {
            "spark_row_count": len(spark_index),
            "python_row_count": len(python_index),
            "cpp_row_count": len(cpp_index),
            "cpp_available": cpp_available,
            "spark_python_consistent": len(spark_python_mismatches) == 0,
            "spark_cpp_consistent": len(spark_cpp_mismatches) == 0 if cpp_available else None,
            "python_cpp_consistent": python_cpp_match if cpp_available else None,
            "spark_python_mismatch_count": len(spark_python_mismatches),
            "spark_cpp_mismatch_count": len(spark_cpp_mismatches),
            "sample_spark_python_mismatches": spark_python_mismatches[:5],
            "sample_spark_cpp_mismatches": spark_cpp_mismatches[:5],
        }
    finally:
        if owns_spark:
            session.stop()
