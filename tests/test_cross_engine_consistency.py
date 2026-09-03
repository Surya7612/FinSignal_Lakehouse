"""Cross-engine consistency tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.validation.cross_engine_consistency import compare_cross_engine


def test_spark_python_cpp_consistency(spark, project_root: Path) -> None:
    report = compare_cross_engine(project_root=project_root, spark=spark)

    assert report["spark_row_count"] > 0
    assert report["python_row_count"] > 0
    assert report["cpp_available"] is True
    assert report["python_cpp_consistent"] is True
    assert report["spark_python_consistent"] is True
    assert report["spark_cpp_consistent"] is True
    assert report["spark_python_mismatch_count"] == 0
    assert report["spark_cpp_mismatch_count"] == 0
