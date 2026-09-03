"""Manifest validation tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.validation.manifest_validation import validate_manifest
from tests.golden import MANIFEST_DETECTION_RATE, MANIFEST_ISSUE_COUNT


@pytest.fixture(scope="module")
def manifest_report(spark, project_root: Path):
    return validate_manifest(project_root=project_root, spark=spark)


def test_manifest_issue_count(manifest_report) -> None:
    assert manifest_report.total_issues == MANIFEST_ISSUE_COUNT


def test_manifest_detection_rate(manifest_report) -> None:
    assert manifest_report.detection_rate == MANIFEST_DETECTION_RATE
    assert manifest_report.missed_count == 0


def test_split_adjustment_issue_detected(manifest_report) -> None:
    split_results = [
        result
        for result in manifest_report.results
        if result.issue_type == "SPLIT_ADJUSTMENT_BREAK"
    ]
    assert len(split_results) == 1
    assert split_results[0].detected is True


def test_duplicate_trade_issues_detected(manifest_report) -> None:
    duplicate_results = [
        result for result in manifest_report.results if result.issue_type == "DUPLICATE_TRADE"
    ]
    assert duplicate_results
    assert all(result.detected for result in duplicate_results)
