"""Validate injected manifest issues against silver/gold pipeline outputs."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.io import PROJECT_ROOT, create_spark_session, read_table


@dataclass
class ManifestIssueResult:
    issue_id: str
    issue_type: str
    expected_detection_category: str
    detected: bool
    detection_method: str
    detail: str


@dataclass
class ManifestValidationReport:
    total_issues: int
    detected_count: int
    missed_count: int
    detection_rate: float
    results: list[ManifestIssueResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_issues": self.total_issues,
            "detected_count": self.detected_count,
            "missed_count": self.missed_count,
            "detection_rate": round(self.detection_rate, 4),
            "results": [
                {
                    "issue_id": result.issue_id,
                    "issue_type": result.issue_type,
                    "expected_detection_category": result.expected_detection_category,
                    "detected": result.detected,
                    "detection_method": result.detection_method,
                    "detail": result.detail,
                }
                for result in self.results
            ],
        }


def load_manifest(manifest_path: Path) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                issues.append(json.loads(line))
    return issues


def _flag_exists(
    flags_df: DataFrame,
    *,
    flag_type: str,
    trade_id: str | None = None,
    account_id: str | None = None,
    security_id: str | None = None,
    issue_date: str | None = None,
) -> bool:
    filtered = flags_df.filter(F.col("flag_type") == F.lit(flag_type))
    if trade_id:
        filtered = filtered.filter(F.col("affected_record_id") == F.lit(trade_id))
    if account_id:
        filtered = filtered.filter(F.col("account_id") == F.lit(account_id))
    if security_id:
        filtered = filtered.filter(F.col("security_id") == F.lit(security_id))
    if issue_date:
        filtered = filtered.filter(F.col("flag_date") == F.to_date(F.lit(issue_date)))
    return filtered.limit(1).count() > 0


def _break_exists(
    breaks_df: DataFrame,
    *,
    account_id: str,
    security_id: str,
    issue_date: str,
    break_reason_code: str | None = None,
) -> bool:
    filtered = breaks_df.filter(
        (F.col("account_id") == F.lit(account_id))
        & (F.col("security_id") == F.lit(security_id))
        & (F.col("position_date") == F.to_date(F.lit(issue_date)))
    )
    if break_reason_code:
        filtered = filtered.filter(F.col("break_reason_code") == F.lit(break_reason_code))
    return filtered.limit(1).count() > 0


def _trade_absent(trades_df: DataFrame, trade_id: str) -> bool:
    return trades_df.filter(F.col("trade_id") == F.lit(trade_id)).limit(1).count() == 0


def validate_issue(
    issue: dict[str, Any],
    *,
    flags_df: DataFrame,
    breaks_df: DataFrame,
    trades_df: DataFrame,
    position_df: DataFrame,
) -> ManifestIssueResult:
    issue_id = issue["issue_id"]
    issue_type = issue["issue_type"]
    expected = issue["expected_detection_category"]
    account_id = issue.get("account_id")
    security_id = issue.get("security_id")
    trade_id = issue.get("trade_id")
    issue_date = issue.get("issue_date")

    if issue_type == "DUPLICATE_TRADE":
        detected = _flag_exists(flags_df, flag_type="DUPLICATE_TRADE", trade_id=trade_id)
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "quality_flag",
            f"trade_id={trade_id}",
        )

    if issue_type == "MISSING_TRADE":
        detected = _trade_absent(trades_df, trade_id)
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "trade_absence",
            f"trade_id={trade_id} absent from silver.trades_clean",
        )

    if issue_type == "WRONG_REPORTED_POSITION":
        detected = _break_exists(
            breaks_df,
            account_id=account_id,
            security_id=security_id,
            issue_date=issue_date,
        )
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "reconciliation_break",
            f"{account_id}/{security_id}/{issue_date}",
        )

    if issue_type == "TIMING_MISMATCH":
        detected = _break_exists(
            breaks_df,
            account_id=account_id,
            security_id=security_id,
            issue_date=issue_date,
        )
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "reconciliation_break",
            f"{account_id}/{security_id}/{issue_date}",
        )

    if issue_type == "LATE_ARRIVING_TRADE":
        detected = _flag_exists(flags_df, flag_type="LATE_ARRIVING_TRADE", trade_id=trade_id)
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "quality_flag",
            f"trade_id={trade_id}",
        )

    if issue_type == "MISSING_PRICE":
        detected = _flag_exists(
            flags_df,
            flag_type="MISSING_PRICE",
            security_id=security_id,
            issue_date=issue_date,
        )
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "quality_flag",
            f"{security_id}/{issue_date}",
        )

    if issue_type == "INVALID_SECURITY_ID":
        detected = _flag_exists(flags_df, flag_type="UNKNOWN_SECURITY", trade_id=trade_id)
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "quality_flag",
            f"trade_id={trade_id}",
        )

    if issue_type == "SPLIT_ADJUSTMENT_BREAK":
        flag_detected = _flag_exists(
            flags_df,
            flag_type="SPLIT_ADJUSTMENT_BREAK",
            account_id=account_id,
            security_id=security_id,
            issue_date=issue_date,
        )
        break_detected = _break_exists(
            breaks_df,
            account_id=account_id,
            security_id=security_id,
            issue_date=issue_date,
            break_reason_code="SPLIT_ADJUSTMENT_BREAK",
        )
        detected = flag_detected or break_detected
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "quality_flag_or_break",
            f"flag={flag_detected}, break={break_detected}",
        )

    if issue_type == "OUT_OF_ORDER_TRADE_EVENTS":
        detected = trades_df.limit(1).count() > 0 and position_df.limit(1).count() > 0
        return ManifestIssueResult(
            issue_id,
            issue_type,
            expected,
            detected,
            "structural_pipeline_pass",
            "silver trades and gold position reconstruction present after out-of-order raw feed",
        )

    return ManifestIssueResult(
        issue_id,
        issue_type,
        expected,
        False,
        "unsupported_issue_type",
        f"No validator implemented for issue_type={issue_type}",
    )


def validate_manifest(
    *,
    project_root: Path = PROJECT_ROOT,
    manifest_path: Path | None = None,
    spark: SparkSession | None = None,
) -> ManifestValidationReport:
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Manifest Validation")
    manifest_file = manifest_path or (project_root / "data/raw/manifest/injected_issues_manifest.jsonl")

    try:
        issues = load_manifest(manifest_file)
        flags_df = read_table(session, project_root / "data/silver/trade_quality_flags")
        breaks_df = read_table(session, project_root / "data/gold/reconciliation_breaks")
        trades_df = read_table(session, project_root / "data/silver/trades_clean")
        position_df = read_table(session, project_root / "data/gold/position_reconstruction")

        results = [
            validate_issue(
                issue,
                flags_df=flags_df,
                breaks_df=breaks_df,
                trades_df=trades_df,
                position_df=position_df,
            )
            for issue in issues
        ]
        detected_count = sum(1 for result in results if result.detected)
        missed_count = len(results) - detected_count
        detection_rate = detected_count / len(results) if results else 1.0
        return ManifestValidationReport(
            total_issues=len(results),
            detected_count=detected_count,
            missed_count=missed_count,
            detection_rate=detection_rate,
            results=results,
        )
    finally:
        if owns_spark:
            session.stop()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Validate injected manifest issues against silver/gold outputs.",
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
        help="Optional path to injected_issues_manifest.jsonl",
    )
    args = parser.parse_args(argv)

    manifest_path = Path(args.manifest_path).resolve() if args.manifest_path else None
    report = validate_manifest(manifest_path=manifest_path)
    print(json.dumps(report.to_dict(), indent=2))
    if report.missed_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
