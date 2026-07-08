"""
Gold reconciliation pipeline for FinSignal Lakehouse.

This module only builds:
- gold position reconstruction
- gold reconciliation breaks
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.algorithms.reconciliation import build_position_reconstruction
from src.utils.io import PROJECT_ROOT, create_spark_session


@dataclass
class GoldReconciliationResult:
    """Summary statistics for one gold reconciliation run."""

    position_reconstruction_count: int
    reconciliation_breaks_count: int
    match_count: int
    break_count: int
    break_counts_by_reason: list[tuple[str, int]]
    break_counts_by_root_cause: list[tuple[str, int]]


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def _load_silver_inputs(spark: SparkSession, project_root: Path) -> dict[str, DataFrame]:
    return {
        "securities": spark.read.parquet(str(_resolve_path("data/silver/securities_clean", project_root))),
        "prices": spark.read.parquet(str(_resolve_path("data/silver/prices_clean", project_root))),
        "starting_positions": spark.read.parquet(
            str(_resolve_path("data/silver/starting_positions_clean", project_root))
        ),
        "trades": spark.read.parquet(str(_resolve_path("data/silver/trades_clean", project_root))),
        "reported_positions": spark.read.parquet(
            str(_resolve_path("data/silver/reported_positions_clean", project_root))
        ),
        "trade_quality_flags": spark.read.parquet(
            str(_resolve_path("data/silver/trade_quality_flags", project_root))
        ),
    }


def _build_reconciliation_breaks(position_reconstruction_df: DataFrame) -> DataFrame:
    breaks = position_reconstruction_df.filter(F.col("reconciliation_status") == F.lit("BREAK"))
    severity = (
        F.when(F.abs(F.col("position_difference")) >= F.lit(100), F.lit("HIGH"))
        .when(F.abs(F.col("position_difference")) >= F.lit(25), F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )

    notes = (
        F.when(
            F.col("break_reason_code") == F.lit("POSITION_NOT_REPORTED"),
            F.lit("No reported position exists for this account/security/date."),
        )
        .when(
            F.col("break_reason_code") == F.lit("DUPLICATE_TRADE"),
            F.lit("Difference aligns with duplicate-trade flags on this date."),
        )
        .when(
            F.col("break_reason_code") == F.lit("LATE_ARRIVING_TRADE"),
            F.lit("Difference aligns with late-arriving trade flags on this date."),
        )
        .when(
            F.col("break_reason_code") == F.lit("MISSING_PRICE"),
            F.lit("Difference aligns with missing-price flags on this date."),
        )
        .otherwise(F.lit("Expected and reported quantities differ for this date."))
    )

    return breaks.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.col("account_id"),
                F.col("security_id"),
                F.col("position_date").cast("string"),
                F.col("break_reason_code"),
                F.col("root_cause_reason_code"),
            ),
            256,
        ).alias("break_id"),
        "account_id",
        "security_id",
        "position_date",
        "expected_position",
        "reported_position",
        "position_difference",
        "break_reason_code",
        "root_cause_reason_code",
        "first_break_date",
        "days_since_first_break",
        "is_cascading_break",
        severity.alias("severity"),
        notes.alias("investigation_notes"),
        "_gold_processed_at",
    )


def run_gold_reconciliation(
    run_id: str,
    *,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> GoldReconciliationResult:
    """Run gold position reconstruction and reconciliation break extraction."""
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Gold Reconciliation")

    try:
        silver = _load_silver_inputs(session, project_root)

        position_reconstruction_df = build_position_reconstruction(
            securities_df=silver["securities"],
            starting_positions_df=silver["starting_positions"],
            trades_df=silver["trades"],
            reported_positions_df=silver["reported_positions"],
            trade_quality_flags_df=silver["trade_quality_flags"],
        ).withColumn("_gold_processed_at", F.current_timestamp())

        reconciliation_breaks_df = _build_reconciliation_breaks(position_reconstruction_df)

        position_path = _resolve_path("data/gold/position_reconstruction", project_root)
        breaks_path = _resolve_path("data/gold/reconciliation_breaks", project_root)
        position_path.parent.mkdir(parents=True, exist_ok=True)
        breaks_path.parent.mkdir(parents=True, exist_ok=True)

        position_reconstruction_df.write.mode("overwrite").parquet(str(position_path))
        reconciliation_breaks_df.write.mode("overwrite").parquet(str(breaks_path))

        position_count = position_reconstruction_df.count()
        break_count = reconciliation_breaks_df.count()
        match_count = position_reconstruction_df.filter(
            F.col("reconciliation_status") == F.lit("MATCH")
        ).count()
        break_status_count = position_reconstruction_df.filter(
            F.col("reconciliation_status") == F.lit("BREAK")
        ).count()
        break_counts_by_reason = [
            (row["break_reason_code"], row["count"])
            for row in reconciliation_breaks_df.groupBy("break_reason_code")
            .count()
            .orderBy("break_reason_code")
            .collect()
        ]
        break_counts_by_root_cause = [
            (row["root_cause_reason_code"], row["count"])
            for row in reconciliation_breaks_df.groupBy("root_cause_reason_code")
            .count()
            .orderBy("root_cause_reason_code")
            .collect()
        ]

        # keep run_id consumed for future partitioning/logging extension
        _ = run_id

        return GoldReconciliationResult(
            position_reconstruction_count=position_count,
            reconciliation_breaks_count=break_count,
            match_count=match_count,
            break_count=break_status_count,
            break_counts_by_reason=break_counts_by_reason,
            break_counts_by_root_cause=break_counts_by_root_cause,
        )
    finally:
        if owns_spark:
            session.stop()


def _print_summary(result: GoldReconciliationResult, run_id: str) -> None:
    print("FinSignal Lakehouse — Gold Reconciliation Complete")
    print(f"gold_run_id: {run_id}")
    print("-" * 72)
    print(f"position_reconstruction row count: {result.position_reconstruction_count}")
    print(f"reconciliation_breaks row count:   {result.reconciliation_breaks_count}")
    print(f"match count:                       {result.match_count}")
    print(f"break count:                       {result.break_count}")
    print("-" * 72)
    print("break counts by reason code:")
    if not result.break_counts_by_reason:
        print("  none")
    for reason, count in result.break_counts_by_reason:
        print(f"  {reason}: {count}")
    print("break counts by root_cause_reason_code:")
    if not result.break_counts_by_root_cause:
        print("  none")
    for reason, count in result.break_counts_by_root_cause:
        print(f"  {reason}: {count}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run gold position reconstruction and reconciliation breaks.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Gold reconciliation run identifier (default: generated UUID).",
    )
    args = parser.parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())

    result = run_gold_reconciliation(run_id=run_id)
    _print_summary(result, run_id)


if __name__ == "__main__":
    main()
