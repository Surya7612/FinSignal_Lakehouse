"""Gold event-window analytics pipeline for FinSignal Lakehouse."""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.algorithms.event_window import build_event_window_metrics
from src.utils.io import PROJECT_ROOT, create_spark_session


@dataclass
class GoldEventWindowResult:
    """Summary for one gold event-window run."""

    event_count: int
    output_row_count: int
    events_with_full_window_count: int
    events_with_partial_window_count: int
    null_metric_counts: list[tuple[str, int]]


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def run_gold_event_window(
    run_id: str,
    *,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> GoldEventWindowResult:
    """Compute bounded event-window metrics and write the gold output."""
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Gold Event Window")

    try:
        prices_df = session.read.parquet(str(_resolve_path("data/silver/prices_clean", project_root)))
        events_df = session.read.parquet(str(_resolve_path("data/silver/events_clean", project_root)))
        position_df = session.read.parquet(
            str(_resolve_path("data/gold/position_reconstruction", project_root))
        )

        metrics_df = build_event_window_metrics(
            prices_df=prices_df,
            events_df=events_df,
            position_reconstruction_df=position_df,
        ).withColumn("_gold_processed_at", F.current_timestamp())

        output_path = _resolve_path("data/gold/event_window_metrics", project_root)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_df.write.mode("overwrite").parquet(str(output_path))

        event_count = events_df.select("event_id", "security_id").dropDuplicates().count()
        output_count = metrics_df.count()
        full_window_count = metrics_df.filter(F.col("is_full_window") == F.lit(True)).count()
        partial_window_count = output_count - full_window_count

        metric_columns = [
            "pre_event_return",
            "post_event_return",
            "event_day_return",
            "pre_event_volatility",
            "post_event_volatility",
            "position_before_event",
            "position_after_event",
            "position_change_around_event",
            "exposure_before_event",
            "exposure_after_event",
            "exposure_change_around_event",
        ]
        null_metric_counts = [
            (column, metrics_df.filter(F.col(column).isNull()).count()) for column in metric_columns
        ]

        _ = run_id
        return GoldEventWindowResult(
            event_count=event_count,
            output_row_count=output_count,
            events_with_full_window_count=full_window_count,
            events_with_partial_window_count=partial_window_count,
            null_metric_counts=null_metric_counts,
        )
    finally:
        if owns_spark:
            session.stop()


def _print_summary(result: GoldEventWindowResult, run_id: str) -> None:
    print("FinSignal Lakehouse — Gold Event Window Complete")
    print(f"gold_event_run_id: {run_id}")
    print("-" * 72)
    print(f"event count:                 {result.event_count}")
    print(f"output row count:            {result.output_row_count}")
    print(f"events with full window:     {result.events_with_full_window_count}")
    print(f"events with partial window:  {result.events_with_partial_window_count}")
    print("-" * 72)
    print("null metric counts:")
    for column, count in result.null_metric_counts:
        print(f"  {column}: {count}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Compute bounded gold event-window analytics metrics.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Gold event-window run identifier (default: generated UUID).",
    )
    args = parser.parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())

    result = run_gold_event_window(run_id=run_id)
    _print_summary(result, run_id)


if __name__ == "__main__":
    main()
