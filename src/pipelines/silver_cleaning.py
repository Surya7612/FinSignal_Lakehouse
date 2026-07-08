"""
Silver cleaning pipeline for FinSignal Lakehouse.

Reads bronze Parquet datasets, enforces basic typing and normalization,
preserves bronze metadata, and writes cleaned silver Parquet outputs.
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.algorithms.split_adjustment import attach_split_factor_by_date, build_split_factors
from src.utils.io import PROJECT_ROOT, create_spark_session
from src.validation.quality_checks import (
    build_split_adjustment_break_flags,
    build_trade_quality_flags,
)

BRONZE_METADATA_COLUMNS = [
    "_ingested_at",
    "_source_file",
    "_bronze_load_id",
    "_raw_record_hash",
]

SILVER_DATASETS: list[dict[str, str]] = [
    {
        "name": "securities_clean",
        "bronze_path": "data/bronze/securities",
        "silver_path": "data/silver/securities_clean",
    },
    {
        "name": "prices_clean",
        "bronze_path": "data/bronze/prices",
        "silver_path": "data/silver/prices_clean",
    },
    {
        "name": "corporate_actions_clean",
        "bronze_path": "data/bronze/corporate_actions",
        "silver_path": "data/silver/corporate_actions_clean",
    },
    {
        "name": "starting_positions_clean",
        "bronze_path": "data/bronze/starting_positions",
        "silver_path": "data/silver/starting_positions_clean",
    },
    {
        "name": "trades_clean",
        "bronze_path": "data/bronze/trades",
        "silver_path": "data/silver/trades_clean",
    },
    {
        "name": "reported_positions_clean",
        "bronze_path": "data/bronze/reported_positions",
        "silver_path": "data/silver/reported_positions_clean",
    },
    {
        "name": "events_clean",
        "bronze_path": "data/bronze/filing_events",
        "silver_path": "data/silver/events_clean",
    },
    {
        "name": "trade_quality_flags",
        "bronze_path": "",
        "silver_path": "data/silver/trade_quality_flags",
    },
]


@dataclass
class SilverCleaningResult:
    """Summary for one silver dataset write."""

    dataset_name: str
    silver_path: Path
    row_count: int


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def _normalize_id(column_name: str) -> F.Column:
    return F.upper(F.trim(F.col(column_name)))


def _normalize_text(column_name: str) -> F.Column:
    return F.trim(F.col(column_name))


def _with_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_processed_at", F.current_timestamp())


def _select_metadata(df: DataFrame) -> list[F.Column]:
    return [F.col(column) for column in BRONZE_METADATA_COLUMNS if column in df.columns]


def clean_securities(df: DataFrame) -> DataFrame:
    return _with_silver_metadata(
        df.select(
            _normalize_id("security_id").alias("security_id"),
            _normalize_id("ticker").alias("ticker"),
            _normalize_text("company_name").alias("company_name"),
            _normalize_id("exchange").alias("exchange"),
            _normalize_text("sector").alias("sector"),
            F.col("is_active").cast("boolean").alias("is_active"),
            *_select_metadata(df),
        )
    )


def clean_prices(df: DataFrame) -> DataFrame:
    return _with_silver_metadata(
        df.select(
            _normalize_id("security_id").alias("security_id"),
            F.to_date("price_date").alias("price_date"),
            F.col("open_price").cast("double").alias("open_price"),
            F.col("high_price").cast("double").alias("high_price"),
            F.col("low_price").cast("double").alias("low_price"),
            F.col("close_price").cast("double").alias("close_price"),
            F.col("volume").cast("long").alias("volume"),
            *_select_metadata(df),
        )
    )


def clean_corporate_actions(df: DataFrame) -> DataFrame:
    return _with_silver_metadata(
        df.select(
            _normalize_text("corporate_action_id").alias("corporate_action_id"),
            _normalize_id("security_id").alias("security_id"),
            _normalize_id("action_type").alias("action_type"),
            F.to_date("effective_date").alias("effective_date"),
            F.col("split_ratio").cast("double").alias("split_ratio"),
            _normalize_text("description").alias("description"),
            *_select_metadata(df),
        )
    )


def clean_starting_positions(df: DataFrame) -> DataFrame:
    return _with_silver_metadata(
        df.select(
            _normalize_id("account_id").alias("account_id"),
            _normalize_id("security_id").alias("security_id"),
            F.to_date("position_date").alias("position_date"),
            F.col("quantity").cast("double").alias("quantity"),
            *_select_metadata(df),
        )
    )


def clean_trades(df: DataFrame) -> DataFrame:
    normalized_side = F.upper(F.trim(F.col("side")))
    standardized_side = (
        F.when(normalized_side.isin("B", "BUY"), F.lit("BUY"))
        .when(normalized_side.isin("S", "SELL"), F.lit("SELL"))
        .otherwise(normalized_side)
    )

    return _with_silver_metadata(
        df.select(
            _normalize_text("trade_id").alias("trade_id"),
            _normalize_id("account_id").alias("account_id"),
            _normalize_id("security_id").alias("security_id"),
            standardized_side.alias("side"),
            F.col("quantity").cast("double").alias("quantity"),
            F.col("execution_price").cast("double").alias("execution_price"),
            F.to_date("trade_date").alias("trade_date"),
            F.to_date("settlement_date").alias("settlement_date"),
            F.to_timestamp("event_timestamp").alias("event_timestamp"),
            F.to_timestamp("ingestion_timestamp").alias("ingestion_timestamp"),
            _normalize_id("source_system").alias("source_system"),
            *_select_metadata(df),
        )
    )


def clean_reported_positions(df: DataFrame) -> DataFrame:
    return _with_silver_metadata(
        df.select(
            _normalize_id("account_id").alias("account_id"),
            _normalize_id("security_id").alias("security_id"),
            F.to_date("position_date").alias("position_date"),
            F.col("reported_quantity").cast("double").alias("reported_quantity"),
            _normalize_id("source_system").alias("source_system"),
            *_select_metadata(df),
        )
    )


def clean_events(df: DataFrame) -> DataFrame:
    return _with_silver_metadata(
        df.select(
            _normalize_text("event_id").alias("event_id"),
            _normalize_id("security_id").alias("security_id"),
            F.to_date("event_date").alias("event_date"),
            _normalize_id("event_type").alias("event_type"),
            _normalize_text("event_summary").alias("event_summary"),
            *_select_metadata(df),
        )
    )


def _write_parquet(df: DataFrame, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    row_count = df.count()
    df.write.mode("overwrite").parquet(str(path))
    return row_count


def _load_bronze_dataset(
    spark: SparkSession,
    *,
    bronze_path: str,
    project_root: Path,
) -> DataFrame:
    return spark.read.parquet(str(_resolve_path(bronze_path, project_root)))


def run_silver_cleaning(
    run_id: str,
    *,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> tuple[list[SilverCleaningResult], list[tuple[str, int]]]:
    """Run silver cleaning and trade-quality flag generation."""
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Silver Cleaning")

    try:
        bronze_securities = _load_bronze_dataset(
            session, bronze_path="data/bronze/securities", project_root=project_root
        )
        bronze_prices = _load_bronze_dataset(
            session, bronze_path="data/bronze/prices", project_root=project_root
        )
        bronze_corporate_actions = _load_bronze_dataset(
            session, bronze_path="data/bronze/corporate_actions", project_root=project_root
        )
        bronze_starting_positions = _load_bronze_dataset(
            session, bronze_path="data/bronze/starting_positions", project_root=project_root
        )
        bronze_trades = _load_bronze_dataset(
            session, bronze_path="data/bronze/trades", project_root=project_root
        )
        bronze_reported_positions = _load_bronze_dataset(
            session, bronze_path="data/bronze/reported_positions", project_root=project_root
        )
        bronze_events = _load_bronze_dataset(
            session, bronze_path="data/bronze/filing_events", project_root=project_root
        )

        silver_securities = clean_securities(bronze_securities)
        silver_prices = clean_prices(bronze_prices)
        silver_corporate_actions = clean_corporate_actions(bronze_corporate_actions)
        silver_starting_positions = clean_starting_positions(bronze_starting_positions)
        silver_trades = clean_trades(bronze_trades)
        silver_reported_positions = clean_reported_positions(bronze_reported_positions)
        silver_events = clean_events(bronze_events)

        split_factors = build_split_factors(
            prices_df=silver_prices,
            corporate_actions_df=silver_corporate_actions,
        )

        silver_prices = (
            attach_split_factor_by_date(
                silver_prices,
                date_col="price_date",
                split_factors_df=split_factors,
            )
            .withColumn("adjusted_open_price", F.col("open_price") / F.col("split_adjustment_factor"))
            .withColumn("adjusted_high_price", F.col("high_price") / F.col("split_adjustment_factor"))
            .withColumn("adjusted_low_price", F.col("low_price") / F.col("split_adjustment_factor"))
            .withColumn("adjusted_close_price", F.col("close_price") / F.col("split_adjustment_factor"))
        )

        silver_starting_positions = (
            attach_split_factor_by_date(
                silver_starting_positions,
                date_col="position_date",
                split_factors_df=split_factors,
            )
            .withColumn("adjusted_quantity", F.col("quantity") * F.col("split_adjustment_factor"))
        )

        silver_trades = (
            attach_split_factor_by_date(
                silver_trades,
                date_col="trade_date",
                split_factors_df=split_factors,
            )
            .withColumn("adjusted_quantity", F.col("quantity") * F.col("split_adjustment_factor"))
            .withColumn(
                "adjusted_execution_price",
                F.col("execution_price") / F.col("split_adjustment_factor"),
            )
        )

        silver_reported_positions = (
            attach_split_factor_by_date(
                silver_reported_positions,
                date_col="position_date",
                split_factors_df=split_factors,
            )
            .withColumn(
                "adjusted_reported_quantity",
                F.col("reported_quantity") * F.col("split_adjustment_factor"),
            )
        )

        flags_df = build_trade_quality_flags(
            trades_df=silver_trades,
            securities_df=silver_securities,
            prices_df=silver_prices,
        )
        split_flags_df = build_split_adjustment_break_flags(
            reported_positions_df=silver_reported_positions,
            corporate_actions_df=silver_corporate_actions,
        )
        flags_df = (
            flags_df.unionByName(split_flags_df)
            .dropDuplicates(["flag_id"])
            .withColumn("_silver_processed_at", F.current_timestamp())
        )

        outputs = [
            ("securities_clean", silver_securities),
            ("prices_clean", silver_prices),
            ("corporate_actions_clean", silver_corporate_actions),
            ("starting_positions_clean", silver_starting_positions),
            ("trades_clean", silver_trades),
            ("reported_positions_clean", silver_reported_positions),
            ("events_clean", silver_events),
            ("trade_quality_flags", flags_df),
        ]

        results: list[SilverCleaningResult] = []
        for dataset_name, dataframe in outputs:
            output_path = _resolve_path(f"data/silver/{dataset_name}", project_root)
            row_count = _write_parquet(dataframe, output_path)
            results.append(
                SilverCleaningResult(
                    dataset_name=dataset_name,
                    silver_path=output_path,
                    row_count=row_count,
                )
            )

        flag_counts = [
            (row["flag_type"], row["count"])
            for row in flags_df.groupBy("flag_type").count().orderBy("flag_type").collect()
        ]
        return results, flag_counts
    finally:
        if owns_spark:
            session.stop()


def _print_results(results: list[SilverCleaningResult], flag_counts: list[tuple[str, int]], run_id: str) -> None:
    print("FinSignal Lakehouse — Silver Cleaning Complete")
    print(f"silver_run_id: {run_id}")
    print("-" * 72)
    for result in results:
        print(f"dataset:   {result.dataset_name}")
        print(f"path:      {result.silver_path}")
        print(f"row count: {result.row_count}")
        print("-" * 72)

    print("Trade quality flags by flag_type:")
    if not flag_counts:
        print("  none")
    for flag_type, count in flag_counts:
        print(f"  {flag_type}: {count}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Clean bronze datasets into silver Parquet tables.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Silver run identifier for logging (default: generated UUID).",
    )
    args = parser.parse_args(argv)

    run_id = args.run_id or str(uuid.uuid4())
    results, flag_counts = run_silver_cleaning(run_id=run_id)
    _print_results(results, flag_counts, run_id)


if __name__ == "__main__":
    main()
