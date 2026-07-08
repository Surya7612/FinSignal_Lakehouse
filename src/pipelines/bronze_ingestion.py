"""
Bronze ingestion pipeline for FinSignal Lakehouse.

Reads raw JSONL datasets, preserves source fields, adds bronze metadata,
and writes Parquet outputs. No silver cleaning or business validation.
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, sha2, struct, to_json

from src.utils.io import PROJECT_ROOT, create_spark_session

# ---------------------------------------------------------------------------
# Explicit raw -> bronze dataset mapping
# ---------------------------------------------------------------------------
BRONZE_DATASETS: list[dict[str, str]] = [
    {
        "name": "securities",
        "raw_path": "data/raw/securities",
        "bronze_path": "data/bronze/securities",
    },
    {
        "name": "prices",
        "raw_path": "data/raw/prices",
        "bronze_path": "data/bronze/prices",
    },
    {
        "name": "corporate_actions",
        "raw_path": "data/raw/corporate_actions",
        "bronze_path": "data/bronze/corporate_actions",
    },
    {
        "name": "starting_positions",
        "raw_path": "data/raw/starting_positions",
        "bronze_path": "data/bronze/starting_positions",
    },
    {
        "name": "trades",
        "raw_path": "data/raw/trades",
        "bronze_path": "data/bronze/trades",
    },
    {
        "name": "reported_positions",
        "raw_path": "data/raw/reported_positions",
        "bronze_path": "data/bronze/reported_positions",
    },
    {
        "name": "filing_events",
        "raw_path": "data/raw/filing_events",
        "bronze_path": "data/bronze/filing_events",
    },
    {
        "name": "injected_issues_manifest",
        "raw_path": "data/raw/manifest",
        "bronze_path": "data/bronze/injected_issues_manifest",
    },
]

BRONZE_METADATA_COLUMNS = ("_ingested_at", "_source_file", "_bronze_load_id", "_raw_record_hash")


@dataclass
class BronzeIngestionResult:
    """Summary for one bronze dataset load."""

    dataset_name: str
    raw_path: Path
    bronze_path: Path
    row_count: int


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def _add_bronze_metadata(df: DataFrame, load_id: str) -> DataFrame:
    """
    Preserve raw columns and append bronze ingestion metadata.

    The record hash is computed from the original raw columns only so it can
    be used later for idempotency and change detection.
    """
    original_columns = [column for column in df.columns if column not in BRONZE_METADATA_COLUMNS]

    bronze_df = (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_bronze_load_id", lit(load_id))
    )

    # Hash the untouched raw payload shape before metadata columns existed.
    bronze_df = bronze_df.withColumn(
        "_raw_record_hash",
        sha2(to_json(struct(*[col(column) for column in original_columns])), 256),
    )
    return bronze_df


def ingest_dataset(
    spark: SparkSession,
    *,
    dataset_name: str,
    raw_path: Path,
    bronze_path: Path,
    load_id: str,
) -> BronzeIngestionResult:
    """Read one raw JSONL dataset and write bronze Parquet with metadata."""
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw path does not exist for {dataset_name}: {raw_path}")

    # Spark reads JSON/JSONL from a file or directory path.
    raw_df = spark.read.json(str(raw_path))
    bronze_df = _add_bronze_metadata(raw_df, load_id)

    row_count = bronze_df.count()
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    # MVP uses full overwrite per dataset load.
    bronze_df.write.mode("overwrite").parquet(str(bronze_path))

    return BronzeIngestionResult(
        dataset_name=dataset_name,
        raw_path=raw_path,
        bronze_path=bronze_path,
        row_count=row_count,
    )


def run_bronze_ingestion(
    load_id: str,
    *,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> list[BronzeIngestionResult]:
    """Ingest all configured raw datasets into bronze Parquet tables."""
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Bronze Ingestion")

    results: list[BronzeIngestionResult] = []
    try:
        for dataset in BRONZE_DATASETS:
            raw_path = _resolve_path(dataset["raw_path"], project_root)
            bronze_path = _resolve_path(dataset["bronze_path"], project_root)

            result = ingest_dataset(
                session,
                dataset_name=dataset["name"],
                raw_path=raw_path,
                bronze_path=bronze_path,
                load_id=load_id,
            )
            results.append(result)
    finally:
        if owns_spark:
            session.stop()

    return results


def _print_results(results: list[BronzeIngestionResult], load_id: str) -> None:
    print("FinSignal Lakehouse — Bronze Ingestion Complete")
    print(f"bronze_load_id: {load_id}")
    print("-" * 72)
    for result in results:
        print(f"dataset:     {result.dataset_name}")
        print(f"raw path:    {result.raw_path}")
        print(f"bronze path: {result.bronze_path}")
        print(f"row count:   {result.row_count}")
        print("-" * 72)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest raw JSONL datasets into bronze Parquet tables.",
    )
    parser.add_argument(
        "--load-id",
        default=None,
        help="Bronze load identifier stored in _bronze_load_id (default: generated UUID).",
    )
    args = parser.parse_args(argv)

    load_id = args.load_id or str(uuid.uuid4())
    results = run_bronze_ingestion(load_id=load_id)
    _print_results(results, load_id)


if __name__ == "__main__":
    main()
