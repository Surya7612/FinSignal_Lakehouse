"""Shared I/O helpers for FinSignal Lakehouse pipelines.

Centralizes Spark session creation and table read/write so pipelines,
validation, and retrieval modules share the same Delta/Parquet behavior.
Set FINSIGNAL_STORAGE_FORMAT=parquet to override the default Delta format.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STORAGE_FORMAT = os.environ.get("FINSIGNAL_STORAGE_FORMAT", "delta").lower()
SUPPORTED_STORAGE_FORMATS = frozenset({"delta", "parquet"})


def normalize_storage_format(storage_format: str | None = None) -> str:
    """Return a supported storage format, defaulting to STORAGE_FORMAT."""
    fmt = (storage_format or STORAGE_FORMAT).lower()
    if fmt not in SUPPORTED_STORAGE_FORMATS:
        supported = ", ".join(sorted(SUPPORTED_STORAGE_FORMATS))
        raise ValueError(f"Unsupported storage format '{fmt}'. Supported formats: {supported}.")
    return fmt


def create_spark_session(app_name: str = "FinSignal Lakehouse") -> SparkSession:
    """Create a local-mode Spark session for batch pipeline jobs."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if normalize_storage_format() == "delta":
        from delta import configure_spark_with_delta_pip

        builder = (
            configure_spark_with_delta_pip(builder)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return builder.getOrCreate()
    return builder.getOrCreate()


def read_table(
    spark: SparkSession,
    path: str | Path,
    storage_format: str | None = None,
) -> DataFrame:
    """Read a bronze/silver/gold table using the configured storage format."""
    resolved_path = Path(path).resolve()
    fmt = normalize_storage_format(storage_format)
    return spark.read.format(fmt).load(str(resolved_path))


def write_table(
    df: DataFrame,
    path: str | Path,
    storage_format: str | None = None,
    *,
    mode: str = "overwrite",
) -> None:
    """Write a bronze/silver/gold table using the configured storage format."""
    resolved_path = Path(path).resolve()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = normalize_storage_format(storage_format)
    df.write.mode(mode).format(fmt).save(str(resolved_path))
