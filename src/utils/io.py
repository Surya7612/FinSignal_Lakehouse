"""Shared I/O helpers for FinSignal Lakehouse pipelines."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_spark_session(app_name: str = "FinSignal Lakehouse") -> SparkSession:
    """Create a local-mode Spark session for batch pipeline jobs."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
