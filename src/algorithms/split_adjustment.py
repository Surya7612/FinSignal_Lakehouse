"""Stock-split adjustment helpers (MVP: STOCK_SPLIT only)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_split_factors(prices_df: DataFrame, corporate_actions_df: DataFrame) -> DataFrame:
    """
    Build cumulative split factors by security/date.

    Factor definition (historical adjustment):
    - For each security/date, multiply split_ratio for split actions whose
      effective_date is strictly after that date.
    - Dates on/after effective_date are not scaled by that action.
    """
    base_dates = prices_df.select("security_id", "price_date").dropDuplicates()
    splits = corporate_actions_df.filter(F.col("action_type") == F.lit("STOCK_SPLIT")).select(
        "security_id",
        "effective_date",
        "split_ratio",
    )

    joined = base_dates.alias("d").join(
        splits.alias("s"),
        on=(F.col("d.security_id") == F.col("s.security_id"))
        & (F.col("s.effective_date") > F.col("d.price_date")),
        how="left",
    )

    # Product via exp(sum(log(x))) for split_ratio > 0.
    return (
        joined.groupBy(F.col("d.security_id").alias("security_id"), F.col("d.price_date").alias("price_date"))
        .agg(
            F.exp(F.sum(F.when(F.col("s.split_ratio").isNotNull(), F.log(F.col("s.split_ratio"))))).alias(
                "split_adjustment_factor"
            )
        )
        .withColumn("split_adjustment_factor", F.coalesce(F.col("split_adjustment_factor"), F.lit(1.0)))
    )


def attach_split_factor_by_date(
    df: DataFrame,
    *,
    date_col: str,
    split_factors_df: DataFrame,
) -> DataFrame:
    """Attach split_adjustment_factor to any security/date keyed dataframe."""
    factors = split_factors_df.select(
        "security_id",
        F.col("price_date").alias(date_col),
        "split_adjustment_factor",
    )
    return (
        df.join(factors, on=["security_id", date_col], how="left")
        .withColumn("split_adjustment_factor", F.coalesce(F.col("split_adjustment_factor"), F.lit(1.0)))
    )
