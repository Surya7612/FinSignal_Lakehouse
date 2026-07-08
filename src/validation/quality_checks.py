"""Reusable quality checks for the silver layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_trade_quality_flags(
    trades_df: DataFrame,
    securities_df: DataFrame,
    prices_df: DataFrame,
) -> DataFrame:
    """Build trade-quality flags without dropping bad trade rows."""
    security_keys = securities_df.select("security_id").dropDuplicates()
    price_keys = (
        prices_df.select(
            "security_id",
            F.col("price_date").alias("trade_date"),
        ).dropDuplicates()
    )

    trades_with_security = trades_df.join(
        security_keys.withColumn("_known_security", F.lit(True)),
        on="security_id",
        how="left",
    )
    trades_with_refs = trades_with_security.join(
        price_keys.withColumn("_has_price", F.lit(True)),
        on=["security_id", "trade_date"],
        how="left",
    )

    duplicate_trade_ids = (
        trades_df.groupBy("trade_id")
        .count()
        .filter(F.col("count") > 1)
        .select("trade_id")
        .withColumn("_duplicate_trade", F.lit(True))
    )
    flagged_trades = trades_with_refs.join(duplicate_trade_ids, on="trade_id", how="left")

    def make_flags(
        condition: F.Column,
        *,
        flag_type: str,
        severity: str,
        description: str,
    ) -> DataFrame:
        return (
            flagged_trades.filter(condition)
            .select(
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit(flag_type),
                        F.coalesce(F.col("trade_id"), F.lit("")),
                        F.coalesce(F.col("account_id"), F.lit("")),
                        F.coalesce(F.col("security_id"), F.lit("")),
                        F.coalesce(F.col("trade_date").cast("string"), F.lit("")),
                    ),
                    256,
                ).alias("flag_id"),
                F.lit(flag_type).alias("flag_type"),
                F.lit("trades").alias("affected_entity"),
                F.col("trade_id").alias("affected_record_id"),
                F.col("account_id"),
                F.col("security_id"),
                F.col("trade_date").alias("flag_date"),
                F.lit(severity).alias("severity"),
                F.lit(description).alias("description"),
            )
        )

    duplicate_flags = make_flags(
        F.col("_duplicate_trade") == True,  # noqa: E712
        flag_type="DUPLICATE_TRADE",
        severity="HIGH",
        description="Duplicate trade_id detected in silver trades.",
    )
    unknown_security_flags = make_flags(
        F.col("_known_security").isNull(),
        flag_type="UNKNOWN_SECURITY",
        severity="HIGH",
        description="Trade references a security_id not present in securities_clean.",
    )
    # Only evaluate missing-price for known securities.
    # Unknown securities are captured by UNKNOWN_SECURITY and should not
    # also be flagged as MISSING_PRICE.
    missing_price_flags = make_flags(
        F.col("_known_security").isNotNull() & F.col("_has_price").isNull(),
        flag_type="MISSING_PRICE",
        severity="MEDIUM",
        description="No matching price record found for trade security_id and trade_date.",
    )
    invalid_quantity_flags = make_flags(
        F.col("quantity").isNull() | (F.col("quantity") <= 0),
        flag_type="INVALID_QUANTITY",
        severity="HIGH",
        description="Trade quantity is null or not strictly positive.",
    )
    invalid_price_flags = make_flags(
        F.col("execution_price").isNull() | (F.col("execution_price") <= 0),
        flag_type="INVALID_PRICE",
        severity="HIGH",
        description="Execution price is null or not strictly positive.",
    )
    late_arriving_flags = make_flags(
        F.to_date("ingestion_timestamp") > F.col("trade_date"),
        flag_type="LATE_ARRIVING_TRADE",
        severity="MEDIUM",
        description="Ingestion timestamp date occurs after the trade date.",
    )

    return (
        duplicate_flags.unionByName(unknown_security_flags)
        .unionByName(missing_price_flags)
        .unionByName(invalid_quantity_flags)
        .unionByName(invalid_price_flags)
        .unionByName(late_arriving_flags)
        .dropDuplicates(["flag_id"])
    )


def build_split_adjustment_break_flags(
    reported_positions_df: DataFrame,
    corporate_actions_df: DataFrame,
) -> DataFrame:
    """
    Flag split-adjustment breaks on reported positions around split effective date.

    Heuristic:
    - For each STOCK_SPLIT security/effective_date and account, compare
      reported_quantity on effective_date vs previous trading day.
    - If effective quantity is materially below expected split-adjusted level,
      emit SPLIT_ADJUSTMENT_BREAK.
    """
    split_actions = corporate_actions_df.filter(F.col("action_type") == F.lit("STOCK_SPLIT")).select(
        "security_id",
        F.col("effective_date"),
        "split_ratio",
        "corporate_action_id",
    )

    current_day = reported_positions_df.alias("r").join(
        split_actions.alias("s"),
        on=(F.col("r.security_id") == F.col("s.security_id"))
        & (F.col("r.position_date") == F.col("s.effective_date")),
        how="inner",
    ).select(
        F.col("r.account_id").alias("account_id"),
        F.col("r.security_id").alias("security_id"),
        F.col("r.position_date").alias("position_date"),
        F.col("r.reported_quantity").alias("effective_reported_quantity"),
        F.col("s.split_ratio").alias("split_ratio"),
        F.col("s.corporate_action_id").alias("corporate_action_id"),
    )

    previous_day = (
        reported_positions_df.select(
            "account_id",
            "security_id",
            F.col("position_date").alias("prev_position_date"),
            F.col("reported_quantity").alias("prev_reported_quantity"),
        )
    )

    paired = current_day.join(
        previous_day,
        on=[
            "account_id",
            "security_id",
        ],
        how="left",
    ).filter(F.col("prev_position_date") < F.col("position_date"))

    # nearest previous position date
    win = Window.partitionBy("account_id", "security_id", "position_date").orderBy(
        F.col("prev_position_date").desc()
    )
    paired = paired.withColumn("rn", F.row_number().over(win)).filter(F.col("rn") == 1)

    expected_effective_qty = F.col("prev_reported_quantity") * F.col("split_ratio")
    flagged = paired.filter(
        F.col("prev_reported_quantity").isNotNull()
        & F.col("effective_reported_quantity").isNotNull()
        & (F.abs(F.col("effective_reported_quantity") - expected_effective_qty) > F.lit(1e-9))
    )

    return flagged.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("SPLIT_ADJUSTMENT_BREAK"),
                F.col("account_id"),
                F.col("security_id"),
                F.col("position_date").cast("string"),
                F.col("corporate_action_id"),
            ),
            256,
        ).alias("flag_id"),
        F.lit("SPLIT_ADJUSTMENT_BREAK").alias("flag_type"),
        F.lit("reported_positions").alias("affected_entity"),
        F.concat_ws(
            "|",
            F.col("account_id"),
            F.col("security_id"),
            F.col("position_date").cast("string"),
        ).alias("affected_record_id"),
        F.col("account_id"),
        F.col("security_id"),
        F.col("position_date").alias("flag_date"),
        F.lit("HIGH").alias("severity"),
        F.lit(
            "Reported position does not align with expected split-adjusted quantity around split date."
        ).alias("description"),
    )
