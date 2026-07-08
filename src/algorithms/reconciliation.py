"""Core reconciliation dataframe logic for the gold layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_position_reconstruction(
    *,
    securities_df: DataFrame,
    starting_positions_df: DataFrame,
    trades_df: DataFrame,
    reported_positions_df: DataFrame,
    trade_quality_flags_df: DataFrame,
) -> DataFrame:
    """
    Reconstruct expected positions (trade-date view) and assign break reasons.

    Rules implemented:
    - Expected position starts from Day 0 baseline from starting_positions.
    - BUY adds quantity; SELL subtracts quantity (cumulative by date).
    - UNKNOWN_SECURITY trades are excluded from expected-position math.
    - Duplicate and late-arriving trades remain in expected-position math.
    """
    valid_security_ids = securities_df.select("security_id").dropDuplicates()

    trade_quantity_col = "adjusted_quantity" if "adjusted_quantity" in trades_df.columns else "quantity"
    starting_quantity_col = (
        "adjusted_quantity" if "adjusted_quantity" in starting_positions_df.columns else "quantity"
    )
    reported_quantity_col = (
        "adjusted_reported_quantity"
        if "adjusted_reported_quantity" in reported_positions_df.columns
        else "reported_quantity"
    )

    # Exclude trades with unknown securities from quantity math.
    valid_trades = trades_df.join(valid_security_ids, on="security_id", how="inner")

    # Daily buy/sell activity by account/security/date.
    daily_trade_activity = (
        valid_trades.groupBy("account_id", "security_id", F.col("trade_date").alias("position_date"))
        .agg(
            F.sum(F.when(F.col("side") == "BUY", F.col(trade_quantity_col)).otherwise(F.lit(0.0))).alias(
                "daily_buy_quantity"
            ),
            F.sum(F.when(F.col("side") == "SELL", F.col(trade_quantity_col)).otherwise(F.lit(0.0))).alias(
                "daily_sell_quantity"
            ),
        )
    )

    # Build account/security keys and date spine for position-day rows.
    keys = (
        starting_positions_df.select("account_id", "security_id")
        .unionByName(valid_trades.select("account_id", "security_id"))
        .unionByName(reported_positions_df.select("account_id", "security_id"))
        .dropDuplicates()
    )
    all_dates = (
        starting_positions_df.select(F.col("position_date"))
        .unionByName(valid_trades.select(F.col("trade_date").alias("position_date")))
        .unionByName(reported_positions_df.select(F.col("position_date")))
        .dropDuplicates()
    )
    scaffold = keys.crossJoin(all_dates)

    # Day 0 baseline quantity by account/security (defaults to 0 if missing).
    starting_baseline = (
        starting_positions_df.groupBy("account_id", "security_id")
        .agg(F.max(F.col(starting_quantity_col)).alias("starting_quantity"))
        .withColumn("starting_quantity", F.coalesce(F.col("starting_quantity"), F.lit(0.0)))
    )

    base_df = (
        scaffold.join(starting_baseline, on=["account_id", "security_id"], how="left")
        .join(daily_trade_activity, on=["account_id", "security_id", "position_date"], how="left")
        .withColumn("starting_quantity", F.coalesce(F.col("starting_quantity"), F.lit(0.0)))
        .withColumn("daily_buy_quantity", F.coalesce(F.col("daily_buy_quantity"), F.lit(0.0)))
        .withColumn("daily_sell_quantity", F.coalesce(F.col("daily_sell_quantity"), F.lit(0.0)))
    )

    # Cumulative quantity math by date.
    cumulative_window = (
        Window.partitionBy("account_id", "security_id")
        .orderBy("position_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    base_df = (
        base_df.withColumn(
            "cumulative_buy_quantity",
            F.sum(F.col("daily_buy_quantity")).over(cumulative_window),
        )
        .withColumn(
            "cumulative_sell_quantity",
            F.sum(F.col("daily_sell_quantity")).over(cumulative_window),
        )
        .withColumn(
            "expected_position",
            F.col("starting_quantity")
            + F.col("cumulative_buy_quantity")
            - F.col("cumulative_sell_quantity"),
        )
    )

    # Join reported positions.
    reported_df = reported_positions_df.select(
        "account_id",
        "security_id",
        "position_date",
        F.col(reported_quantity_col).alias("reported_position"),
    )
    base_df = base_df.join(
        reported_df,
        on=["account_id", "security_id", "position_date"],
        how="left",
    )

    # Bring in daily trade-quality flag signals.
    flag_signals = (
        trade_quality_flags_df.groupBy("account_id", "security_id", F.col("flag_date").alias("position_date"))
        .agg(
            F.max(
                F.when(F.col("flag_type") == "SPLIT_ADJUSTMENT_BREAK", F.lit(1)).otherwise(F.lit(0))
            ).alias("has_split_adjustment_break_flag"),
            F.max(F.when(F.col("flag_type") == "DUPLICATE_TRADE", F.lit(1)).otherwise(F.lit(0))).alias(
                "has_duplicate_trade_flag"
            ),
            F.max(
                F.when(F.col("flag_type") == "LATE_ARRIVING_TRADE", F.lit(1)).otherwise(F.lit(0))
            ).alias("has_late_arriving_trade_flag"),
            F.max(F.when(F.col("flag_type") == "MISSING_PRICE", F.lit(1)).otherwise(F.lit(0))).alias(
                "has_missing_price_flag"
            ),
        )
    )
    base_df = (
        base_df.join(flag_signals, on=["account_id", "security_id", "position_date"], how="left")
        .withColumn(
            "has_split_adjustment_break_flag",
            F.coalesce(F.col("has_split_adjustment_break_flag"), F.lit(0)),
        )
        .withColumn("has_duplicate_trade_flag", F.coalesce(F.col("has_duplicate_trade_flag"), F.lit(0)))
        .withColumn(
            "has_late_arriving_trade_flag", F.coalesce(F.col("has_late_arriving_trade_flag"), F.lit(0))
        )
        .withColumn("has_missing_price_flag", F.coalesce(F.col("has_missing_price_flag"), F.lit(0)))
    )

    # Position difference and status.
    base_df = (
        base_df.withColumn(
            "position_difference",
            F.when(F.col("reported_position").isNull(), F.lit(None).cast("double")).otherwise(
                F.col("reported_position") - F.col("expected_position")
            ),
        )
        .withColumn(
            "reconciliation_status",
            F.when(F.col("reported_position").isNull(), F.lit("BREAK"))
            .when(F.col("position_difference") == F.lit(0.0), F.lit("MATCH"))
            .otherwise(F.lit("BREAK")),
        )
    )

    # Reason priority requested by spec.
    diff_non_zero = F.col("position_difference").isNotNull() & (F.col("position_difference") != F.lit(0.0))
    base_df = base_df.withColumn(
        "break_reason_code",
        F.when(F.col("reported_position").isNull(), F.lit("POSITION_NOT_REPORTED"))
        .when(
            diff_non_zero & (F.col("has_split_adjustment_break_flag") == F.lit(1)),
            F.lit("SPLIT_ADJUSTMENT_BREAK"),
        )
        .when(diff_non_zero & (F.col("has_duplicate_trade_flag") == F.lit(1)), F.lit("DUPLICATE_TRADE"))
        .when(
            diff_non_zero & (F.col("has_late_arriving_trade_flag") == F.lit(1)),
            F.lit("LATE_ARRIVING_TRADE"),
        )
        .when(diff_non_zero & (F.col("has_missing_price_flag") == F.lit(1)), F.lit("MISSING_PRICE"))
        .when(diff_non_zero, F.lit("QUANTITY_MISMATCH"))
        .otherwise(F.lit("MATCH")),
    )

    # ------------------------------------------------------------------
    # Cascade / root-cause enrichment
    # ------------------------------------------------------------------
    pair_window = Window.partitionBy("account_id", "security_id").orderBy("position_date")
    pair_to_current = pair_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Start a new break segment whenever BREAK appears after a non-BREAK day.
    base_df = (
        base_df.withColumn("is_break", F.when(F.col("reconciliation_status") == "BREAK", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("prev_is_break", F.lag("is_break").over(pair_window))
        .withColumn(
            "break_segment_start",
            F.when(
                (F.col("is_break") == 1)
                & (F.coalesce(F.col("prev_is_break"), F.lit(0)) == 0),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("break_segment_id", F.sum("break_segment_start").over(pair_to_current))
    )

    segment_window = Window.partitionBy("account_id", "security_id", "break_segment_id")
    segment_ordered = segment_window.orderBy("position_date")

    # First break date and first day reason within each contiguous break segment.
    base_df = (
        base_df.withColumn(
            "first_break_date",
            F.when(
                F.col("is_break") == 1,
                F.min("position_date").over(segment_window),
            ),
        )
        .withColumn(
            "segment_first_reason",
            F.when(
                F.col("is_break") == 1,
                F.first("break_reason_code", ignorenulls=True).over(segment_ordered),
            ),
        )
        .withColumn(
            "root_cause_reason_code",
            F.when(F.col("is_break") == 0, F.lit("MATCH"))
            .when(
                F.col("segment_first_reason").isin(
                    "SPLIT_ADJUSTMENT_BREAK",
                    "DUPLICATE_TRADE",
                    "LATE_ARRIVING_TRADE",
                    "MISSING_PRICE",
                    "POSITION_NOT_REPORTED",
                ),
                F.col("segment_first_reason"),
            )
            .otherwise(F.lit("QUANTITY_MISMATCH")),
        )
        .withColumn(
            "days_since_first_break",
            F.when(
                F.col("is_break") == 1,
                F.datediff(F.col("position_date"), F.col("first_break_date")),
            ),
        )
        .withColumn(
            "is_cascading_break",
            F.when(
                (F.col("is_break") == 1) & (F.col("position_date") > F.col("first_break_date")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
    )

    return base_df.select(
        "account_id",
        "security_id",
        "position_date",
        "starting_quantity",
        "cumulative_buy_quantity",
        "cumulative_sell_quantity",
        "expected_position",
        "reported_position",
        "position_difference",
        "reconciliation_status",
        "break_reason_code",
        "root_cause_reason_code",
        "first_break_date",
        "days_since_first_break",
        "is_cascading_break",
    )
