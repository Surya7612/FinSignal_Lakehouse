"""Bounded event-window analytics for the gold layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_event_window_metrics(
    *,
    prices_df: DataFrame,
    events_df: DataFrame,
    position_reconstruction_df: DataFrame,
) -> DataFrame:
    """
    Build one event-window metrics row per event_id/security_id.

    Uses security-local trading day indexes and bounded joins only.
    """
    # 1) Price index per security.
    security_date_window = Window.partitionBy("security_id").orderBy("price_date")
    price_indexed = (
        prices_df.select("security_id", "price_date", "close_price")
        .withColumn("trading_day_index", F.row_number().over(security_date_window))
        .withColumn("prev_close_price", F.lag("close_price").over(security_date_window))
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close_price").isNotNull() & (F.col("prev_close_price") != 0),
                (F.col("close_price") - F.col("prev_close_price")) / F.col("prev_close_price"),
            ),
        )
    )

    # 2) Map each event to nearest later (or exact) trading day for same security.
    event_map_candidates = events_df.alias("e").join(
        price_indexed.select("security_id", "price_date", "trading_day_index").alias("p"),
        on=(F.col("e.security_id") == F.col("p.security_id"))
        & (F.col("p.price_date") >= F.col("e.event_date")),
        how="left",
    )
    event_rank_window = Window.partitionBy("e.event_id", "e.security_id").orderBy(F.col("p.price_date").asc())
    event_mapped = (
        event_map_candidates.withColumn("event_price_rank", F.row_number().over(event_rank_window))
        .filter(F.col("event_price_rank") == 1)
        .select(
            F.col("e.event_id").alias("event_id"),
            F.col("e.security_id").alias("security_id"),
            F.col("e.event_date").alias("event_date"),
            F.col("e.event_type").alias("event_type"),
            F.col("p.price_date").alias("event_price_date"),
            F.col("p.trading_day_index").alias("event_trading_day_index"),
        )
    )

    # 3) Bounded join for +/- 5 trading-day window.
    window_prices = event_mapped.alias("ev").join(
        price_indexed.alias("px"),
        on=(F.col("ev.security_id") == F.col("px.security_id"))
        & (
            F.col("px.trading_day_index").between(
                F.col("ev.event_trading_day_index") - F.lit(5),
                F.col("ev.event_trading_day_index") + F.lit(5),
            )
        ),
        how="left",
    ).select(
        F.col("ev.event_id"),
        F.col("ev.security_id"),
        F.col("ev.event_date"),
        F.col("ev.event_type"),
        F.col("ev.event_trading_day_index"),
        F.col("px.price_date"),
        F.col("px.trading_day_index"),
        F.col("px.close_price"),
        F.col("px.daily_return"),
        (F.col("px.trading_day_index") - F.col("ev.event_trading_day_index")).alias("offset"),
    )

    # 4) Price-window return/volatility metrics.
    price_metrics = window_prices.groupBy(
        "event_id",
        "security_id",
        "event_date",
        "event_type",
        "event_trading_day_index",
    ).agg(
        F.max(F.when(F.col("offset") == -5, F.col("close_price"))).alias("close_m5"),
        F.max(F.when(F.col("offset") == -1, F.col("close_price"))).alias("close_m1"),
        F.max(F.when(F.col("offset") == 0, F.col("close_price"))).alias("close_0"),
        F.max(F.when(F.col("offset") == 1, F.col("close_price"))).alias("close_p1"),
        F.max(F.when(F.col("offset") == 5, F.col("close_price"))).alias("close_p5"),
        F.stddev_samp(F.when(F.col("offset").between(-5, -1), F.col("daily_return"))).alias(
            "pre_event_volatility"
        ),
        F.stddev_samp(F.when(F.col("offset").between(1, 5), F.col("daily_return"))).alias(
            "post_event_volatility"
        ),
        F.count(F.when(F.col("offset").between(-5, -1) & F.col("daily_return").isNotNull(), F.lit(1))).alias(
            "pre_return_obs"
        ),
        F.count(F.when(F.col("offset").between(1, 5) & F.col("daily_return").isNotNull(), F.lit(1))).alias(
            "post_return_obs"
        ),
    )

    price_metrics = (
        price_metrics.withColumn(
            "pre_event_return",
            F.when(
                F.col("close_m5").isNotNull() & (F.col("close_m5") != 0) & F.col("close_m1").isNotNull(),
                (F.col("close_m1") - F.col("close_m5")) / F.col("close_m5"),
            ),
        )
        .withColumn(
            "post_event_return",
            F.when(
                F.col("close_p1").isNotNull() & (F.col("close_p1") != 0) & F.col("close_p5").isNotNull(),
                (F.col("close_p5") - F.col("close_p1")) / F.col("close_p1"),
            ),
        )
        .withColumn(
            "event_day_return",
            F.when(
                F.col("close_m1").isNotNull() & (F.col("close_m1") != 0) & F.col("close_0").isNotNull(),
                (F.col("close_0") - F.col("close_m1")) / F.col("close_m1"),
            ),
        )
        .withColumn(
            "is_full_window",
            (F.col("close_m5").isNotNull())
            & (F.col("close_m1").isNotNull())
            & (F.col("close_p1").isNotNull())
            & (F.col("close_p5").isNotNull())
            & (F.col("pre_return_obs") >= F.lit(5))
            & (F.col("post_return_obs") >= F.lit(5)),
        )
    )

    # 5) Position + exposure metrics (aggregate expected positions by security/date).
    position_by_security_date = (
        position_reconstruction_df.groupBy("security_id", F.col("position_date").alias("price_date"))
        .agg(F.sum("expected_position").alias("position_quantity"))
        .join(
            price_indexed.select("security_id", "price_date", "trading_day_index", "close_price"),
            on=["security_id", "price_date"],
            how="left",
        )
    )

    before_candidates = event_mapped.alias("ev").join(
        position_by_security_date.alias("ps"),
        on=(F.col("ev.security_id") == F.col("ps.security_id"))
        & (F.col("ps.trading_day_index") < F.col("ev.event_trading_day_index")),
        how="left",
    )
    before_rank = Window.partitionBy("ev.event_id", "ev.security_id").orderBy(F.col("ps.trading_day_index").desc())
    position_before = (
        before_candidates.withColumn("rn", F.row_number().over(before_rank))
        .filter(F.col("rn") == 1)
        .select(
            F.col("ev.event_id").alias("event_id"),
            F.col("ev.security_id").alias("security_id"),
            F.col("ps.position_quantity").alias("position_before_event"),
            F.col("ps.close_price").alias("close_price_before"),
        )
    )

    after_candidates = event_mapped.alias("ev").join(
        position_by_security_date.alias("ps"),
        on=(F.col("ev.security_id") == F.col("ps.security_id"))
        & (F.col("ps.trading_day_index") > F.col("ev.event_trading_day_index"))
        & (F.col("ps.trading_day_index") <= F.col("ev.event_trading_day_index") + F.lit(5)),
        how="left",
    )
    after_rank = Window.partitionBy("ev.event_id", "ev.security_id").orderBy(F.col("ps.trading_day_index").desc())
    position_after = (
        after_candidates.withColumn("rn", F.row_number().over(after_rank))
        .filter(F.col("rn") == 1)
        .select(
            F.col("ev.event_id").alias("event_id"),
            F.col("ev.security_id").alias("security_id"),
            F.col("ps.position_quantity").alias("position_after_event"),
            F.col("ps.close_price").alias("close_price_after"),
        )
    )

    metrics = (
        price_metrics.join(position_before, on=["event_id", "security_id"], how="left")
        .join(position_after, on=["event_id", "security_id"], how="left")
        .withColumn(
            "position_change_around_event",
            F.when(
                F.col("position_before_event").isNotNull() & F.col("position_after_event").isNotNull(),
                F.col("position_after_event") - F.col("position_before_event"),
            ),
        )
        .withColumn(
            "exposure_before_event",
            F.when(
                F.col("position_before_event").isNotNull() & F.col("close_price_before").isNotNull(),
                F.col("position_before_event") * F.col("close_price_before"),
            ),
        )
        .withColumn(
            "exposure_after_event",
            F.when(
                F.col("position_after_event").isNotNull() & F.col("close_price_after").isNotNull(),
                F.col("position_after_event") * F.col("close_price_after"),
            ),
        )
        .withColumn(
            "exposure_change_around_event",
            F.when(
                F.col("exposure_before_event").isNotNull() & F.col("exposure_after_event").isNotNull(),
                F.col("exposure_after_event") - F.col("exposure_before_event"),
            ),
        )
    )

    return metrics.select(
        "event_id",
        "security_id",
        "event_date",
        "event_type",
        "event_trading_day_index",
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
        "is_full_window",
    )
