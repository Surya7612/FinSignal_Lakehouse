"""
Build deterministic investigation corpus documents for later embedding.

This step is data prep only:
- no LLM calls
- no embedding generation
- no orchestration/UI/streaming components
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.io import PROJECT_ROOT, create_spark_session


@dataclass
class InvestigationCorpusResult:
    corpus_count: int
    counts_by_document_type: list[tuple[str, int]]
    output_path: Path


def _resolve_path(relative_path: str, project_root: Path) -> Path:
    return (project_root / relative_path).resolve()


def _load_events_clean(spark: SparkSession, project_root: Path) -> DataFrame:
    primary = _resolve_path("data/silver/events_clean", project_root)
    fallback = _resolve_path("data/silver/filing_events_clean", project_root)
    if primary.exists():
        return spark.read.parquet(str(primary))
    if fallback.exists():
        return spark.read.parquet(str(fallback))
    raise FileNotFoundError("Neither data/silver/events_clean nor data/silver/filing_events_clean exists.")


def _reconciliation_break_docs(reconciliation_breaks_df: DataFrame) -> DataFrame:
    return reconciliation_breaks_df.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("RECONCILIATION_BREAK_SUMMARY"),
                F.col("account_id"),
                F.col("security_id"),
                F.col("position_date").cast("string"),
                F.col("break_reason_code"),
            ),
            256,
        ).alias("document_id"),
        F.lit("RECONCILIATION_BREAK_SUMMARY").alias("document_type"),
        F.col("account_id"),
        F.col("security_id"),
        F.col("position_date"),
        F.lit(None).cast("date").alias("event_date"),
        F.concat_ws(
            " ",
            F.lit("Reconciliation break"),
            F.col("account_id"),
            F.lit("/"),
            F.col("security_id"),
            F.lit("on"),
            F.col("position_date").cast("string"),
        ).alias("title"),
        F.format_string(
            (
                "Reconciliation break for %s / %s on %s. Expected position was %.4f, "
                "reported position was %.4f, difference was %.4f. Break reason was %s. "
                "Root cause reason was %s."
            ),
            F.col("account_id"),
            F.col("security_id"),
            F.col("position_date").cast("string"),
            F.col("expected_position"),
            F.col("reported_position"),
            F.col("position_difference"),
            F.col("break_reason_code"),
            F.col("root_cause_reason_code"),
        ).alias("text"),
        F.array(F.lit("gold.reconciliation_breaks")).alias("source_tables"),
        F.create_map(
            F.lit("break_id"),
            F.col("break_id").cast("string"),
            F.lit("break_reason_code"),
            F.col("break_reason_code").cast("string"),
            F.lit("root_cause_reason_code"),
            F.col("root_cause_reason_code").cast("string"),
            F.lit("severity"),
            F.col("severity").cast("string"),
            F.lit("first_break_date"),
            F.col("first_break_date").cast("string"),
            F.lit("days_since_first_break"),
            F.col("days_since_first_break").cast("string"),
            F.lit("is_cascading_break"),
            F.col("is_cascading_break").cast("string"),
        ).alias("metadata"),
    )


def _event_window_docs(event_window_df: DataFrame, events_clean_df: DataFrame) -> DataFrame:
    events_lookup = events_clean_df.select(
        "event_id",
        F.col("event_type").alias("lookup_event_type"),
        "event_summary",
    ).dropDuplicates(["event_id"])

    joined = event_window_df.join(events_lookup, on="event_id", how="left").withColumn(
        "window_completeness",
        F.when(F.col("is_full_window") == F.lit(True), F.lit("FULL")).otherwise(F.lit("PARTIAL")),
    )
    return joined.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("EVENT_WINDOW_SUMMARY"),
                F.col("event_id"),
                F.col("security_id"),
                F.col("event_date").cast("string"),
            ),
            256,
        ).alias("document_id"),
        F.lit("EVENT_WINDOW_SUMMARY").alias("document_type"),
        F.lit(None).cast("string").alias("account_id"),
        F.col("security_id"),
        F.lit(None).cast("date").alias("position_date"),
        F.col("event_date"),
        F.concat_ws(
            " ",
            F.lit("Event window"),
            F.col("event_id"),
            F.lit("for"),
            F.col("security_id"),
            F.lit("on"),
            F.col("event_date").cast("string"),
        ).alias("title"),
        F.format_string(
            (
                "Event-window metrics for %s (%s) for %s on %s. Pre-event return was %.6f, "
                "post-event return was %.6f, event-day return was %.6f. Position change around "
                "event was %.4f, exposure change was %.4f. Window completeness status was %s."
            ),
            F.col("event_id"),
            F.coalesce(F.col("lookup_event_type"), F.col("event_type"), F.lit("UNKNOWN_EVENT_TYPE")),
            F.col("security_id"),
            F.col("event_date").cast("string"),
            F.col("pre_event_return"),
            F.col("post_event_return"),
            F.col("event_day_return"),
            F.col("position_change_around_event"),
            F.col("exposure_change_around_event"),
            F.col("window_completeness"),
        ).alias("text"),
        F.array(F.lit("gold.event_window_metrics"), F.lit("silver.events_clean")).alias("source_tables"),
        F.create_map(
            F.lit("event_id"),
            F.col("event_id").cast("string"),
            F.lit("event_type"),
            F.coalesce(F.col("lookup_event_type"), F.col("event_type"), F.lit("UNKNOWN_EVENT_TYPE")).cast("string"),
            F.lit("window_completeness"),
            F.col("window_completeness").cast("string"),
            F.lit("event_summary"),
            F.col("event_summary").cast("string"),
            F.lit("pre_event_return"),
            F.col("pre_event_return").cast("string"),
            F.lit("post_event_return"),
            F.col("post_event_return").cast("string"),
            F.lit("event_day_return"),
            F.col("event_day_return").cast("string"),
            F.lit("position_change_around_event"),
            F.col("position_change_around_event").cast("string"),
            F.lit("exposure_change_around_event"),
            F.col("exposure_change_around_event").cast("string"),
        ).alias("metadata"),
    )


def _trade_quality_docs(flags_df: DataFrame, trades_df: DataFrame) -> DataFrame:
    grouped_flags = flags_df.groupBy("account_id", "security_id", F.col("flag_date").alias("position_date")).agg(
        F.collect_set("flag_type").alias("flag_types"),
        F.count("*").alias("flag_count"),
        F.concat_ws(" | ", F.sort_array(F.collect_set("description"))).alias("flag_descriptions"),
    )

    daily_trades = trades_df.groupBy("account_id", "security_id", F.col("trade_date").alias("position_date")).agg(
        F.count("*").alias("trade_count"),
        F.sum(F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(F.lit(0.0))).alias("buy_quantity"),
        F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(F.lit(0.0))).alias("sell_quantity"),
    )

    joined = (
        grouped_flags.join(daily_trades, on=["account_id", "security_id", "position_date"], how="left")
        .withColumn("trade_count", F.coalesce(F.col("trade_count"), F.lit(0)))
        .withColumn("buy_quantity", F.coalesce(F.col("buy_quantity"), F.lit(0.0)))
        .withColumn("sell_quantity", F.coalesce(F.col("sell_quantity"), F.lit(0.0)))
    )

    return joined.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("TRADE_QUALITY_SUMMARY"),
                F.col("account_id"),
                F.col("security_id"),
                F.col("position_date").cast("string"),
            ),
            256,
        ).alias("document_id"),
        F.lit("TRADE_QUALITY_SUMMARY").alias("document_type"),
        F.col("account_id"),
        F.col("security_id"),
        F.col("position_date"),
        F.lit(None).cast("date").alias("event_date"),
        F.concat_ws(
            " ",
            F.lit("Trade quality flags"),
            F.col("account_id"),
            F.lit("/"),
            F.col("security_id"),
            F.lit("on"),
            F.col("position_date").cast("string"),
        ).alias("title"),
        F.format_string(
            (
                "Trade quality summary for %s / %s on %s. Detected %d quality flags with types [%s]. "
                "Trade count was %d, buy quantity was %.4f, sell quantity was %.4f. Flag details: %s."
            ),
            F.col("account_id"),
            F.col("security_id"),
            F.col("position_date").cast("string"),
            F.col("flag_count"),
            F.concat_ws(", ", F.sort_array(F.col("flag_types"))),
            F.col("trade_count"),
            F.col("buy_quantity"),
            F.col("sell_quantity"),
            F.col("flag_descriptions"),
        ).alias("text"),
        F.array(F.lit("silver.trade_quality_flags"), F.lit("silver.trades_clean")).alias("source_tables"),
        F.create_map(
            F.lit("flag_types"),
            F.concat_ws(", ", F.sort_array(F.col("flag_types"))),
            F.lit("flag_count"),
            F.col("flag_count").cast("string"),
            F.lit("trade_count"),
            F.col("trade_count").cast("string"),
            F.lit("buy_quantity"),
            F.col("buy_quantity").cast("string"),
            F.lit("sell_quantity"),
            F.col("sell_quantity").cast("string"),
        ).alias("metadata"),
    )


def run_build_investigation_corpus(
    run_id: str,
    *,
    project_root: Path = PROJECT_ROOT,
    spark: SparkSession | None = None,
) -> InvestigationCorpusResult:
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Investigation Corpus")
    output_path = _resolve_path(
        "data/retrieval/investigation_corpus/investigation_corpus.jsonl",
        project_root,
    )

    try:
        reconciliation_breaks_df = session.read.parquet(
            str(_resolve_path("data/gold/reconciliation_breaks", project_root))
        )
        event_window_df = session.read.parquet(str(_resolve_path("data/gold/event_window_metrics", project_root)))
        events_clean_df = _load_events_clean(session, project_root)
        trades_df = session.read.parquet(str(_resolve_path("data/silver/trades_clean", project_root)))
        flags_df = session.read.parquet(str(_resolve_path("data/silver/trade_quality_flags", project_root)))

        recon_docs = _reconciliation_break_docs(reconciliation_breaks_df)
        event_docs = _event_window_docs(event_window_df, events_clean_df)
        quality_docs = _trade_quality_docs(flags_df, trades_df)

        corpus_df = recon_docs.unionByName(event_docs).unionByName(quality_docs)
        corpus_count = corpus_df.count()
        counts_by_type = [
            (row["document_type"], row["count"])
            for row in corpus_df.groupBy("document_type").count().orderBy("document_type").collect()
        ]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            for line in corpus_df.orderBy("document_type", "security_id", "account_id", "position_date", "event_date").toJSON().collect():
                f.write(line)
                f.write("\n")

        _ = run_id
        return InvestigationCorpusResult(
            corpus_count=corpus_count,
            counts_by_document_type=counts_by_type,
            output_path=output_path,
        )
    finally:
        if owns_spark:
            session.stop()


def _print_summary(result: InvestigationCorpusResult, run_id: str) -> None:
    print("FinSignal Lakehouse — Investigation Corpus Build Complete")
    print(f"corpus_run_id: {run_id}")
    print("-" * 72)
    print(f"corpus row count: {result.corpus_count}")
    print("counts by document_type:")
    if not result.counts_by_document_type:
        print("  none")
    for document_type, count in result.counts_by_document_type:
        print(f"  {document_type}: {count}")
    print(f"output path: {result.output_path}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build deterministic investigation corpus JSONL from gold/silver data.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Corpus build run identifier (default: generated UUID).",
    )
    args = parser.parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    result = run_build_investigation_corpus(run_id=run_id)
    _print_summary(result, run_id)


if __name__ == "__main__":
    main()
