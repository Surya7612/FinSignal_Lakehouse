"""Load silver-layer inputs and run Python/C++ position engine parity checks."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from src.algorithms.position_engine import (
    QualityFlag,
    ReportedPosition,
    StartingPosition,
    Trade,
    reconstruct_positions,
)
from src.utils.io import PROJECT_ROOT, create_spark_session, read_table


def _resolve_path(relative_path: str, project_root) -> Any:
    from pathlib import Path

    return (project_root / relative_path).resolve()


def _date_str(value: Any) -> str:
    return str(value)[:10]


def load_engine_inputs(*, project_root=PROJECT_ROOT, spark: SparkSession | None = None) -> dict[str, Any]:
    owns_spark = spark is None
    session = spark or create_spark_session("FinSignal Engine Loader")

    try:
        securities_df = read_table(session, _resolve_path("data/silver/securities_clean", project_root))
        starting_df = read_table(
            session, _resolve_path("data/silver/starting_positions_clean", project_root)
        )
        trades_df = read_table(session, _resolve_path("data/silver/trades_clean", project_root))
        reported_df = read_table(
            session, _resolve_path("data/silver/reported_positions_clean", project_root)
        )
        flags_df = read_table(session, _resolve_path("data/silver/trade_quality_flags", project_root))

        valid_security_ids = {row.security_id for row in securities_df.select("security_id").collect()}
        trade_qty_col = "adjusted_quantity" if "adjusted_quantity" in trades_df.columns else "quantity"
        starting_qty_col = "adjusted_quantity" if "adjusted_quantity" in starting_df.columns else "quantity"
        reported_qty_col = (
            "adjusted_reported_quantity"
            if "adjusted_reported_quantity" in reported_df.columns
            else "reported_quantity"
        )

        trades = [
            Trade(
                account_id=row.account_id,
                security_id=row.security_id,
                trade_date=_date_str(row.trade_date),
                side=row.side,
                quantity=float(row[trade_qty_col]),
            )
            for row in trades_df.select("account_id", "security_id", "trade_date", "side", trade_qty_col).collect()
        ]
        starting_positions = [
            StartingPosition(
                account_id=row.account_id,
                security_id=row.security_id,
                position_date=_date_str(row.position_date),
                quantity=float(row[starting_qty_col]),
            )
            for row in starting_df.select(
                "account_id", "security_id", "position_date", starting_qty_col
            ).collect()
        ]
        reported_positions = [
            ReportedPosition(
                account_id=row.account_id,
                security_id=row.security_id,
                position_date=_date_str(row.position_date),
                reported_quantity=float(row[reported_qty_col]),
            )
            for row in reported_df.select(
                "account_id", "security_id", "position_date", reported_qty_col
            ).collect()
        ]
        quality_flags = [
            QualityFlag(
                account_id=row.account_id,
                security_id=row.security_id,
                flag_date=_date_str(row.flag_date),
                flag_type=row.flag_type,
            )
            for row in flags_df.select("account_id", "security_id", "flag_date", "flag_type").collect()
        ]
        return {
            "valid_security_ids": valid_security_ids,
            "trades": trades,
            "starting_positions": starting_positions,
            "reported_positions": reported_positions,
            "quality_flags": quality_flags,
        }
    finally:
        if owns_spark:
            session.stop()


def run_python_engine(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    return reconstruct_positions(
        trades=inputs["trades"],
        starting_positions=inputs["starting_positions"],
        reported_positions=inputs["reported_positions"],
        quality_flags=inputs["quality_flags"],
        valid_security_ids=inputs["valid_security_ids"],
    )


def cpp_rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        output.append(
            {
                "account_id": row.account_id,
                "security_id": row.security_id,
                "position_date": row.position_date,
                "expected_position": row.expected_position,
                "reported_position": row.reported_position,
                "reconciliation_status": row.reconciliation_status,
                "break_reason_code": row.break_reason_code,
            }
        )
    return output


def build_cpp_engine(inputs: dict[str, Any]) -> Any:
    import finsignal_engine as fe

    engine = fe.EventEngine()
    engine.set_valid_securities(sorted(inputs["valid_security_ids"]))

    for trade in inputs["trades"]:
        cpp_trade = fe.Trade()
        cpp_trade.account_id = trade.account_id
        cpp_trade.security_id = trade.security_id
        cpp_trade.trade_date = trade.trade_date
        cpp_trade.side = trade.side
        cpp_trade.quantity = trade.quantity
        engine.add_trade(cpp_trade)

    for position in inputs["starting_positions"]:
        cpp_position = fe.StartingPosition()
        cpp_position.account_id = position.account_id
        cpp_position.security_id = position.security_id
        cpp_position.position_date = position.position_date
        cpp_position.quantity = position.quantity
        engine.add_starting_position(cpp_position)

    for position in inputs["reported_positions"]:
        cpp_position = fe.ReportedPosition()
        cpp_position.account_id = position.account_id
        cpp_position.security_id = position.security_id
        cpp_position.position_date = position.position_date
        cpp_position.reported_quantity = position.reported_quantity
        engine.add_reported_position(cpp_position)

    for flag in inputs["quality_flags"]:
        cpp_flag = fe.QualityFlag()
        cpp_flag.account_id = flag.account_id
        cpp_flag.security_id = flag.security_id
        cpp_flag.flag_date = flag.flag_date
        cpp_flag.flag_type = flag.flag_type
        engine.add_quality_flag(cpp_flag)

    return engine


def run_cpp_engine(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    engine = build_cpp_engine(inputs)
    return cpp_rows_to_dicts(engine.reconstruct())


def normalize_engine_rows(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    normalized = []
    for row in rows:
        normalized.append(
            (
                row["account_id"],
                row["security_id"],
                row["position_date"],
                round(float(row["expected_position"]), 6),
                None if row["reported_position"] is None else round(float(row["reported_position"]), 6),
                row["reconciliation_status"],
                row["break_reason_code"],
            )
        )
    return sorted(normalized)
