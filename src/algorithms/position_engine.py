"""Pure-Python reference implementation of position reconstruction."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass
class Trade:
    account_id: str
    security_id: str
    trade_date: str
    side: str
    quantity: float


@dataclass
class StartingPosition:
    account_id: str
    security_id: str
    position_date: str
    quantity: float


@dataclass
class ReportedPosition:
    account_id: str
    security_id: str
    position_date: str
    reported_quantity: float


@dataclass
class QualityFlag:
    account_id: str
    security_id: str
    flag_date: str
    flag_type: str


def _is_buy(side: str) -> bool:
    return side.upper() in {"BUY", "B"}


def _is_sell(side: str) -> bool:
    return side.upper() in {"SELL", "S"}


def _classify_break_reason(
    has_reported: bool,
    position_difference: float | None,
    flags: dict[str, bool],
) -> str:
    if not has_reported:
        return "POSITION_NOT_REPORTED"
    if position_difference is None or abs(position_difference) <= 1e-12:
        return "MATCH"
    if flags.get("SPLIT_ADJUSTMENT_BREAK"):
        return "SPLIT_ADJUSTMENT_BREAK"
    if flags.get("DUPLICATE_TRADE"):
        return "DUPLICATE_TRADE"
    if flags.get("LATE_ARRIVING_TRADE"):
        return "LATE_ARRIVING_TRADE"
    if flags.get("MISSING_PRICE"):
        return "MISSING_PRICE"
    return "QUANTITY_MISMATCH"


def reconstruct_positions(
    *,
    trades: list[Trade],
    starting_positions: list[StartingPosition],
    reported_positions: list[ReportedPosition],
    quality_flags: list[QualityFlag],
    valid_security_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Reconstruct expected positions and classify breaks (Python reference)."""
    pairs: set[tuple[str, str]] = set()
    all_dates: set[str] = set()

    for row in starting_positions:
        pairs.add((row.account_id, row.security_id))
        all_dates.add(row.position_date)
    for row in trades:
        pairs.add((row.account_id, row.security_id))
        all_dates.add(row.trade_date)
    for row in reported_positions:
        pairs.add((row.account_id, row.security_id))
        all_dates.add(row.position_date)

    starting_quantity_by_pair: dict[tuple[str, str], float] = defaultdict(float)
    for row in starting_positions:
        key = (row.account_id, row.security_id)
        starting_quantity_by_pair[key] = max(starting_quantity_by_pair[key], row.quantity)

    daily_activity: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {"buy_quantity": 0.0, "sell_quantity": 0.0}
    )
    for trade in trades:
        if valid_security_ids and trade.security_id not in valid_security_ids:
            continue
        key = (trade.account_id, trade.security_id, trade.trade_date)
        if _is_buy(trade.side):
            daily_activity[key]["buy_quantity"] += trade.quantity
        elif _is_sell(trade.side):
            daily_activity[key]["sell_quantity"] += trade.quantity

    reported_by_key = {
        (row.account_id, row.security_id, row.position_date): row.reported_quantity
        for row in reported_positions
    }

    flags_by_key: dict[tuple[str, str, str], dict[str, bool]] = defaultdict(dict)
    for flag in quality_flags:
        flags_by_key[(flag.account_id, flag.security_id, flag.flag_date)][flag.flag_type] = True

    output: list[dict[str, Any]] = []
    sorted_dates = sorted(all_dates)

    for account_id, security_id in sorted(pairs):
        cumulative_buy = 0.0
        cumulative_sell = 0.0
        starting_quantity = starting_quantity_by_pair[(account_id, security_id)]

        for position_date in sorted_dates:
            activity_key = (account_id, security_id, position_date)
            cumulative_buy += daily_activity[activity_key]["buy_quantity"]
            cumulative_sell += daily_activity[activity_key]["sell_quantity"]
            expected_position = starting_quantity + cumulative_buy - cumulative_sell

            has_reported = activity_key in reported_by_key
            reported_position = reported_by_key.get(activity_key)
            position_difference = (
                None if not has_reported else reported_position - expected_position
            )
            flag_map = flags_by_key.get(activity_key, {})
            break_reason_code = _classify_break_reason(has_reported, position_difference, flag_map)

            if not has_reported:
                reconciliation_status = "BREAK"
            elif position_difference is not None and abs(position_difference) > 1e-12:
                reconciliation_status = "BREAK"
            else:
                reconciliation_status = "MATCH"

            output.append(
                {
                    "account_id": account_id,
                    "security_id": security_id,
                    "position_date": position_date,
                    "starting_quantity": starting_quantity,
                    "cumulative_buy_quantity": cumulative_buy,
                    "cumulative_sell_quantity": cumulative_sell,
                    "expected_position": expected_position,
                    "reported_position": reported_position,
                    "position_difference": position_difference,
                    "reconciliation_status": reconciliation_status,
                    "break_reason_code": break_reason_code,
                }
            )

    return output
