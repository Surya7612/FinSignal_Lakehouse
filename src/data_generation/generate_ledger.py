"""Controlled synthetic ledger generator for FinSignal Lakehouse Milestone 1."""

from __future__ import annotations

import argparse
import json
import random
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.data_generation import config
from src.data_generation.schemas import (
    FilingEventRecord,
    InjectedIssueRecord,
    PriceRecord,
    ReportedPositionRecord,
    SecurityRecord,
    StartingPositionRecord,
    TradeRecord,
)

__all__ = ["LedgerGenerator", "main"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iso_date(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%d")


def _iso_timestamp(dt: pd.Timestamp) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def _business_days(start: str, count: int) -> list[pd.Timestamp]:
    """Return `count` consecutive business days beginning at `start`."""
    start_ts = pd.Timestamp(start)
    return list(pd.bdate_range(start=start_ts, periods=count))


@dataclass
class GenerationSummary:
    """Counts and paths produced by a ledger generation run."""

    securities: int = 0
    prices: int = 0
    starting_positions: int = 0
    trades: int = 0
    reported_positions: int = 0
    filing_events: int = 0
    injected_issues: int = 0
    clean_trade_count_before_injection: int = 0
    raw_trade_count_after_injection: int = 0
    duplicate_trade_count_added: int = 0
    missing_trade_count_removed: int = 0
    invalid_trade_count_added: int = 0
    injected_issue_count: int = 0
    validation_passed: bool = True
    validation_messages: list[str] = field(default_factory=list)
    output_paths: dict[str, Path] = field(default_factory=dict)


class LedgerGenerator:
  """
  Build a controlled synthetic financial ledger.

  Generation flow
  ---------------
  1. Generate clean reference data (securities, prices, starting positions).
  2. Generate clean trades over the 30-day reconciliation window.
  3. Compute true expected positions from starting positions + trades.
  4. Copy expected positions into reported positions (clean baseline).
  5. Generate filing events for event-window analytics later.
  6. Inject known data-quality issues into raw outputs.
  7. Persist JSONL datasets and the injected-issues manifest.
  """

  def __init__(self, seed: int = config.DEFAULT_SEED) -> None:
    self.seed = seed
    self.rng = random.Random(seed)

    self.trading_days: list[pd.Timestamp] = _business_days(
      config.WINDOW_START_DATE, config.NUM_TRADING_DAYS
    )
    self.day_zero = self.trading_days[0]

    # Clean ledger artifacts (pre-injection).
    self.securities: list[SecurityRecord] = []
    self.prices: list[PriceRecord] = []
    self.starting_positions: list[StartingPositionRecord] = []
    self.clean_trades: list[TradeRecord] = []
    self.reported_positions: list[ReportedPositionRecord] = []
    self.filing_events: list[FilingEventRecord] = []

    # Post-injection raw outputs.
    self.raw_trades: list[TradeRecord] = []
    self.raw_prices: list[PriceRecord] = []
    self.injected_issues: list[InjectedIssueRecord] = []

    self._issue_counter = 0
    self._price_lookup: dict[tuple[str, str], float] = {}
    self._trade_date_expected: pd.DataFrame = pd.DataFrame()
    self._settlement_date_expected: pd.DataFrame = pd.DataFrame()

    # Injection bookkeeping (populated during inject_issues).
    self._manifest_keys: set[tuple[str, str, str | None]] = set()
    self._late_arriving_trade_ids: set[str] = set()
    self._duplicated_trade_ids: set[str] = set()
    self._removed_price_keys: set[tuple[str, str]] = set()
    self._clean_trade_count_before_injection: int = 0
    self._duplicate_trade_count_added: int = 0
    self._missing_trade_count_removed: int = 0
    self._invalid_trade_count_added: int = 0

  # -------------------------------------------------------------------------
  # Step 1 — Clean reference data
  # -------------------------------------------------------------------------

  def generate_securities(self) -> None:
    """Create the securities reference table from static definitions."""
    self.securities = [
      SecurityRecord(**definition)  # type: ignore[arg-type]
      for definition in config.SECURITY_DEFINITIONS
    ]

  def generate_prices(self) -> None:
    """Generate daily OHLCV prices for every security across the window."""
    base_prices = {
      "SEC001": 185.0,
      "SEC002": 410.0,
      "SEC003": 195.0,
      "SEC004": 105.0,
      "SEC005": 155.0,
    }

    records: list[PriceRecord] = []
    for security in self.securities:
      security_id = security["security_id"]
      close = base_prices[security_id]

      for trade_day in self.trading_days:
        # Small deterministic daily drift so prices are not flat.
        drift = self.rng.uniform(-0.015, 0.015)
        close = max(1.0, close * (1.0 + drift))
        open_price = close * self.rng.uniform(0.995, 1.005)
        high_price = max(open_price, close) * self.rng.uniform(1.0, 1.01)
        low_price = min(open_price, close) * self.rng.uniform(0.99, 1.0)
        volume = self.rng.randint(500_000, 5_000_000)

        price_date = _iso_date(trade_day)
        records.append(
          PriceRecord(
            security_id=security_id,
            price_date=price_date,
            open_price=round(open_price, 4),
            high_price=round(high_price, 4),
            low_price=round(low_price, 4),
            close_price=round(close, 4),
            volume=volume,
          )
        )

    self.prices = records
    self._price_lookup = {
      (row["security_id"], row["price_date"]): row["close_price"] for row in records
    }

  def generate_starting_positions(self) -> None:
    """
    Create Day 0 checkpoint baselines.

    Each account holds a non-zero quantity in a subset of securities so
    reconciliation has meaningful starting balances.
    """
    records: list[StartingPositionRecord] = []
    position_date = _iso_date(self.day_zero)

    holdings_plan = {
      "ACC001": {"SEC001": 500, "SEC002": 200, "SEC003": 300},
      "ACC002": {"SEC002": 150, "SEC004": 400, "SEC005": 250},
      "ACC003": {"SEC001": 100, "SEC003": 180, "SEC005": 320, "SEC004": 90},
    }

    for account_id, holdings in holdings_plan.items():
      for security_id, quantity in holdings.items():
        records.append(
          StartingPositionRecord(
            account_id=account_id,
            security_id=security_id,
            position_date=position_date,
            quantity=float(quantity),
          )
        )

    self.starting_positions = records

  def ensure_explicit_starting_positions(self) -> None:
    """
    Add explicit Day 0 rows (quantity 0) for every traded account/security pair.

    This removes implicit-zero ambiguity during later reconciliation.
    """
    position_date = _iso_date(self.day_zero)
    existing = {(row["account_id"], row["security_id"]) for row in self.starting_positions}
    traded_pairs = {(trade["account_id"], trade["security_id"]) for trade in self.clean_trades}

    for account_id, security_id in sorted(traded_pairs - existing):
      self.starting_positions.append(
        StartingPositionRecord(
          account_id=account_id,
          security_id=security_id,
          position_date=position_date,
          quantity=0.0,
        )
      )

    self.starting_positions.sort(key=lambda row: (row["account_id"], row["security_id"]))

  # -------------------------------------------------------------------------
  # Step 2 — Clean trades
  # -------------------------------------------------------------------------

  def generate_clean_trades(self) -> None:
    """Generate 150–300 clean trades distributed across accounts and securities."""
    target_count = self.rng.randint(config.MIN_TRADES, config.MAX_TRADES)
    security_ids = [sec["security_id"] for sec in self.securities]

    trades: list[TradeRecord] = []
    for index in range(target_count):
      trade_day = self.rng.choice(self.trading_days)
      settlement_day = trade_day + pd.tseries.offsets.BDay(
        config.SETTLEMENT_OFFSET_BUSINESS_DAYS
      )

      security_id = self.rng.choice(security_ids)
      price_date = _iso_date(trade_day)
      execution_price = self._price_lookup.get(
        (security_id, price_date),
        100.0,
      )
      # Execution price may differ slightly from the close.
      execution_price = round(execution_price * self.rng.uniform(0.998, 1.002), 4)

      event_time = trade_day + timedelta(
        hours=self.rng.randint(9, 16),
        minutes=self.rng.randint(0, 59),
        seconds=self.rng.randint(0, 59),
      )
      ingestion_time = event_time + timedelta(minutes=self.rng.randint(1, 30))

      trades.append(
        TradeRecord(
          trade_id=f"TRD{index + 1:06d}",
          account_id=self.rng.choice(config.ACCOUNT_IDS),
          security_id=security_id,
          side=self.rng.choice(["BUY", "SELL"]),
          quantity=float(self.rng.randint(1, 50)),
          execution_price=execution_price,
          trade_date=price_date,
          settlement_date=_iso_date(settlement_day),
          event_timestamp=_iso_timestamp(pd.Timestamp(event_time)),
          ingestion_timestamp=_iso_timestamp(pd.Timestamp(ingestion_time)),
          source_system=self.rng.choice(config.SOURCE_SYSTEMS),
        )
      )

    # Stable logical order before any out-of-order injection.
    trades.sort(key=lambda row: (row["trade_date"], row["event_timestamp"], row["trade_id"]))
    self.clean_trades = trades

  # -------------------------------------------------------------------------
  # Step 3 — Expected positions
  # -------------------------------------------------------------------------

  def _trades_dataframe(self, trades: list[TradeRecord]) -> pd.DataFrame:
    if not trades:
      return pd.DataFrame(
        columns=[
          "trade_id",
          "account_id",
          "security_id",
          "side",
          "quantity",
          "trade_date",
          "settlement_date",
        ]
      )
    return pd.DataFrame(trades)

  def _compute_expected_positions(
    self,
    trades: list[TradeRecord],
    *,
    date_field: str,
  ) -> pd.DataFrame:
    """
    Reconstruct expected positions using a checkpointed baseline.

    expected(position_date) = starting_quantity_at_day_0
                            + cumulative_net_trades(through position_date)

    Trades are attributed on either trade_date or settlement_date depending on
    `date_field`, which enables timing-mismatch scenarios later.
    """
    starting_map: dict[tuple[str, str], float] = {
      (row["account_id"], row["security_id"]): float(row["quantity"])
      for row in self.starting_positions
    }

    pair_keys: set[tuple[str, str]] = set(starting_map)
    for trade in trades:
      pair_keys.add((trade["account_id"], trade["security_id"]))

    records: list[dict[str, Any]] = []
    trades_df = self._trades_dataframe(trades)

    for account_id, security_id in sorted(pair_keys):
      starting_quantity = starting_map.get((account_id, security_id), 0.0)
      cumulative_net = 0.0

      if trades_df.empty:
        pair_activity = pd.DataFrame(columns=[date_field, "net_quantity"])
      else:
        pair_trades = trades_df[
          (trades_df["account_id"] == account_id)
          & (trades_df["security_id"] == security_id)
        ].copy()
        pair_trades["signed_quantity"] = pair_trades.apply(
          lambda row: row["quantity"] if row["side"] == "BUY" else -row["quantity"],
          axis=1,
        )
        pair_activity = (
          pair_trades.groupby(date_field, as_index=False)["signed_quantity"]
          .sum()
          .rename(columns={"signed_quantity": "net_quantity"})
        )

      activity_by_day = {
        row[date_field]: float(row["net_quantity"]) for _, row in pair_activity.iterrows()
      }

      for trade_day in self.trading_days:
        position_date = _iso_date(trade_day)
        net_quantity = activity_by_day.get(position_date, 0.0)
        buy_quantity = max(net_quantity, 0.0)
        sell_quantity = max(-net_quantity, 0.0)
        cumulative_net += net_quantity
        expected_position = starting_quantity + cumulative_net

        records.append(
          {
            "account_id": account_id,
            "security_id": security_id,
            "position_date": position_date,
            "starting_quantity": starting_quantity if position_date == _iso_date(self.day_zero) else 0.0,
            "buy_quantity": buy_quantity,
            "sell_quantity": sell_quantity,
            "expected_position": round(expected_position, 4),
          }
        )

    return pd.DataFrame(records)

  def compute_expected_positions(self) -> None:
    """Compute trade-date and settlement-date expected position views."""
    self._trade_date_expected = self._compute_expected_positions(
      self.clean_trades, date_field="trade_date"
    )
    self._settlement_date_expected = self._compute_expected_positions(
      self.clean_trades, date_field="settlement_date"
    )

  # -------------------------------------------------------------------------
  # Step 4 — Reported positions (clean copy of trade-date expected)
  # -------------------------------------------------------------------------

  def generate_reported_positions(self) -> None:
    """
    Copy trade-date expected positions into reported position snapshots.

    At this stage reported quantities match the clean ledger exactly.
    Issue injection will corrupt selected records afterward.
    """
    records: list[ReportedPositionRecord] = []
    for _, row in self._trade_date_expected.iterrows():
      records.append(
        ReportedPositionRecord(
          account_id=row["account_id"],
          security_id=row["security_id"],
          position_date=row["position_date"],
          reported_quantity=round(float(row["expected_position"]), 4),
          source_system="POSITION_REPORTING",
        )
      )
    self.reported_positions = records

  # -------------------------------------------------------------------------
  # Step 5 — Filing events
  # -------------------------------------------------------------------------

  def generate_filing_events(self) -> None:
    """Create a small set of filing-style events tied to securities."""
    records: list[FilingEventRecord] = []
    event_days = self.rng.sample(self.trading_days[3:-3], k=min(8, len(self.trading_days) - 6))

    for index, (security, event_day) in enumerate(
      zip(self.securities, sorted(event_days), strict=False)
    ):
      event_type = config.FILING_EVENT_TYPES[index % len(config.FILING_EVENT_TYPES)]
      records.append(
        FilingEventRecord(
          event_id=f"EVT{index + 1:04d}",
          security_id=security["security_id"],
          event_date=_iso_date(event_day),
          event_type=event_type,
          event_summary=(
            f"{security['company_name']} {event_type.replace('_', ' ').lower()} "
            f"on {_iso_date(event_day)}"
          ),
        )
      )

    self.filing_events = records

  # -------------------------------------------------------------------------
  # Step 6 — Controlled issue injection
  # -------------------------------------------------------------------------

  def _next_issue_id(self) -> str:
    self._issue_counter += 1
    return f"ISS{self._issue_counter:04d}"

  def _traded_security_dates(self) -> set[tuple[str, str]]:
    """Return (security_id, trade_date) pairs from the clean trade ledger."""
    return {(trade["security_id"], trade["trade_date"]) for trade in self.clean_trades}

  def _event_window_security_dates(self) -> set[tuple[str, str]]:
    """Return (security_id, date) pairs inside a filing event window."""
    day_index = {_iso_date(day): index for index, day in enumerate(self.trading_days)}
    window_dates: set[tuple[str, str]] = set()
    radius = config.FILING_EVENT_WINDOW_DAYS

    for event in self.filing_events:
      event_index = day_index.get(event["event_date"])
      if event_index is None:
        continue

      start = max(0, event_index - radius)
      end = min(len(self.trading_days), event_index + radius + 1)
      for index in range(start, end):
        window_dates.add((event["security_id"], _iso_date(self.trading_days[index])))

    return window_dates

  def _missing_price_candidate_indices(self) -> list[int]:
    """Price rows eligible for MISSING_PRICE (traded date or event-window date)."""
    traded_dates = self._traded_security_dates()
    event_window_dates = self._event_window_security_dates()
    eligible_dates = traded_dates | event_window_dates

    return [
      index
      for index, price in enumerate(self.raw_prices)
      if (price["security_id"], price["price_date"]) in eligible_dates
      and (price["security_id"], price["price_date"]) not in self._removed_price_keys
    ]

  def _issue_manifest_key(
    self,
    issue_type: str,
    affected_record_id: str,
    issue_date: str | None,
  ) -> tuple[str, str, str | None]:
    return (issue_type, affected_record_id, issue_date)

  def _record_issue(
    self,
    *,
    issue_type: str,
    affected_entity: str,
    affected_record_id: str,
    expected_detection_category: str,
    short_description: str,
    account_id: str | None = None,
    security_id: str | None = None,
    trade_id: str | None = None,
    issue_date: str | None = None,
  ) -> InjectedIssueRecord | None:
    manifest_key = self._issue_manifest_key(issue_type, affected_record_id, issue_date)
    if manifest_key in self._manifest_keys:
      return None

    issue = InjectedIssueRecord(
      issue_id=self._next_issue_id(),
      issue_type=issue_type,  # type: ignore[typeddict-item]
      affected_entity=affected_entity,
      affected_record_id=affected_record_id,
      account_id=account_id,
      security_id=security_id,
      trade_id=trade_id,
      issue_date=issue_date,
      expected_detection_category=expected_detection_category,
      short_description=short_description,
    )
    self._manifest_keys.add(manifest_key)
    self.injected_issues.append(issue)
    return issue

  def inject_issues(self) -> None:
    """
    Corrupt selected raw records to create known, testable data-quality issues.

    Each injection is documented once in `injected_issues_manifest`.
    """
    self.raw_trades = deepcopy(self.clean_trades)
    self.raw_prices = deepcopy(self.prices)
    self.injected_issues = []
    self._manifest_keys = set()
    self._late_arriving_trade_ids = set()
    self._duplicated_trade_ids = set()
    self._removed_price_keys = set()
    self._clean_trade_count_before_injection = len(self.clean_trades)
    self._duplicate_trade_count_added = 0
    self._missing_trade_count_removed = 0
    self._invalid_trade_count_added = 0

    # Deterministic ordering of issue injectors — at least one of each required type.
    injectors = [
      self._inject_duplicate_trade,
      self._inject_missing_trade,
      self._inject_wrong_reported_position,
      self._inject_timing_mismatch,
      self._inject_late_arriving_trade,
      self._inject_missing_price,
      self._inject_invalid_security_id,
    ]

    for injector in injectors:
      injector()

    # Add extra instances until we reach the target issue count range.
    target_issues = self.rng.randint(config.MIN_INJECTED_ISSUES, config.MAX_INJECTED_ISSUES)
    extra_pool = [
      self._inject_duplicate_trade,
      self._inject_missing_trade,
      self._inject_wrong_reported_position,
      self._inject_late_arriving_trade,
      self._inject_missing_price,
    ]
    stagnant_attempts = 0
    while len(self.injected_issues) < target_issues and stagnant_attempts < len(extra_pool) * 3:
      before = len(self.injected_issues)
      self.rng.choice(extra_pool)()
      if len(self.injected_issues) == before:
        stagnant_attempts += 1
      else:
        stagnant_attempts = 0

    # OUT_OF_ORDER is applied at write time; record a single manifest entry now.
    self._inject_out_of_order_trade_events()

  def _inject_duplicate_trade(self) -> bool:
    if not self.raw_trades:
      return False

    eligible = [
      trade for trade in self.raw_trades if trade["trade_id"] not in self._duplicated_trade_ids
    ]
    if not eligible:
      return False

    original = eligible[self.rng.randrange(len(eligible))]
    self.raw_trades.append(deepcopy(original))
    self._duplicated_trade_ids.add(original["trade_id"])
    self._duplicate_trade_count_added += 1

    recorded = self._record_issue(
      issue_type="DUPLICATE_TRADE",
      affected_entity="trades",
      affected_record_id=original["trade_id"],
      account_id=original["account_id"],
      security_id=original["security_id"],
      trade_id=original["trade_id"],
      issue_date=original["trade_date"],
      expected_detection_category="DUPLICATE_TRADE",
      short_description=(
        f"Duplicated trade {original['trade_id']} in the raw trade feed."
      ),
    )
    return recorded is not None

  def _inject_missing_trade(self) -> bool:
    if not self.raw_trades:
      return False

    # Only remove singleton trades so raw state matches the manifest entry.
    removable_indices = [
      index
      for index, trade in enumerate(self.raw_trades)
      if sum(1 for row in self.raw_trades if row["trade_id"] == trade["trade_id"]) == 1
    ]
    if not removable_indices:
      return False

    index = removable_indices[self.rng.randrange(len(removable_indices))]
    removed = self.raw_trades.pop(index)
    self._missing_trade_count_removed += 1

    recorded = self._record_issue(
      issue_type="MISSING_TRADE",
      affected_entity="trades",
      affected_record_id=removed["trade_id"],
      account_id=removed["account_id"],
      security_id=removed["security_id"],
      trade_id=removed["trade_id"],
      issue_date=removed["trade_date"],
      expected_detection_category="MISSING_TRADE",
      short_description=(
        f"Removed trade {removed['trade_id']} from raw feed; it remains in the clean ledger."
      ),
    )
    return recorded is not None

  def _inject_wrong_reported_position(self) -> bool:
    if not self.reported_positions:
      return False

    target = self.reported_positions[self.rng.randrange(len(self.reported_positions))]
    record_id = (
      f"{target['account_id']}|{target['security_id']}|{target['position_date']}"
    )
    manifest_key = self._issue_manifest_key(
      "WRONG_REPORTED_POSITION", record_id, target["position_date"]
    )
    if manifest_key in self._manifest_keys:
      return False

    original_qty = target["reported_quantity"]
    delta = self.rng.choice([-25.0, -10.0, 10.0, 25.0, 50.0])
    target["reported_quantity"] = round(original_qty + delta, 4)

    recorded = self._record_issue(
      issue_type="WRONG_REPORTED_POSITION",
      affected_entity="reported_positions",
      affected_record_id=record_id,
      account_id=target["account_id"],
      security_id=target["security_id"],
      issue_date=target["position_date"],
      expected_detection_category="QUANTITY_MISMATCH",
      short_description=(
        f"Altered reported quantity from {original_qty} to "
        f"{target['reported_quantity']} for {record_id}."
      ),
    )
    return recorded is not None

  def _inject_timing_mismatch(self) -> bool:
    """
    Set reported quantity to the settlement-date expected value on a date where
    trade-date and settlement-date views diverge.

    Trade-date reconstruction will show a break; settlement-date view will match.
    """
    if self._trade_date_expected.empty or self._settlement_date_expected.empty:
      return False

    merged = self._trade_date_expected.merge(
      self._settlement_date_expected,
      on=["account_id", "security_id", "position_date"],
      suffixes=("_trade", "_settlement"),
    )
    divergent = merged[
      merged["expected_position_trade"].round(4) != merged["expected_position_settlement"].round(4)
    ]
    if divergent.empty:
      return False

    row = divergent.iloc[self.rng.randrange(len(divergent))]
    record_id = f"{row['account_id']}|{row['security_id']}|{row['position_date']}"
    manifest_key = self._issue_manifest_key("TIMING_MISMATCH", record_id, row["position_date"])
    if manifest_key in self._manifest_keys:
      return False

    settlement_qty = round(float(row["expected_position_settlement"]), 4)
    trade_qty = round(float(row["expected_position_trade"]), 4)

    for reported in self.reported_positions:
      if (
        reported["account_id"] == row["account_id"]
        and reported["security_id"] == row["security_id"]
        and reported["position_date"] == row["position_date"]
      ):
        reported["reported_quantity"] = settlement_qty
        break

    recorded = self._record_issue(
      issue_type="TIMING_MISMATCH",
      affected_entity="reported_positions",
      affected_record_id=record_id,
      account_id=row["account_id"],
      security_id=row["security_id"],
      issue_date=row["position_date"],
      expected_detection_category="TIMING_MISMATCH",
      short_description=(
        f"Reported quantity set to settlement-date expected ({settlement_qty}) "
        f"while trade-date expected is {trade_qty} for {record_id}."
      ),
    )
    return recorded is not None

  def _inject_late_arriving_trade(self) -> bool:
    if not self.raw_trades:
      return False

    eligible = [
      trade for trade in self.raw_trades if trade["trade_id"] not in self._late_arriving_trade_ids
    ]
    if not eligible:
      return False

    trade = eligible[self.rng.randrange(len(eligible))]
    trade_day = pd.Timestamp(trade["trade_date"])
    late_ingestion = trade_day + timedelta(days=self.rng.randint(3, 7))
    trade["ingestion_timestamp"] = _iso_timestamp(
      pd.Timestamp(late_ingestion.replace(hour=18, minute=0, second=0))
    )
    self._late_arriving_trade_ids.add(trade["trade_id"])

    recorded = self._record_issue(
      issue_type="LATE_ARRIVING_TRADE",
      affected_entity="trades",
      affected_record_id=trade["trade_id"],
      account_id=trade["account_id"],
      security_id=trade["security_id"],
      trade_id=trade["trade_id"],
      issue_date=trade["trade_date"],
      expected_detection_category="LATE_ARRIVING_TRADE",
      short_description=(
        f"Set ingestion_timestamp to {trade['ingestion_timestamp']} for trade "
        f"{trade['trade_id']} with trade_date {trade['trade_date']}."
      ),
    )
    return recorded is not None

  def _inject_missing_price(self) -> bool:
    candidate_indices = self._missing_price_candidate_indices()
    if not candidate_indices:
      return False

    index = candidate_indices[self.rng.randrange(len(candidate_indices))]
    removed = self.raw_prices.pop(index)
    price_key = (removed["security_id"], removed["price_date"])
    self._removed_price_keys.add(price_key)
    record_id = f"{removed['security_id']}|{removed['price_date']}"

    recorded = self._record_issue(
      issue_type="MISSING_PRICE",
      affected_entity="prices",
      affected_record_id=record_id,
      security_id=removed["security_id"],
      issue_date=removed["price_date"],
      expected_detection_category="MISSING_PRICE",
      short_description=f"Removed price record for {record_id} from the raw price feed.",
    )
    return recorded is not None

  def _inject_invalid_security_id(self) -> bool:
    if not self.raw_trades:
      return False

    trade = deepcopy(self.raw_trades[self.rng.randrange(len(self.raw_trades))])
    trade["trade_id"] = f"TRD_INVALID_{self._issue_counter + 1:04d}"
    trade["security_id"] = config.INVALID_SECURITY_PLACEHOLDER
    self.raw_trades.append(trade)
    self._invalid_trade_count_added += 1

    recorded = self._record_issue(
      issue_type="INVALID_SECURITY_ID",
      affected_entity="trades",
      affected_record_id=trade["trade_id"],
      account_id=trade["account_id"],
      security_id=trade["security_id"],
      trade_id=trade["trade_id"],
      issue_date=trade["trade_date"],
      expected_detection_category="UNKNOWN_SECURITY",
      short_description=(
        f"Added trade {trade['trade_id']} referencing unknown security "
        f"{config.INVALID_SECURITY_PLACEHOLDER}."
      ),
    )
    return recorded is not None

  def _inject_out_of_order_trade_events(self) -> bool:
    """Record that raw trades will be shuffled before persistence."""
    recorded = self._record_issue(
      issue_type="OUT_OF_ORDER_TRADE_EVENTS",
      affected_entity="trades",
      affected_record_id="ALL_TRADES",
      issue_date=_iso_date(self.day_zero),
      expected_detection_category="OUT_OF_ORDER_TRADE_EVENTS",
      short_description=(
        "Raw trade records are written in shuffled event order; pipelines must "
        "sort by trade_date and event_timestamp."
      ),
    )
    return recorded is not None

  # -------------------------------------------------------------------------
  # Step 7 — Persist outputs
  # -------------------------------------------------------------------------

  def _trades_for_output(self) -> list[TradeRecord]:
    """Return trades in intentionally shuffled order for raw ingestion realism."""
    # Use a dedicated RNG so manifest issue ordering does not affect shuffle.
    shuffle_rng = random.Random(self.seed + 9_001)
    shuffled = deepcopy(self.raw_trades)
    shuffle_rng.shuffle(shuffled)
    return shuffled

  def write_outputs(self) -> dict[str, Path]:
    """Write all JSONL datasets to configured raw paths."""
    paths = config.OUTPUT_PATHS

    _write_jsonl(self.securities, paths["securities"])
    _write_jsonl(self.raw_prices, paths["prices"])
    _write_jsonl(self.starting_positions, paths["starting_positions"])
    _write_jsonl(self._trades_for_output(), paths["trades"])
    _write_jsonl(self.reported_positions, paths["reported_positions"])
    _write_jsonl(self.filing_events, paths["filing_events"])
    _write_jsonl(self.injected_issues, paths["manifest"])

    return paths

  # -------------------------------------------------------------------------
  # Validation
  # -------------------------------------------------------------------------

  def validate_generation(self) -> list[str]:
    """Run post-generation checks; return error messages (empty if all pass)."""
    errors: list[str] = []

    manifest_keys = [
      self._issue_manifest_key(
        issue["issue_type"], issue["affected_record_id"], issue.get("issue_date")
      )
      for issue in self.injected_issues
    ]
    if len(manifest_keys) != len(set(manifest_keys)):
      errors.append(
        "duplicate manifest rows found with the same issue_type + affected_record_id + issue_date"
      )

    traded_dates = self._traded_security_dates()
    event_window_dates = self._event_window_security_dates()
    for issue in self.injected_issues:
      if issue["issue_type"] != "MISSING_PRICE":
        continue
      security_id = issue.get("security_id")
      issue_date = issue.get("issue_date")
      if not security_id or not issue_date:
        errors.append(f"MISSING_PRICE issue {issue['issue_id']} missing security_id or issue_date")
        continue
      price_key = (security_id, issue_date)
      if price_key not in traded_dates and price_key not in event_window_dates:
        errors.append(
          f"MISSING_PRICE issue {issue['issue_id']} targets {security_id}|{issue_date}, "
          "which is not a traded or event-window date"
        )

    starting_pairs = {(row["account_id"], row["security_id"]) for row in self.starting_positions}
    traded_pairs = {(trade["account_id"], trade["security_id"]) for trade in self.clean_trades}
    missing_pairs = sorted(traded_pairs - starting_pairs)
    if missing_pairs:
      errors.append(
        f"traded account/security pairs missing Day 0 starting positions: {missing_pairs}"
      )

    return errors

  # -------------------------------------------------------------------------
  # Orchestration
  # -------------------------------------------------------------------------

  def run(self) -> GenerationSummary:
    """Execute the full generation pipeline and return summary statistics."""
    # --- Clean ledger ---
    self.generate_securities()
    self.generate_prices()
    self.generate_starting_positions()
    self.generate_clean_trades()
    self.ensure_explicit_starting_positions()
    self.compute_expected_positions()
    self.generate_reported_positions()
    self.generate_filing_events()

    # --- Controlled corruption ---
    self.inject_issues()

    validation_messages = self.validate_generation()
    validation_passed = not validation_messages

    if not validation_passed:
      raise ValueError("Generation validation failed:\n- " + "\n- ".join(validation_messages))

    output_paths = self.write_outputs()

    return GenerationSummary(
      securities=len(self.securities),
      prices=len(self.raw_prices),
      starting_positions=len(self.starting_positions),
      trades=len(self.raw_trades),
      reported_positions=len(self.reported_positions),
      filing_events=len(self.filing_events),
      injected_issues=len(self.injected_issues),
      clean_trade_count_before_injection=self._clean_trade_count_before_injection,
      raw_trade_count_after_injection=len(self.raw_trades),
      duplicate_trade_count_added=self._duplicate_trade_count_added,
      missing_trade_count_removed=self._missing_trade_count_removed,
      invalid_trade_count_added=self._invalid_trade_count_added,
      injected_issue_count=len(self.injected_issues),
      validation_passed=validation_passed,
      validation_messages=validation_messages,
      output_paths=output_paths,
    )


def _print_summary(summary: GenerationSummary) -> None:
  print("FinSignal Lakehouse — Synthetic Ledger Generation Complete")
  print("-" * 60)
  print(f"Securities generated:              {summary.securities}")
  print(f"Price records generated:           {summary.prices}")
  print(f"Starting positions generated:      {summary.starting_positions}")
  print(f"Reported positions generated:      {summary.reported_positions}")
  print(f"Filing events generated:           {summary.filing_events}")
  print("-" * 60)
  print(f"clean_trade_count_before_injection: {summary.clean_trade_count_before_injection}")
  print(f"raw_trade_count_after_injection:    {summary.raw_trade_count_after_injection}")
  print(f"duplicate_trade_count_added:        {summary.duplicate_trade_count_added}")
  print(f"missing_trade_count_removed:        {summary.missing_trade_count_removed}")
  print(f"invalid_trade_count_added:          {summary.invalid_trade_count_added}")
  print(f"injected_issue_count:               {summary.injected_issue_count}")
  print("-" * 60)
  print("Validation checks:")
  checks = [
    "no duplicate manifest rows with same issue_type + affected_record_id + issue_date",
    "missing price issue targets a traded or event-window date",
    "all traded account/security pairs have starting position rows",
  ]
  for check in checks:
    status = "PASS" if summary.validation_passed else "FAIL"
    print(f"  {status}: {check}")
  if summary.validation_messages:
    for message in summary.validation_messages:
      print(f"    - {message}")
  print("-" * 60)
  print("Output paths written:")
  for name, path in summary.output_paths.items():
    print(f"  {name}: {path}")


def main(argv: list[str] | None = None) -> None:
  parser = argparse.ArgumentParser(
    description="Generate controlled synthetic ledger data for FinSignal Lakehouse.",
  )
  parser.add_argument(
    "--seed",
    type=int,
    default=config.DEFAULT_SEED,
    help=f"Random seed for reproducible generation (default: {config.DEFAULT_SEED}).",
  )
  args = parser.parse_args(argv)

  generator = LedgerGenerator(seed=args.seed)
  summary = generator.run()
  _print_summary(summary)


if __name__ == "__main__":
  main()
