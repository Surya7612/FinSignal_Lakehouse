"""Typed record shapes for synthetic ledger entities."""

from __future__ import annotations

from typing import Literal, TypedDict

TradeSide = Literal["BUY", "SELL"]

IssueType = Literal[
    "DUPLICATE_TRADE",
    "MISSING_TRADE",
    "WRONG_REPORTED_POSITION",
    "TIMING_MISMATCH",
    "LATE_ARRIVING_TRADE",
    "MISSING_PRICE",
    "INVALID_SECURITY_ID",
    "OUT_OF_ORDER_TRADE_EVENTS",
    "SPLIT_ADJUSTMENT_BREAK",
]


class SecurityRecord(TypedDict):
    security_id: str
    ticker: str
    company_name: str
    exchange: str
    sector: str
    is_active: bool


class PriceRecord(TypedDict):
    security_id: str
    price_date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int


class StartingPositionRecord(TypedDict):
    account_id: str
    security_id: str
    position_date: str
    quantity: float


class TradeRecord(TypedDict):
    trade_id: str
    account_id: str
    security_id: str
    side: TradeSide
    quantity: float
    execution_price: float
    trade_date: str
    settlement_date: str
    event_timestamp: str
    ingestion_timestamp: str
    source_system: str


class ReportedPositionRecord(TypedDict):
    account_id: str
    security_id: str
    position_date: str
    reported_quantity: float
    source_system: str


class FilingEventRecord(TypedDict):
    event_id: str
    security_id: str
    event_date: str
    event_type: str
    event_summary: str


class CorporateActionRecord(TypedDict):
    corporate_action_id: str
    security_id: str
    action_type: str
    effective_date: str
    split_ratio: float
    description: str


class InjectedIssueRecord(TypedDict, total=False):
    issue_id: str
    issue_type: IssueType
    affected_entity: str
    affected_record_id: str
    account_id: str | None
    security_id: str | None
    trade_id: str | None
    issue_date: str | None
    expected_detection_category: str
    short_description: str
