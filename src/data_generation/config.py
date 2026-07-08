"""Configuration constants for the controlled synthetic ledger generator."""

from pathlib import Path

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
DEFAULT_SEED: int = 42

# ---------------------------------------------------------------------------
# Data scale (Milestone 1 v1)
# ---------------------------------------------------------------------------
NUM_ACCOUNTS: int = 3
NUM_SECURITIES: int = 5
NUM_TRADING_DAYS: int = 30
MIN_TRADES: int = 150
MAX_TRADES: int = 300
MIN_INJECTED_ISSUES: int = 10
MAX_INJECTED_ISSUES: int = 20

# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------
# First trading day of the reconciliation window (ISO date).
WINDOW_START_DATE: str = "2025-01-02"

# Standard T+2 settlement offset in business days for clean trades.
SETTLEMENT_OFFSET_BUSINESS_DAYS: int = 2

# Trading days before/after a filing event included in the event-window price check.
FILING_EVENT_WINDOW_DAYS: int = 5

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
ACCOUNT_IDS: list[str] = ["ACC001", "ACC002", "ACC003"]

SOURCE_SYSTEMS: list[str] = ["OMS_ALPHA", "EMS_BETA", "CUSTODY_GAMMA"]

SECURITY_DEFINITIONS: list[dict[str, str | bool]] = [
    {
        "security_id": "SEC001",
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "is_active": True,
    },
    {
        "security_id": "SEC002",
        "ticker": "MSFT",
        "company_name": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "is_active": True,
    },
    {
        "security_id": "SEC003",
        "ticker": "JPM",
        "company_name": "JPMorgan Chase & Co.",
        "exchange": "NYSE",
        "sector": "Financials",
        "is_active": True,
    },
    {
        "security_id": "SEC004",
        "ticker": "XOM",
        "company_name": "Exxon Mobil Corporation",
        "exchange": "NYSE",
        "sector": "Energy",
        "is_active": True,
    },
    {
        "security_id": "SEC005",
        "ticker": "JNJ",
        "company_name": "Johnson & Johnson",
        "exchange": "NYSE",
        "sector": "Healthcare",
        "is_active": True,
    },
]

FILING_EVENT_TYPES: list[str] = [
    "EARNINGS_RELEASE",
    "SEC_10K",
    "SEC_10Q",
    "DIVIDEND_ANNOUNCEMENT",
    "GUIDANCE_UPDATE",
]

# Placeholder security id used for INVALID_SECURITY_ID injections.
INVALID_SECURITY_PLACEHOLDER: str = "SEC_INVALID_999"

# ---------------------------------------------------------------------------
# Output paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

RAW_DATA_ROOT: Path = PROJECT_ROOT / "data" / "raw"

OUTPUT_PATHS: dict[str, Path] = {
    "securities": RAW_DATA_ROOT / "securities" / "securities.jsonl",
    "prices": RAW_DATA_ROOT / "prices" / "prices.jsonl",
    "corporate_actions": RAW_DATA_ROOT / "corporate_actions" / "corporate_actions.jsonl",
    "starting_positions": RAW_DATA_ROOT / "starting_positions" / "starting_positions.jsonl",
    "trades": RAW_DATA_ROOT / "trades" / "trades.jsonl",
    "reported_positions": RAW_DATA_ROOT / "reported_positions" / "reported_positions.jsonl",
    "filing_events": RAW_DATA_ROOT / "filing_events" / "filing_events.jsonl",
    "manifest": RAW_DATA_ROOT / "manifest" / "injected_issues_manifest.jsonl",
}
