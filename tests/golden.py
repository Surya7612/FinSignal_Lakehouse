"""Golden expectations for seed=42 reproducible pipeline runs."""

DEFAULT_SEED = 42

GOLD_ROW_COUNTS = {
    "position_reconstruction": 450,
    "reconciliation_breaks": 64,
    "trade_quality_flags": 9,
    "event_window_metrics": 5,
    "corporate_actions_clean": 1,
    "investigation_corpus": 78,
}

SPLIT_BREAK = {
    "account_id": "ACC001",
    "security_id": "SEC002",
    "position_date": "2025-01-20",
    "break_reason_code": "SPLIT_ADJUSTMENT_BREAK",
    "expected_position": 382.0,
    "reported_position": 191.0,
    "position_difference": -191.0,
}

INVESTIGATION_QUERY = "Why is there a break for ACC001 SEC002 on 2025-01-20?"

MANIFEST_ISSUE_COUNT = 17
MANIFEST_DETECTION_RATE = 1.0
