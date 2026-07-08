"""Pipeline entry points for FinSignal Lakehouse."""

__all__ = [
    "BronzeIngestionResult",
    "GoldEventWindowResult",
    "GoldReconciliationResult",
    "SilverCleaningResult",
    "main",
    "run_bronze_ingestion",
    "run_gold_event_window",
    "run_gold_reconciliation",
    "run_silver_cleaning",
]


def __getattr__(name: str):
    if name == "BronzeIngestionResult":
        from src.pipelines.bronze_ingestion import BronzeIngestionResult

        return BronzeIngestionResult
    if name in {"run_bronze_ingestion", "main"}:
        from src.pipelines.bronze_ingestion import main, run_bronze_ingestion

        return {
            "main": main,
            "run_bronze_ingestion": run_bronze_ingestion,
        }[name]
    if name == "SilverCleaningResult":
        from src.pipelines.silver_cleaning import SilverCleaningResult

        return SilverCleaningResult
    if name == "run_silver_cleaning":
        from src.pipelines.silver_cleaning import run_silver_cleaning

        return run_silver_cleaning
    if name == "GoldReconciliationResult":
        from src.pipelines.gold_reconciliation import GoldReconciliationResult

        return GoldReconciliationResult
    if name == "run_gold_reconciliation":
        from src.pipelines.gold_reconciliation import run_gold_reconciliation

        return run_gold_reconciliation
    if name == "GoldEventWindowResult":
        from src.pipelines.gold_event_window import GoldEventWindowResult

        return GoldEventWindowResult
    if name == "run_gold_event_window":
        from src.pipelines.gold_event_window import run_gold_event_window

        return run_gold_event_window
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
