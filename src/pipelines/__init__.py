"""Pipeline entry points for FinSignal Lakehouse."""

__all__ = [
    "BronzeIngestionResult",
    "SilverCleaningResult",
    "main",
    "run_bronze_ingestion",
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
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
