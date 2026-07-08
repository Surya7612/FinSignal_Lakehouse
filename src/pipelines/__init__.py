"""Bronze layer ingestion pipelines."""

__all__ = ["BronzeIngestionResult", "main", "run_bronze_ingestion"]


def __getattr__(name: str):
    if name in __all__:
        from src.pipelines.bronze_ingestion import BronzeIngestionResult, main, run_bronze_ingestion

        return {
            "BronzeIngestionResult": BronzeIngestionResult,
            "main": main,
            "run_bronze_ingestion": run_bronze_ingestion,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
