"""Controlled synthetic ledger generation for FinSignal Lakehouse."""

__all__ = ["LedgerGenerator", "main"]


def __getattr__(name: str):
  if name in __all__:
    from src.data_generation.generate_ledger import LedgerGenerator, main

    return {"LedgerGenerator": LedgerGenerator, "main": main}[name]
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
