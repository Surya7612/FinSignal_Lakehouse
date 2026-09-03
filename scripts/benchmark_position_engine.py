"""Load silver-layer inputs and benchmark Python vs C++ position engines."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.algorithms.engine_loader import (
    build_cpp_engine,
    cpp_rows_to_dicts,
    load_engine_inputs,
    normalize_engine_rows,
    run_python_engine,
)
from src.utils.io import PROJECT_ROOT


def benchmark(*, iterations: int = 3, project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    inputs = load_engine_inputs(project_root=project_root)

    python_start = time.perf_counter()
    python_rows = run_python_engine(inputs)
    python_elapsed_ms = (time.perf_counter() - python_start) * 1000.0

    try:
        cpp_engine = build_cpp_engine(inputs)
        cpp_rows = cpp_rows_to_dicts(cpp_engine.reconstruct())
        cpp_times = []
        for _ in range(iterations):
            start = time.perf_counter()
            cpp_rows = cpp_rows_to_dicts(cpp_engine.reconstruct())
            cpp_times.append((time.perf_counter() - start) * 1000.0)
        cpp_elapsed_ms = sum(cpp_times) / len(cpp_times)
        cpp_available = True
    except ImportError:
        cpp_rows = []
        cpp_elapsed_ms = None
        cpp_available = False

    python_normalized = normalize_engine_rows(python_rows)
    cpp_normalized = normalize_engine_rows(cpp_rows) if cpp_available else []
    rows_match = python_normalized == cpp_normalized

    return {
        "python_row_count": len(python_rows),
        "cpp_row_count": len(cpp_rows),
        "rows_match": rows_match,
        "python_elapsed_ms": round(python_elapsed_ms, 3),
        "cpp_elapsed_ms": round(cpp_elapsed_ms, 3) if cpp_elapsed_ms is not None else None,
        "speedup_x": round(python_elapsed_ms / cpp_elapsed_ms, 2)
        if cpp_elapsed_ms and cpp_elapsed_ms > 0
        else None,
        "cpp_available": cpp_available,
        "note": (
            "Parity is the primary C++ validation metric at MVP scale; "
            "pybind marshalling dominates latency for small datasets."
        ),
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Benchmark Python vs C++ position engines.")
    parser.add_argument("--iterations", type=int, default=3, help="C++ benchmark iterations.")
    args = parser.parse_args(argv)

    result = benchmark(iterations=args.iterations)
    print(json.dumps(result, indent=2))
    if not result["cpp_available"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
