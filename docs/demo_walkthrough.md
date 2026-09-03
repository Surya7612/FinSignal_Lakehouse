# FinSignal Lakehouse — Demo Walkthrough

This walkthrough is written for interview/demo use. It explains the problem, architecture, one concrete reconciliation case, and how ground truth validates the pipeline.

## 1. Problem

Operations teams need to answer:

> “Why does account **ACC001** show **SEC002** quantity **191** on **2025-01-20** when our trade ledger implies **382**?”

FinSignal Lakehouse automates the structured path to that answer:

1. ingest raw financial events
2. reconstruct expected holdings
3. compare against reported positions
4. classify breaks
5. assemble investigation evidence

## 2. Architecture (30 seconds)

```text
Synthetic Ledger (JSONL + manifest)
        ↓
Bronze (Delta)   preserve raw + ingestion metadata
        ↓
Silver (Delta)   clean, validate, quality flags, split adjustment
        ↓
Gold (Delta)     position reconstruction, reconciliation breaks, event windows
        ↓
Investigation    corpus → embeddings → deterministic retrieval workflow
```

**Design principle:** structured outputs first, retrieval second, deterministic templates only (no LLM generation).

## 3. One-Command Reproducible Run

```bash
bash scripts/run_all.sh
```

Uses seed `42` and verifies:

- golden row counts
- manifest-driven anomaly detection
- split-break case
- cross-engine consistency (Spark vs Python vs C++)
- investigation groundedness

## 4. Controlled Synthetic Data

The generator builds a clean world first, then injects known anomalies and records them in:

`data/raw/manifest/injected_issues_manifest.jsonl`

Each manifest row includes:

- `issue_type` (e.g. `SPLIT_ADJUSTMENT_BREAK`)
- `expected_detection_category`
- affected account/security/date

This gives ground truth for pipeline validation.

### Manifest validation

```bash
python -m src.validation.manifest_validation
```

Expected result (seed 42): **17/17 detected**, detection rate **1.0**.

Detection examples:

| Issue | How detected |
|-------|----------------|
| `DUPLICATE_TRADE` | `silver.trade_quality_flags` |
| `MISSING_TRADE` | trade absent from `silver.trades_clean` |
| `WRONG_REPORTED_POSITION` | gold reconciliation break |
| `SPLIT_ADJUSTMENT_BREAK` | quality flag + gold break |
| `OUT_OF_ORDER_TRADE_EVENTS` | structural pass (pipeline succeeds on shuffled raw feed) |

## 5. Hero Case: Stock Split Break

### Injected issue (manifest)

`ISS0008` — `SPLIT_ADJUSTMENT_BREAK` for `ACC001|SEC002|2025-01-20`

Reported quantity was altered from **382** to **191** on split effective date using incorrect split handling.

### Gold reconciliation output

```json
{
  "account_id": "ACC001",
  "security_id": "SEC002",
  "position_date": "2025-01-20",
  "expected_position": 382.0,
  "reported_position": 191.0,
  "position_difference": -191.0,
  "break_reason_code": "SPLIT_ADJUSTMENT_BREAK",
  "root_cause_reason_code": "SPLIT_ADJUSTMENT_BREAK"
}
```

Gold counts (seed 42):

- `position_reconstruction`: **450**
- `reconciliation_breaks`: **64**
- `trade_quality_flags`: **9**

## 6. Cross-Engine Consistency

Spark handles batch lakehouse analytics. A C++20/pybind11 reference engine implements the same deterministic reconciliation core outside Spark.

Validation strategy:

1. **Python reference engine** — pure Python mirror of core logic
2. **C++ engine** — parity-tested against Python
3. **Spark gold output** — compared on overlapping account/security/date keys

At seed 42:

- Spark/Python/C++ agree on expected position, reported position, status, and break reason for all Spark gold keys
- Python/C++ full outputs also match each other

Build and benchmark:

```bash
bash scripts/build_cpp_engine.sh
python scripts/benchmark_position_engine.py
pytest tests/test_cross_engine_consistency.py -q
```

Resume-safe framing:

> C++ provides a portable deterministic reconciliation core validated by automated parity tests; Spark remains the batch lakehouse execution layer.

## 7. Investigation Workflow (Deterministic)

Query:

```bash
python -m src.retrieval.investigation_graph \
  --query "Why is there a break for ACC001 SEC002 on 2025-01-20?"
```

Workflow nodes:

1. parse account/security/date from query
2. lookup structured break in gold tables
3. fetch quality flags and event context
4. hybrid evidence retrieval (metadata-first, semantic fallback)
5. build template investigation packet

Evidence ordering priority:

1. `RECONCILIATION_BREAK_SUMMARY`
2. `TRADE_QUALITY_SUMMARY`
3. `EVENT_WINDOW_SUMMARY`

Evaluation:

```bash
python -m src.retrieval.evaluate_investigation \
  --query "Why is there a break for ACC001 SEC002 on 2025-01-20?"
```

Expected (seed 42):

- `structured_break_found`: PASS
- `exact_evidence_found`: PASS
- `evidence_type_coverage`: PASS
- `unsupported_claim_risk`: LOW
- `groundedness_score`: **1.0**

## 8. What This Project Is (and Is Not)

**Is:**

- trade-position reconciliation lakehouse
- synthetic anomaly injection with manifest validation
- gold-layer break classification
- bounded event-window analytics
- deterministic retrieval-based investigation with groundedness checks
- C++ reference engine with parity tests

**Is not:**

- trading bot / price predictor
- Databricks deployment (local PySpark; patterns are portable)
- LLM-generated investigation summaries
- streaming orchestration platform

## 9. Suggested 90-Second Interview Narrative

1. “I modeled a realistic ops problem: reconciling trades against reported positions over a 30-day window.”
2. “I generate controlled synthetic data, inject known anomalies, and store ground truth in a manifest.”
3. “Bronze/silver/gold Delta pipelines reconstruct expected holdings and classify breaks; split-adjustment is a first-class break type.”
4. “I validate detection against the manifest and cross-check Spark gold against Python/C++ reference engines.”
5. “For investigation, I combine structured gold outputs with retrieved evidence and score groundedness deterministically—no LLM hand-waving.”

## 10. Quick Verification Checklist

- [ ] `bash scripts/run_all.sh` completes successfully
- [ ] `python -m src.validation.manifest_validation` shows 17/17 detected
- [ ] split-break query returns `SPLIT_ADJUSTMENT_BREAK`
- [ ] evaluation returns `groundedness_score >= 0.8`
- [ ] `pytest tests/test_cross_engine_consistency.py -q` passes
