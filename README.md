# FinSignal Lakehouse

**Trade-Position Reconciliation & Event Analytics Platform**

FinSignal Lakehouse is a PySpark-based financial data engineering project focused on a realistic operations problem: reconciling trades against reported positions. It ingests raw trade, price, position, security, and event data through bronze/silver/gold lakehouse layers, reconstructs expected positions from trade activity, detects reconciliation breaks, and computes bounded event-window analytics around filing-style events.

The project is intentionally **not** a trading bot, stock predictor, or generic financial chatbot. It demonstrates how financial data platforms build reliable structured outputs before any optional AI layer is added.

## Core Question

Given raw trades, starting positions, reported positions, price data, and event data, can the system:

1. Reconstruct expected positions
2. Compare them against reported positions
3. Detect and classify reconciliation breaks
4. Produce investigation-ready gold-layer outputs

## Architecture

```text
Synthetic Ledger Generator
        │
        ▼
   Raw JSONL          data/raw/
        │
        ▼
   Bronze Parquet     data/bronze/     (+ ingestion metadata)
        │
        ▼
   Silver Parquet     data/silver/     (planned: clean, typed, validated)
        │
        ▼
   Gold Parquet       data/gold/       (planned: reconciliation + event analytics)
```

### Layering

| Layer | Format | Purpose |
|-------|--------|---------|
| **Raw** | JSONL | Simulated API/event payloads from the synthetic ledger generator |
| **Bronze** | Parquet | Preserve raw fields; add `_ingested_at`, `_source_file`, `_bronze_load_id`, `_raw_record_hash` |
| **Silver** | Parquet | Schema enforcement, normalization, validation, trade-quality flags |
| **Gold** | Parquet | Position reconstruction, reconciliation breaks, event-window metrics |

### Reconciliation Logic (planned)

The MVP uses **checkpointed starting positions** as trusted Day 0 baselines:

```text
expected_position = starting_quantity + cumulative_buys - cumulative_sells
```

It does not reconstruct full account history from inception. Reconciliation runs over a fixed 30-day window.

## Data Entities

| Entity | Grain | Description |
|--------|-------|-------------|
| `securities` | 1 row / security | Reference data for tradable instruments |
| `prices` | 1 row / security / date | Daily OHLCV prices |
| `starting_positions` | 1 row / account / security / date | Trusted Day 0 checkpoint baselines |
| `trades` | 1 row / trade | Raw trade events with trade and settlement dates |
| `reported_positions` | 1 row / account / security / date | Position snapshots from reporting systems |
| `filing_events` | 1 row / event | Simplified filing-style events for event-window analytics |
| `injected_issues_manifest` | 1 row / issue | Ground-truth record of intentionally injected data-quality issues |

See [docs/data_model.md](docs/data_model.md) for full column definitions and gold-layer outputs.

## Controlled Synthetic Data

The ledger generator creates a **clean financial world first**, computes true expected positions, copies them into reported positions, then injects known data-quality issues with a manifest for pipeline validation.

**v1 scale:** 3 accounts · 5 securities · 30 trading days · 150–300 trades · 10–20 injected issues

**Injected issue types:**

- `DUPLICATE_TRADE`
- `MISSING_TRADE`
- `WRONG_REPORTED_POSITION`
- `TIMING_MISMATCH`
- `LATE_ARRIVING_TRADE`
- `MISSING_PRICE`
- `INVALID_SECURITY_ID`
- `OUT_OF_ORDER_TRADE_EVENTS`

## Project Status

| Milestone | Status | Description |
|-----------|--------|-------------|
| 1 — Synthetic Ledger Generator | **Done** | Controlled raw JSONL + injected issues manifest |
| 2 — Bronze Ingestion | **Done** | Raw JSONL → bronze Parquet with ingestion metadata |
| 3 — Silver Cleaning | Planned | Schema enforcement, validation, trade-quality flags |
| 4 — Gold Reconciliation | Planned | Position reconstruction and break classification |
| 5 — Event-Window Analytics | Planned | Bounded metrics around filing events |
| 6 — AI Investigation | Optional | Retrieval over curated gold/silver outputs only |

## Repository Layout

```text
FinSignal_Lakehouse/
├── data/
│   ├── raw/                  # JSONL from synthetic generator
│   └── bronze/               # Parquet from bronze ingestion
├── docs/
│   ├── project_brief.md      # Scope, milestones, interview positioning
│   ├── requirements.md       # Functional and non-functional requirements
│   ├── data_model.md         # Entity and layer definitions
│   └── architecture_decisions/
│       └── 001-scope-lock.md
├── src/
│   ├── data_generation/      # Milestone 1: synthetic ledger generator
│   ├── pipelines/            # Milestone 2+: PySpark batch pipelines
│   └── utils/                # Shared helpers (Spark session, paths)
└── requirements.txt
```

## Setup

### Prerequisites

- Python 3.10+
- Java JDK 17 or 21 (required for PySpark; JDK 26 is not yet supported)

On macOS with Homebrew OpenJDK:

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

Add those `export` lines to `~/.zshrc` to persist them.

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### 1. Generate raw synthetic ledger

```bash
python -m src.data_generation.generate_ledger --seed 42
```

Writes JSONL to `data/raw/` and an `injected_issues_manifest.jsonl` ground-truth file.

### 2. Ingest bronze tables

```bash
python -m src.pipelines.bronze_ingestion --load-id test_load_001
```

Reads raw JSONL, preserves source fields, adds bronze metadata, and writes Parquet to `data/bronze/`.

## Out of Scope (MVP)

- Corporate actions, macro indicators, full SEC filing ingestion
- Real-time streaming, Kafka, Airflow, Kubernetes
- Trading strategies, stock prediction, buy/sell recommendations
- Dashboards/UI before core pipeline works
- LangGraph / complex agent workflows
- Optional AI layer before structured pipelines are complete

## Documentation

- [Project Brief & Scope Lock](docs/project_brief.md)
- [Requirements](docs/requirements.md)
- [Data Model](docs/data_model.md)
- [ADR 001 — Scope Lock](docs/architecture_decisions/001-scope-lock.md)

## License

See [LICENSE](LICENSE).
