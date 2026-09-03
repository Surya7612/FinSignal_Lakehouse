# FinSignal Lakehouse — Project Brief & Scope Lock

## 1. Project Name

**FinSignal Lakehouse — Trade-Position Reconciliation & Event Analytics Platform**

## 2. One-Sentence Summary

FinSignal Lakehouse is a PySpark-based financial data engineering platform that ingests raw trade, price, position, security, and event data, processes it through bronze/silver/gold lakehouse layers, reconstructs expected positions from trade activity, detects reconciliation breaks, validates detections against a ground-truth manifest, computes bounded event-window analytics, and supports deterministic evidence-based investigation using curated lakehouse outputs.

## 3. Why I Am Building This

I am building this project to learn and demonstrate how financial data platforms support reliable reconciliation, analytics, and evidence-based investigation.

Instead of building a generic financial chatbot, trading bot, or stock prediction model, this project focuses on a more realistic financial data engineering problem: reconciling trades against reported positions.

In financial systems, data quality matters before AI or analytics can be trusted. A position mismatch can come from duplicate trades, missing trades, late-arriving records, timing mismatches, invalid securities, missing prices, or incorrect position reports. This project gives me a focused way to build a system that detects those issues, classifies them, and produces investigation-ready gold-layer outputs.

The project is designed to strengthen four areas:

1. Financial data modeling
2. PySpark and lakehouse data engineering
3. Algorithmic reconciliation and time-series processing
4. Optional evidence-based investigation built on reliable structured data

## 4. Target Roles

This project is designed to support applications for:

* Data Engineering roles
* Financial Data Engineering roles
* Data Platform roles
* Quantitative Data Infrastructure roles
* McKinsey / QuantumBlack-style data and AI engineering roles
* HRT-style data production, data infrastructure, and research platform roles

The project is not intended to claim trading alpha, stock prediction ability, portfolio optimization, or buy/sell recommendation logic.

## 5. Core Problem

Financial data often comes from separate systems:

* Trade execution logs
* Reported position snapshots
* Security reference data
* Price feeds
* Filing or event calendars

These systems may not align perfectly. Trades may be duplicated, missing, late, out of order, or tied to invalid securities. Reported positions may not match reconstructed positions. Prices may be missing for securities that traded. Events may explain changes in price, exposure, or position behavior.

The core question this project answers is:

**Given raw trades, starting positions, reported positions, price data, and event data, can the system reconstruct expected positions, compare them against reported positions, detect breaks, classify likely causes, and produce investigation-ready outputs?**

## 6. MVP Focus

The MVP focuses deeply on one structured financial data engineering problem:

**Trade-position reconciliation.**

A secondary module will compute event-window analytics around filing-style events.

The AI layer is optional and downstream. It should not carry the project.

The MVP should prove that the structured data pipeline works before any retrieval or AI layer is added.

## 7. MVP Data Entities

The MVP includes only these entities:

### Required Entities

1. **securities**
   Reference data for tradable instruments.

2. **prices**
   Daily price data by security and trading date.

3. **corporate_actions**
   Stock-split corporate actions (MVP: `STOCK_SPLIT` only).

4. **starting_positions**
   Trusted baseline positions at the beginning of the reconciliation window.

5. **trades**
   Raw trade events containing account, security, side, quantity, price, trade date, settlement date, event timestamp, and ingestion timestamp.

6. **reported_positions**
   Position snapshots reported by account, security, and date.

7. **filing_events**
   Simplified company or market events used for event-window analytics.

8. **injected_issues_manifest**
   Internal truth manifest documenting intentionally injected data-quality issues.

### Optional Later Entities

1. **event_notes**
   Short text notes for optional AI retrieval.

2. **accounts**
   Separate account reference table, only if account modeling needs to become more explicit.

## 8. Explicitly Out of Scope for MVP

The MVP will not include:

* Broad corporate-action coverage beyond stock splits
* Macro indicators
* Full SEC filing ingestion
* Large-scale PDF/document parsing
* Portfolio optimization
* Trading strategy backtesting
* Stock prediction
* Buy/sell recommendations
* Real-time streaming
* Kafka
* Airflow
* Kubernetes
* Real brokerage integration
* Bloomberg/Refinitiv-style data
* LLM-generated investigation summaries
* UI/dashboard

These are intentionally excluded to keep the first version focused, buildable, and interview-defensible.

## 9. Core Architecture

The project uses a batch-first bronze/silver/gold lakehouse architecture.

### Raw Layer

Synthetic data is emitted as raw JSON payloads to simulate event/API-style ingestion.

Expected raw outputs:

* raw_securities
* raw_prices
* raw_starting_positions
* raw_trades
* raw_reported_positions
* raw_filing_events
* injected_issues_manifest

### Bronze Layer

Bronze stores raw ingested records with minimal transformation.

Examples:

* bronze_securities
* bronze_prices
* bronze_starting_positions
* bronze_trades
* bronze_reported_positions
* bronze_filing_events

### Silver Layer

Silver stores cleaned, typed, normalized, and validated records.

Examples:

* silver_securities_clean
* silver_prices_clean
* silver_starting_positions_clean
* silver_trades_clean
* silver_reported_positions_clean
* silver_events_clean

### Gold Layer

Gold stores investigation-ready outputs.

Examples:

* gold_position_reconstruction
* gold_reconciliation_breaks
* silver_trade_quality_flags
* gold_event_window_metrics

## 10. Data Generation Strategy

The first milestone is not to generate a large financial universe.

The first milestone is to generate a controlled synthetic ledger.

The generator should follow this order:

1. Generate a clean base world.
2. Generate securities, prices, starting positions, trades, reported positions, and filing events.
3. Compute the true expected positions from the clean ledger.
4. Copy true expected positions into reported positions.
5. Intentionally corrupt selected records to create known data-quality issues.
6. Save the raw datasets.
7. Save an `injected_issues_manifest` documenting every intentional issue and the expected detection category.

This prevents random synthetic data from becoming meaningless. The data generator should produce controlled errors that the reconciliation engine can later detect and classify.

## 11. MVP Data Scale

The first version should stay intentionally small:

* 3 accounts
* 5 securities
* 30 trading days
* 150–300 trades
* 10–20 injected issues

The reconciliation window should be limited to 30 days for the first version.

The project may later add a scale-test mode with 60–90 days or larger trade counts, but only after the MVP works correctly.

## 12. Injected Data-Quality Issues

The generator must intentionally create these issue types:

1. **Duplicate trades**
   Same trade appears more than once.

2. **Missing trades**
   A trade is included in the clean ledger used to calculate true positions but removed from the raw trade feed.

3. **Wrong reported positions**
   Reported position quantity is intentionally altered.

4. **Timing mismatches**
   Trade-date and settlement-date logic produce different position views.

5. **Late-arriving trades**
   Trade date is earlier than the ingestion timestamp, simulating delayed arrival.

6. **Missing prices**
   Price record is missing for a security/date needed for valuation or event analytics.

7. **Invalid security IDs**
   Trade references a security not present in the securities reference table.

8. **Out-of-order trade events**
   Raw trade records are shuffled so pipeline correctness does not depend on input order.

9. **Split-adjustment breaks**
   Reported positions intentionally mishandle stock-split effective dates.

## 13. Injected Issues Manifest

The generator must output an `injected_issues_manifest`.

Each issue record should include:

* issue_id
* issue_type
* affected_entity
* affected_record_id
* account_id, if applicable
* security_id, if applicable
* trade_id, if applicable
* event_date or trade_date, if applicable
* expected_detection_category
* short_description

The manifest is used to validate whether the pipeline correctly detects known issues.

This makes the project testable and interview-defensible.

## 14. Storage Format Decision

The project avoids CSV as the primary format.

Raw data should be emitted as JSON to simulate API/event payloads.

Bronze, silver, and gold outputs should be stored as Parquet or Delta-style tables.

Preferred flow:

Raw JSON
→ Bronze Parquet/Delta
→ Silver Parquet/Delta
→ Gold Parquet/Delta

CSV may only be used for quick manual inspection, not as the main storage format.

## 15. Main Algorithmic Module: Trade-Position Reconciliation

The main algorithmic module is the trade-position reconciliation engine.

The engine uses starting position snapshots as trusted checkpoint baselines.

For each account, security, and position date:

Starting position

* cumulative buys

- cumulative sells
  = expected position

The engine compares expected positions against reported positions.

Expected outputs:

* account_id
* security_id
* position_date
* starting_quantity
* buy_quantity
* sell_quantity
* expected_position
* reported_position
* position_difference
* reconciliation_status
* break_reason_code

Possible reason codes:

* MATCH
* DUPLICATE_TRADE
* MISSING_TRADE
* TIMING_MISMATCH
* LATE_ARRIVING_TRADE
* MISSING_PRICE
* UNKNOWN_SECURITY
* QUANTITY_MISMATCH
* POSITION_NOT_REPORTED

## 16. Position Tracking Design

The MVP does not reconstruct full account history from inception.

Instead, it uses a checkpointed baseline approach:

Trusted starting position as of Day 0

* trades during the reconciliation window
  = expected positions during the window

This is intentional.

Full historical reconstruction can require large cumulative windows and stateful tracking. A checkpointed approach is more realistic for an MVP and mirrors how production systems often limit reconciliation windows using trusted snapshots.

## 17. Secondary Algorithmic Module: Event-Window Analytics

The secondary module is event-window analytics.

For each filing event, the system calculates market and position behavior around the event.

Expected metrics:

* pre-event return
* post-event return
* price movement around event date
* volatility before event
* volatility after event
* position change around event
* exposure change around event

The event-window engine must avoid naive cross joins.

Preferred design:

1. Assign a trading-day index per security.
2. Map each event to the nearest trading-day index.
3. Join prices using a bounded index range such as event_index - 5 to event_index + 5.
4. Compute returns and volatility within the bounded window.

This avoids accidental Cartesian products and keeps the event analytics scalable in design.

## 18. Optional AI Layer

The AI layer is optional and should come only after the structured pipeline works.

The AI layer may answer questions like:

* Why was there a reconciliation break for AAPL on this date?
* What trades contributed to this position mismatch?
* Was there a nearby event that could explain the exposure change?
* What does the event note say about the company around this date?

The AI layer should retrieve from curated data, not raw untrusted records.

Possible retrieval sources:

* reconciliation outputs
* trade quality flags
* event notes
* filing-style summaries
* event-window metrics

The structured lakehouse remains the source of truth. The vector index is only a downstream retrieval layer.

## 19. Vector Store Boundary

If a vector database is added, it is treated as a downstream serving layer.

The lakehouse owns clean structured and text/event tables.

The vector store consumes curated silver or gold outputs.

For the MVP, FAISS or Chroma may be used locally, but this must be documented as a single-node prototype.

Production alternatives include:

* distributed embedding generation
* embedding metadata stored in Delta tables
* managed vector search
* embedding refresh logic
* document version tracking

## 20. Main System Questions

The finished MVP should answer:

1. Did reported positions match positions reconstructed from trades?
2. Which trades contributed to a position mismatch?
3. Are there duplicate, missing, invalid, or late-arriving trades?
4. Are there missing prices for traded securities?
5. Does the break disappear under settlement-date logic?
6. How did price and position behavior change around a filing event?
7. What gold-layer outputs would an analyst or data engineer use to investigate the issue?

## 21. Success Criteria

The MVP is successful if it can:

1. Generate controlled synthetic financial ledger data.
2. Inject known data-quality and reconciliation issues.
3. Save an injected issues manifest.
4. Ingest raw JSON data into bronze tables.
5. Clean and normalize records into silver tables.
6. Reconstruct expected positions using starting position baselines and trade activity.
7. Compare expected positions against reported positions.
8. Produce `gold_reconciliation_breaks`.
9. Classify reconciliation breaks with reason codes.
10. Detect duplicate trades, invalid securities, missing prices, timing mismatches, and quantity mismatches.
11. Compute bounded event-window analytics around filing events.
12. Save gold outputs as Parquet or Delta-style tables.
13. Document architecture choices, tradeoffs, scalability concerns, and limitations.
14. Explain the full project clearly in a technical interview.

## 22. Build Milestones

### Milestone 1 — Controlled Synthetic Ledger Generator

Generate:

* securities
* prices
* starting_positions
* trades
* reported_positions
* filing_events
* injected_issues_manifest

The generator must create a clean ledger first, then intentionally inject known issues.

### Milestone 2 — Bronze Ingestion

Read raw JSON data, preserve raw fields, add ingestion metadata, and write bronze tables.

### Milestone 3 — Silver Cleaning and Validation

Enforce schemas, normalize identifiers, validate required fields, detect invalid securities, detect duplicate trades, detect missing prices, and write clean silver tables.

### Milestone 4 — Gold Reconciliation Engine

Reconstruct expected positions, compare them against reported positions, calculate differences, classify break reasons, and output gold reconciliation tables.

### Milestone 5 — Event-Window Analytics

Compute bounded event-window metrics around filing events using trading-day indexes or Spark window functions.

### Milestone 6 — Deterministic Investigation Workflow

Build investigation corpus documents, vector index, deterministic LangGraph workflow, and groundedness evaluation. No LLM generation.

### Milestone 7 — Validation and Reference Engines

Add manifest-driven validation, cross-engine consistency checks, and a C++20/pybind11 reference reconciliation engine with parity tests.

## 23. Interview Positioning

The strongest interview explanation is:

“I built FinSignal Lakehouse to understand how financial data platforms support reconciliation, analytics, and evidence-based investigation. I focused first on the data foundation: ingesting raw trade, price, position, and event data, cleaning it through bronze and silver layers, reconstructing expected positions from trade activity, and producing gold-layer reconciliation breaks. I intentionally injected known data-quality issues and validated detection against a ground-truth manifest. I added bounded event-window analytics, a deterministic investigation workflow with groundedness checks, and a C++ reference engine parity-tested against Spark and Python outputs.”

## 24. Scope Lock

The MVP is locked to:

* securities
* prices
* corporate_actions (stock splits only)
* starting_positions
* trades
* reported_positions
* filing_events
* injected_issues_manifest
* bronze/silver/gold pipelines
* reconciliation engine
* trade quality flags
* bounded event-window analytics
* manifest validation
* deterministic investigation workflow
* C++ reference engine with parity tests
* architecture and tradeoff documentation

Stock-split corporate actions, deterministic investigation, and C++ reference validation are in scope. LLM generation, Databricks deployment artifacts, streaming orchestration, and dashboards remain out of scope.
