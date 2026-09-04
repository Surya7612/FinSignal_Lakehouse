# Requirements

## 1. Functional Requirements

## 1.1 Controlled Synthetic Data Generation

The system must generate a controlled synthetic financial ledger.

Required raw datasets:

1. securities
2. prices
3. corporate_actions
4. starting_positions
5. trades
6. reported_positions
7. filing_events
8. injected_issues_manifest

The generator must first create a clean base ledger, compute expected positions, and then intentionally inject known data-quality issues.

The first version should use:

* 3 accounts
* 5 securities
* 30 trading days
* 150–300 trades
* 10–20 injected issues

## 1.2 Required Injected Issues

The data generator must support these injected issue types:

1. Duplicate trades
2. Missing trades
3. Wrong reported positions
4. Timing mismatches between trade date and settlement date
5. Late-arriving trades
6. Missing prices
7. Invalid security IDs
8. Out-of-order trade events
9. Split-adjustment breaks (reported positions mishandle stock-split effective dates)

Each injected issue must be recorded in the `injected_issues_manifest`.

## 1.3 Injected Issues Manifest

The injected issues manifest must include:

* issue_id
* issue_type
* affected_entity
* affected_record_id
* account_id, if applicable
* security_id, if applicable
* trade_id, if applicable
* trade_date or event_date, if applicable
* expected_detection_category
* short_description

The manifest will be used to compare known injected issues against pipeline-detected issues.

## 1.4 Raw Layer

The raw layer must store generated payloads as JSON.

Expected raw output paths:

* data/raw/securities/
* data/raw/prices/
* data/raw/corporate_actions/
* data/raw/starting_positions/
* data/raw/trades/
* data/raw/reported_positions/
* data/raw/filing_events/
* data/raw/manifest/

CSV should not be used as the main project format.

## 1.5 Bronze Layer

The bronze ingestion pipeline must:

1. Read raw JSON payloads.
2. Preserve original source fields where possible.
3. Add ingestion metadata.
4. Add ingestion timestamp.
5. Avoid heavy transformations.
6. Write bronze outputs as Parquet or Delta-style tables.

Expected bronze tables:

* bronze_securities
* bronze_prices
* bronze_corporate_actions
* bronze_starting_positions
* bronze_trades
* bronze_reported_positions
* bronze_filing_events

## 1.6 Silver Layer

The silver cleaning pipeline must:

1. Enforce explicit schemas.
2. Normalize date fields.
3. Normalize timestamp fields.
4. Normalize security identifiers.
5. Normalize account identifiers.
6. Standardize trade side values into BUY and SELL.
7. Validate required fields.
8. Detect duplicate trade IDs.
9. Detect invalid security IDs.
10. Detect missing price records.
11. Detect invalid quantities.
12. Detect invalid trade prices.
13. Handle out-of-order trade events by sorting logically.
14. Write cleaned outputs as Parquet or Delta-style tables.

Expected silver tables:

* silver_securities_clean
* silver_prices_clean
* silver_corporate_actions_clean
* silver_starting_positions_clean
* silver_trades_clean
* silver_reported_positions_clean
* silver_events_clean

## 1.7 Trade Quality Flags

The system must create trade quality flags for records with issues.

Required flags:

* DUPLICATE_TRADE
* UNKNOWN_SECURITY
* MISSING_PRICE
* INVALID_QUANTITY
* INVALID_PRICE
* LATE_ARRIVING_TRADE

Expected output:

* silver_trade_quality_flags

## 1.8 Gold Reconciliation Engine

The gold reconciliation engine must:

1. Use starting positions as checkpoint baselines.
2. Aggregate trades by account, security, and date.
3. Calculate cumulative buy quantity.
4. Calculate cumulative sell quantity.
5. Reconstruct expected position.
6. Compare expected position against reported position.
7. Calculate position difference.
8. Assign reconciliation status.
9. Assign break reason code.
10. Output position reconstruction records.
11. Output reconciliation break records.

Expected outputs:

* gold_position_reconstruction
* gold_reconciliation_breaks

Required fields for `gold_position_reconstruction`:

* account_id
* security_id
* position_date
* starting_quantity
* cumulative_buy_quantity
* cumulative_sell_quantity
* expected_position
* reported_position
* position_difference
* reconciliation_status
* break_reason_code

Required reconciliation statuses:

* MATCH
* BREAK

Required break reason codes:

* MATCH
* DUPLICATE_TRADE
* MISSING_TRADE
* TIMING_MISMATCH
* LATE_ARRIVING_TRADE
* MISSING_PRICE
* UNKNOWN_SECURITY
* QUANTITY_MISMATCH
* POSITION_NOT_REPORTED
* SPLIT_ADJUSTMENT_BREAK

## 1.9 Timing Mismatch Logic

The reconciliation engine should support trade-date and settlement-date views.

The system should identify a timing mismatch when:

1. A break exists under trade-date logic.
2. The break disappears or materially changes under settlement-date logic.

This should be classified as:

* TIMING_MISMATCH

## 1.10 Event-Window Analytics

The event-window analytics module must:

1. Join filing events to securities.
2. Assign trading-day indexes to prices per security.
3. Map each event to the nearest trading-day index.
4. Use bounded joins or Spark window functions.
5. Avoid unbounded cross joins.
6. Calculate pre-event return.
7. Calculate post-event return.
8. Calculate price movement around event date.
9. Calculate volatility before event.
10. Calculate volatility after event.
11. Calculate position change around event.
12. Calculate exposure change around event.

Expected output:

* gold_event_window_metrics

Required event-window fields:

* event_id
* security_id
* event_date
* event_type
* pre_event_return
* post_event_return
* pre_event_volatility
* post_event_volatility
* position_change_around_event
* exposure_change_around_event

## 1.11 Deterministic Investigation Workflow

The investigation layer is downstream of gold outputs and does not use LLM generation.

It must:

1. Build an investigation corpus from curated silver/gold tables.
2. Index corpus documents for semantic retrieval.
3. Run a deterministic LangGraph workflow with metadata-first evidence retrieval.
4. Evaluate groundedness against structured gold outputs (no LLM judge).

Possible questions:

* Why was there a reconciliation break for this account/security/date?
* Which trades contributed to the mismatch?
* Was there a nearby filing event?
* What event context is relevant to this break?

## 1.12 Manifest Validation

The system must compare injected manifest issues against silver/gold pipeline outputs.

For seed 42, all 17 injected issues must be detected.

## 1.13 Cross-Engine Consistency

A Python reference engine and C++20/pybind11 reference engine must produce parity results on overlapping reconciliation keys with Spark gold outputs.

## 2. Non-Functional Requirements

## 2.1 Reproducibility

The project must be reproducible.

The data generator should use a fixed random seed by default.

Running the generator with the same seed should produce the same raw data and injected issue manifest.

## 2.2 Determinism

The reconciliation engine must be deterministic.

The same input data should always produce the same gold reconciliation outputs.

## 2.3 Explainability

Every major output should be explainable.

For each reconciliation break, the system should expose enough fields to understand:

* account
* security
* date
* expected position
* reported position
* difference
* likely reason code
* contributing trade records, if applicable

## 2.4 Modularity

The project should separate:

* data generation
* bronze ingestion
* silver cleaning
* validation
* reconciliation
* event analytics
* optional retrieval

## 2.5 Scalability Awareness

v1 can run locally, but the architecture should be designed with scalability in mind.

The project should document:

* why checkpointed starting positions are used
* why full historical reconstruction is avoided in v1
* how partitions would be chosen
* how cumulative windows could become expensive
* how event-window joins avoid Cartesian products
* how the vector store would scale beyond local FAISS/Chroma

## 2.6 Latency Awareness

v1 is batch-first.

The project should document that batch processing is acceptable for daily reconciliation and event analytics.

Streaming is intentionally excluded from v1.

## 2.7 Storage Format

Raw data must be JSON.

Bronze, silver, and gold data must be Parquet or Delta-style tables.

CSV must not be used as the main storage format.

## 2.8 Documentation

The README and docs must explain:

* project purpose
* architecture
* data model
* reconciliation logic
* injected issue strategy
* event-window logic
* tradeoffs
* limitations
* future improvements

## 3. v1 Constraints

v1 must not include:

1. Broad corporate-action coverage beyond stock splits
2. Macro indicators
3. Full SEC filing ingestion
4. Large-scale document parsing
5. Streaming
6. Kafka
7. Airflow
8. Kubernetes
9. Dashboard/UI
10. Trading strategy logic
11. Stock prediction
12. Buy/sell recommendations
13. LLM-generated investigation summaries
14. Databricks deployment artifacts

## 4. Definition of Done

v1 is complete when:

1. Synthetic raw data is generated for securities, prices, corporate actions, starting positions, trades, reported positions, and filing events.
2. An injected issues manifest is generated.
3. Raw data is saved as JSON.
4. Bronze tables are created.
5. Silver tables are created with explicit schemas and validation.
6. Trade quality flags are produced in silver.
7. Expected positions are reconstructed from starting positions and trades.
8. Reported positions are compared against expected positions.
9. Reconciliation breaks are classified.
10. Event-window metrics are calculated using bounded logic.
11. Gold outputs are saved as Delta (default) or Parquet tables.
12. Manifest validation detects all injected issues (17/17 for seed 42).
13. Cross-engine consistency checks pass for Python/C++ reference engines.
14. Deterministic investigation workflow and groundedness evaluation work.
15. Documentation explains design decisions and tradeoffs.

## 5. Reproducible Run

The canonical end-to-end run is:

```bash
bash scripts/run_all.sh
```

This regenerates data (seed 42), runs all pipelines, builds the C++ engine, validates manifest detection, and executes pytest.
