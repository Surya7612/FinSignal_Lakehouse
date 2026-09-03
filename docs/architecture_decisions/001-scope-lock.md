# ADR 001 — Scope Lock: Trade-Position Reconciliation First

## Status

Accepted

## Decision

The MVP will focus on a batch-first financial lakehouse for trade-position reconciliation and bounded event-window analytics.

The core system will ingest raw trade, price, starting position, reported position, security, and event data; process it through bronze/silver/gold layers; reconstruct expected positions from trade activity; detect reconciliation breaks; classify likely break reasons; and produce investigation-ready gold outputs.

The optional retrieval layer is downstream and does not replace deterministic reconciliation logic.

## Why This Decision Was Made

The original project idea included too many components:

* market data
* trades
* positions
* portfolio holdings
* macro indicators
* corporate actions
* SEC filings
* research notes
* vector search
* LangChain/LangGraph
* event analytics
* reconciliation

This was too broad for a solo project under time pressure.

The highest-value and most defensible part of the project is trade-position reconciliation. It demonstrates financial data modeling, PySpark engineering, deterministic algorithms, data quality checks, time-series processing, and practical understanding of financial operations.

This is stronger than building a generic financial RAG chatbot or shallow lakehouse demo.

## Chosen MVP Scope

The MVP includes:

* securities
* prices
* corporate_actions (stock splits only)
* starting_positions
* trades
* reported_positions
* filing_events
* injected_issues_manifest
* bronze ingestion
* silver cleaning
* gold reconciliation outputs
* trade quality flags
* bounded event-window analytics
* manifest validation
* deterministic investigation workflow (no LLM generation)
* C++ reference engine with parity tests
* architecture and tradeoff documentation

## Explicitly Excluded From MVP

The MVP excludes:

* broad corporate-action coverage beyond stock splits
* macro indicators
* full SEC filings
* portfolio optimization
* trading strategy execution
* stock prediction
* buy/sell recommendations
* real-time streaming
* Kafka
* Airflow
* Kubernetes
* dashboard/UI
* real brokerage integration
* paid financial data
* LLM-generated investigation summaries
* Databricks deployment artifacts

## Alternatives Considered

## 1. Generic Financial RAG Chatbot

Rejected because it is common and does not strongly demonstrate data engineering depth.

## 2. SEC Filing Analysis Platform

Rejected for MVP because full SEC ingestion and document parsing can become a large project by itself.

## 3. Trading Bot

Rejected because it is overdone, difficult to validate, and risky to defend seriously in interviews.

## 4. Stock Prediction Model

Rejected because prediction quality would be hard to prove and could distract from the data engineering goal.

## 5. Broad Financial Lakehouse

Rejected for MVP because too many datasets would create shallow implementation and synthetic data complexity.

## 6. Healthcare Lakehouse

Rejected for this project because the current target is finance/data infrastructure roles.

## Key Design Choices

## 1. Checkpointed Starting Positions

The MVP uses starting position snapshots as trusted baselines.

This avoids reconstructing full account history from inception.

Trade-position reconciliation is calculated over a fixed 30-day window.

This keeps the MVP manageable while still demonstrating realistic reconciliation logic.

## 2. Controlled Synthetic Ledger

The generator creates a clean ledger first, then injects known issues.

This is better than random synthetic data because the system can be tested against ground truth.

The injected issues manifest records the known breaks and expected detection categories.

## 3. Batch-First Lakehouse

The MVP is batch-first because daily reconciliation and event analytics do not require streaming in the first version.

Streaming could be added later for intraday trade monitoring or live break detection.

## 4. JSON Raw Data, Parquet/Delta Outputs

Raw data is emitted as JSON to simulate API/event-style payloads.

Bronze, silver, and gold outputs are stored as Parquet or Delta-style tables.

CSV is avoided as the main format because of schema, typing, and precision weaknesses.

## 5. Bounded Event-Window Analytics

Event-window analytics must avoid naive cross joins.

The preferred design is to assign trading-day indexes per security and join only within bounded event windows.

This prevents accidental Cartesian products and demonstrates scalable time-series thinking.

## 6. AI as Downstream Consumer

The lakehouse remains the source of truth.

The investigation workflow reads from curated silver/gold outputs and uses deterministic templates plus vector retrieval.

It must not replace deterministic reconciliation logic or generate unsupported financial conclusions via LLM output.

## Tradeoffs

## Benefits

This scope is focused, realistic, and interview-defensible.

It allows the project to go deep on:

* schema design
* PySpark transformations
* validation
* deterministic reconciliation logic
* anomaly detection
* financial operations reasoning
* time-series event analytics
* architecture tradeoffs

## Costs

The project initially looks narrower than a full finance AI platform.

It does not immediately demonstrate orchestration, streaming, large-scale document AI, or dashboards.

These are acceptable tradeoffs because the first priority is to build a working, explainable system.

## Scalability Considerations

The MVP runs locally or in a small PySpark/Databricks environment.

The architecture is still designed to scale conceptually:

* raw data lands in bronze
* cleaned entities are normalized in silver
* investigation-ready outputs are produced in gold
* trades can be partitioned by trade_date, account_id, or security_id
* prices can be partitioned by date or security_id
* reconciliation can be computed incrementally by date/account/security
* starting position checkpoints limit cumulative history requirements
* event-window metrics use bounded joins instead of unbounded cross joins

## Latency Considerations

The MVP is batch-first.

This is intentional because position reconciliation and event-window analytics are acceptable as scheduled batch workflows.

Streaming would reduce freshness latency but would add complexity before the core logic is proven.

In production, streaming could be added for intraday trades or live reconciliation alerts.

## AI Boundary

The investigation layer is downstream.

The lakehouse remains the source of truth.

The vector store consumes curated investigation corpus documents built from silver/gold tables.

The workflow may assemble and rank evidence, but it must not replace deterministic reconciliation logic or generate unsupported financial conclusions.

## Production Improvements

Future production improvements may include:

* Databricks Workflows or Airflow orchestration
* incremental processing
* Delta Lake merge/upsert logic
* data quality monitoring
* lineage tracking
* streaming trade ingestion
* managed vector search
* stronger observability
* automated reconciliation alerts
* analyst-facing dashboard

## Final Scope Lock

The MVP is complete when the structured pipeline, manifest validation, cross-engine parity checks, and deterministic investigation workflow all pass under `bash scripts/run_all.sh`.

Do not add broad corporate-action coverage, macro data, full SEC ingestion, LLM generation, Databricks deployment artifacts, Airflow, Kafka, dashboard/UI, or stock prediction beyond the locked MVP scope above.