# Financial Fraud Detection

## Overview
This project implements an end-to-end fraud detection system with:

- Offline training and promotion pipeline jobs
- Redis-backed feature store with warm start and atomic updates
- Offline ↔ online parity checks

The goal is to demonstrate **production-style ML serving**, not just model training.

## Architecture
    Bronze (ingest transactions)
    ↓
    Silver (cleaned + validated)
    ↓
    Gold (engineered features)
    ↓
    Model Training (offline)
    ↓
    Redis Feature Store (online)
    ↓
    Real-time Scoring + Explanations

**Key properties**
- Offline and Online pipelines share the same feature definitions
- Online features are updated atomically per transaction
- Parity tests ensure training == serving

## Demo
The demo uses **Streamlit** to simulate:
- A warm-start phase
- Real-time streamed transactions
- Per-transaction atomic feature store updates

### Run Demo
1. make install

2. make redis-up

3. make demo

Parity test and job commands in makefile.

## Feature Store

This feature store implements rolling window aggregates using a ring-buffer strategy to ensure an up-to-date state.

## Entities

The system models two entities: origin and destination.

EDA showed that:

    -Origin entities have a low rate of repeated interactions

    -Destination entities exhibit strong repeat behavior

Given the cost of maintaining state, only destination entities are stored in the feature store, providing higher signal-to-cost efficiency.

## Design Goal

- Offline-Online parity 
- Feature-Store backed serving
- Explainable predictions 
- Reproducible pipelines and jobs

## Notes

This project is intended as a portfolio-grade ML engineering demo, not a production deployment.

## Roadmap

Planned extensions to evolve the system toward a full production workflow:

- Workflow orchestration (Dagster)
- Pipeline composition and scheduling
- Batch reporting and backfills
- Redis TTL and state lifecycle management
- Extended multi-iteration demo