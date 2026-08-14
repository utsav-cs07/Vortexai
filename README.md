# VortexAI — Real-Time Streaming RAG Pipeline

[![Run Tests](https://github.com/utsav-cs07/Vortexai/actions/workflows/test.yml/badge.svg)](https://github.com/utsav-cs07/Vortexai/actions/workflows/test.yml)

A real-time, event-driven data pipeline that ingests a live data stream, validates and cleans it through a Medallion architecture, embeds it into a vector database, and guards retrieval results with a **hybrid dense + keyword search groundedness check** — all visible on a live telemetry dashboard.

## Problem Statement

Traditional batch RAG pipelines lag by 12–24 hours and feed dirty, unvalidated data directly into vector databases, with no mechanism to catch bad matches at retrieval time. VortexAI addresses both problems: data is validated *before* it becomes searchable, and every retrieved result is checked for genuine relevance — using both semantic and keyword signals — before being trusted.

## Architecture

```
[Producer: Hacker News API]
        |
        v
[Kafka: raw-events-topic]
        |
        v
[Consumer: Pydantic Validation]
        |
        +--> valid --> [Kafka: validated-events-topic]
        |
        +--> invalid --> [Kafka: dlq-topic]  (Dead Letter Queue)
        |
        v
[Bronze Storage: raw validated events, partitioned Parquet]
        |
        v
[Silver Storage: cleaned, deduplicated, HTML-stripped text]
        |
        v
[Vector Sink: sentence-transformers embeddings --> Qdrant]
        |
        v
[Hybrid Groundedness Guardrail]
    - Dense search (cosine similarity, Qdrant)
    - Keyword search (BM25, rank_bm25)
    - Fused via Reciprocal Rank Fusion
    - Grounded only if BOTH a per-query dynamic threshold
      AND an absolute floor (dense or BM25) are cleared
        |
        v
[Streamlit Dashboard: live telemetry + interactive hybrid query tester]
```

Orchestration (Prefect) wraps the Silver → Qdrant refresh as a scheduled, auto-retrying flow rather than a manually-run script. Kafka, Zookeeper, Qdrant, and the ingestion/validation services run as Docker containers.

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Hacker News public API |
| Message broker | Apache Kafka (Confluent images, Docker) |
| Validation | Pydantic v2 |
| Storage (Bronze/Silver) | Parquet, Pandas, PyArrow |
| Embeddings | sentence-transformers (`all-MiniLM-L6-v2`) |
| Vector database | Qdrant |
| Keyword search | BM25 (`rank_bm25`) |
| Retrieval fusion | Reciprocal Rank Fusion (RRF) |
| Orchestration | Prefect |
| Dashboard | Streamlit |
| Logging | Structured JSON (custom formatter) |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Containerization | Docker, Docker Compose |

## Key Features

- **Real-time ingestion** from a live public API, not a static/batch dataset — plus a concurrent backfill utility to bootstrap a meaningful dataset instantly
- **Schema validation with Dead Letter Queue routing** — invalid events are quarantined with a reason, never silently dropped; DLQ routing verified live with a deliberately malformed test message
- **Medallion architecture** (Bronze → Silver) separating immutable raw storage from cleaned, embedding-ready data
- **Idempotent vector sync** — deterministic content-hash-based point IDs mean re-running the sync can never create duplicate vectors; incremental sync only embeds genuinely new rows
- **Hybrid retrieval groundedness guardrail** — combines dense vector similarity (semantic meaning) with BM25 keyword scoring (exact term overlap) via Reciprocal Rank Fusion, so a query is caught by whichever method actually works for it (e.g. a bare-name query with weak semantic signal but a strong keyword match)
- **Per-query dynamic thresholding** — the groundedness cutoff adapts to each query's own score distribution (mean + 0.5×stdev), combined with a calibrated absolute floor so a query with genuinely no good matches is still correctly rejected in full
- **Live dashboard** — Kafka topic depths, storage row counts, vector sync status, and an interactive hybrid query tester showing Dense/BM25/Fused scores side by side, auto-refreshing every 5 seconds
- **Automated orchestration** — Prefect schedules and retries the Silver/Qdrant refresh pipeline, with full run history visible in a local dashboard
- **Automated testing + CI** — pytest suite covering validation rules and text-cleaning edge cases (including real production bugs like NaN-valued fields), run automatically on every push via GitHub Actions
- **Structured JSON logging** across every service, for real log-aggregation compatibility
- **Containerized infrastructure and ingestion services** via Docker Compose, with environment-variable-driven configuration so the same code runs identically locally or in containers

## Setup

### Prerequisites
- Docker Desktop
- Python 3.11+

### 1. Start infrastructure and containerized services
```bash
docker compose up -d
```
Starts Kafka, Zookeeper, Qdrant, and the containerized producer/consumer/bronze-writer services. Verify Qdrant is up at `http://localhost:6333/dashboard`.

### 2. Install dependencies (for running Silver/Qdrant/dashboard/orchestration locally)
```bash
pip install -r requirements.txt
```

### 3. Bootstrap a dataset
```bash
python producer/backfill_hackernews.py
```

### 4. Build Silver and sync to Qdrant
```bash
python storage/silver_transform.py
python vector_sink/qdrant_sync.py
```
Or run both as an orchestrated Prefect flow:
```bash
python orchestration/vortex_flow.py
```
For a continuously scheduled refresh (every 5 minutes):
```bash
python orchestration/vortex_flow.py serve
```

### 5. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

## Testing

```bash
pytest tests/ -v
```
Runs entirely in isolation — no Docker, Kafka, or Qdrant required. Covers Pydantic validation rules and Silver's text-cleaning functions, including edge cases actually encountered in production (e.g. NaN-valued fields from Parquet round-trips).

## Project Structure

```
├── producer/              # Data ingestion (live poll + backfill)
├── consumer/              # Validation + DLQ routing
├── schemas.py             # Pydantic data models (dependency-free)
├── logging_config.py      # Structured JSON logging, shared across all services
├── storage/                # Bronze writer, Silver transform, compaction
├── vector_sink/           # Qdrant sync, hybrid search + groundedness, cleanup
├── dashboard/              # Streamlit telemetry UI
├── orchestration/          # Prefect flow definitions
├── tests/                  # pytest suite
├── .github/workflows/      # CI pipeline
├── Dockerfile.light        # Kafka-only services (no ML dependencies)
├── Dockerfile.heavy        # Services requiring embeddings (orchestrator/dashboard, run locally)
└── docker-compose.yml      # Full infrastructure + containerized ingestion services
```

## Future Work

- Full AWS Free Tier migration (SQS, Lambda, S3, EC2) — architecture scoped, implementation in progress
- Containerize the dashboard and orchestrator services (currently run locally to manage disk footprint on constrained development machines)
- Expand hybrid search calibration with a larger, more diverse dataset