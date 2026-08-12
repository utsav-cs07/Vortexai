# VortexAI — Real-Time Streaming RAG Pipeline
![CI Status](https://github.com/utsav-cs07/Vortexai/actions/workflows/test.yml/badge.svg)
A real-time, event-driven data pipeline that ingests a live data stream, validates and cleans it through a Medallion architecture, embeds it into a vector database, and guards retrieval results with a cosine-similarity groundedness check — all visible on a live telemetry dashboard.

## Problem Statement

Traditional batch RAG pipelines lag by 12–24 hours and feed dirty, unvalidated data directly into vector databases, with no mechanism to catch bad matches at retrieval time. VortexAI addresses both problems: data is validated *before* it becomes searchable, and every retrieved result is checked for genuine semantic relevance before being trusted.

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
[Groundedness Guardrail: cosine similarity threshold check]
        |
        v
[Streamlit Dashboard: live telemetry + interactive query tester]
```

Orchestration (Prefect) wraps the Silver → Qdrant refresh as a scheduled, auto-retrying flow rather than a manually-run script.

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Hacker News public API |
| Message broker | Apache Kafka (Confluent images, Docker) |
| Validation | Pydantic v2 |
| Storage (Bronze/Silver) | Parquet, Pandas, PyArrow |
| Embeddings | sentence-transformers (`all-MiniLM-L6-v2`) |
| Vector database | Qdrant |
| Orchestration | Prefect |
| Dashboard | Streamlit |
| Testing | pytest |
| CI/CD | GitHub Actions |

## Key Features

- **Real-time ingestion** from a live public API, not a static/batch dataset
- **Schema validation with Dead Letter Queue routing** — invalid events are quarantined with a reason, never silently dropped or allowed to corrupt downstream data
- **Medallion architecture** (Bronze → Silver) separating immutable raw storage from cleaned, embedding-ready data
- **Idempotent vector sync** — deterministic content-hash-based point IDs mean re-running the sync can never create duplicate vectors
- **Groundedness guardrail** — every retrieval result is scored by cosine similarity against a calibrated threshold, and explicitly labeled trustworthy or not, rather than blindly returned
- **Live dashboard** — Kafka topic depths, storage row counts, vector sync status, and an interactive query tester, auto-refreshing every 5 seconds
- **Automated orchestration** — Prefect schedules and retries the Silver/Qdrant refresh pipeline, with full run history
- **Automated testing + CI** — pytest suite covering validation rules and text-cleaning edge cases, run automatically on every push via GitHub Actions

## Setup

### Prerequisites
- Docker Desktop
- Python 3.11+

### 1. Start infrastructure
```bash
docker compose up -d
```
Starts Kafka, Zookeeper, and Qdrant. Verify Qdrant is up at `http://localhost:6333/dashboard`.

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline
In separate terminals:
```bash
python producer/hackernews_producer.py
python consumer/validated_consumer.py
python storage/bronze_writer.py
```

Optionally bootstrap a larger initial dataset:
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

### 5. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

## Testing

```bash
pytest tests/ -v
```
Runs entirely in isolation — no Docker, Kafka, or Qdrant required. Covers Pydantic validation rules and Silver's text-cleaning functions, including edge cases like malformed/missing data encountered in production.

## Project Structure

```
├── producer/            # Data ingestion (live poll + backfill)
├── consumer/            # Validation + DLQ routing
├── schemas.py           # Pydantic data models (dependency-free)
├── storage/              # Bronze writer, Silver transform, compaction
├── vector_sink/         # Qdrant sync, groundedness guardrail, cleanup
├── dashboard/            # Streamlit telemetry UI
├── orchestration/        # Prefect flow definitions
├── tests/                # pytest suite
└── .github/workflows/    # CI pipeline
```

## Future Work

- Hybrid search combining dense vector similarity with BM25 keyword scoring
- Cloud deployment on AWS Free Tier (SQS, Lambda, S3, EC2)
- Dynamic, empirically-calibrated groundedness thresholds
- Containerized Python services for one-command startup