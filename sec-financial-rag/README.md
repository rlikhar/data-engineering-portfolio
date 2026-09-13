# Enterprise Financial Intelligence & Compliance RAG Engine

---

## 📌 Business Problem Statement

Investment firms and compliance teams struggle to manually analyze thousands of unstructured **SEC 10-K and 10-Q financial filings** to extract critical risk factors, material changes, and debt commitments.

* **Traditional Keyword Search (BM25):** Misses semantic context, synonym variations, and complex contextual financial clauses.
* **Naive LLM Architectures:** Frequently hallucinate financial figures, hit strict context-window limits, and fail when processing large multi-page tables.

### The Solution

This project implements a production-grade **Unstructured Financial Document RAG & Data Platform**. The engine streams public SEC EDGAR filings, extracts financial text and tabular data using **distributed Ray batch processing**, generates dense embeddings, indexes vectors into **Qdrant** with hybrid metadata filtering, and monitors ingestion performance and metrics via **Prometheus & Grafana**.

---

## 🏗️ System Architecture

```
                                [ SEC EDGAR API ]
                                        │
                                        ▼
                            [ Ingestion Engine (Python) ]
                                        │
           ┌────────────────────────────┴────────────────────────────┐
           ▼                                                         ▼
[ MinIO S3 Lakehouse ]                                    [ Redpanda Kafka Topic ]
 (Raw Partitioned HTML)                                    (raw-document-events)
                                                                     │
                                                                     ▼
                                                       [ Ray Distributed Processing ]
                                                       (Parallel Chunking & Embedding)
                                                                     │
                                                                     ▼
                                                           [ Qdrant Vector DB ]
                                                        (Hybrid Search & Metadata)
                                                                     │
                                                                     ▼
                                                         [ Prometheus & Grafana ]
                                                        (Observability & Metrics)

```

---

## ✨ Key Technical Features & Engineering Patterns

* **Partitioned Data Lake Storage:** Ingests raw financial filings directly from SEC EDGAR into an S3-compatible MinIO data lake using clean hive-partition keys (`raw/year=YYYY/ticker=SYMBOL/`).
* **Event-Driven Ingestion:** Uses Redpanda (Kafka protocol) to publish streaming document metadata with strict Pydantic schema validation.
* **Distributed Processing with Ray:** Accelerates CPU/GPU intensive document parsing, semantic chunking, and batch embedding extraction using Ray tasks.
* **Hybrid Vector Search:** Structured data modeling in Qdrant with dense vectors combined with rich metadata payloads (`ticker`, `filing_date`, `accession_number`) to prevent context dilution.
* **Full Observability:** System-level metrics export tracking queue lag, vector indexing performance, token cost estimates, and API latencies in Prometheus and Grafana.

---

## 🚀 Quickstart Guide

### Prerequisites

* **OS:** Linux or Windows WSL2 (Ubuntu 22.04+)
* **Engine:** Docker Desktop with WSL2 Integration enabled
* **Runtime:** Python 3.11+ (recommended: [`uv`](https://github.com/astral-sh/uv) for fast package resolution)

---

### 1. Clone Repository & Start Services

```bash
# Clone project repo
git clone https://github.com/YOUR_GITHUB_USERNAME/financial-rag-data-engine.git
cd financial-rag-data-engine

# Spin up local infrastructure (Kafka, Qdrant, MinIO, Prometheus, Grafana)
docker compose up -d

```

---

### 2. Configure Environment

Create your `.env` file based on the provided template:

```bash
cp .env.example .env

```

> ⚠️ **Note on SEC EDGAR API Policy:** Update `SEC_USER_AGENT` inside your `.env` file with your custom name and email to comply with SEC request header rate limits.

---

### 3. Initialize Virtual Environment & Run Pipeline

```bash
# Setup virtual environment using uv
uv venv .venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt

# Run the SEC raw ingestion pipeline
python -m src.ingest_sec

```

---

## 📊 Infrastructure Dashboards

Once services are running, access the user interfaces via your browser:

| Component | Platform | URL | Default Credentials |
| --- | --- | --- | --- |
| **Kafka Admin UI** | Redpanda Console | `http://localhost:8080` | *None* |
| **Vector Index UI** | Qdrant Dashboard | `http://localhost:6333/dashboard` | *None* |
| **S3 Data Lake UI** | MinIO Console | `http://localhost:9001` | `minioadmin` / `minioadminpassword` |
| **Monitoring System** | Grafana Dashboards | `http://localhost:3000` | `admin` / `admin` |

---

## 📁 Repository Structure

```text
financial-rag-data-engine/
├── config/
│   └── prometheus.yml          # Prometheus scrape targets
├── src/
│   ├── __init__.py
│   └── ingest_sec.py           # Ingestion producer & MinIO/Kafka writer
├── .env.example                # Safe environment variable blueprint
├── .gitignore                  # Git exclusion rule definitions
├── docker-compose.yml          # Infrastructure container specification
├── README.md                   # System documentation
└── requirements.txt            # Locked Python dependencies

```

---

## 📜 License

This project is open-source software licensed under the [MIT License](https://www.google.com/search?q=LICENSE).
