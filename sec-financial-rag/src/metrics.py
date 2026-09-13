import time
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# --- PROMETHEUS METRIC DEFINITIONS ---

# 1. Counter: Total documents processed by ticker and status
DOCUMENTS_INGESTED = Counter(
    'rag_documents_ingested_total',
    'Total raw SEC EDGAR documents ingested',
    ['ticker', 'form_type', 'status']
)

# 2. Counter: Total chunks embedded and upserted
CHUNKS_UPSERTED = Counter(
    'rag_chunks_upserted_total',
    'Total vector chunks upserted into Qdrant',
    ['ticker', 'year']
)

# 3. Histogram: Latency of Ray chunking and embedding generation
RAY_PROCESSING_LATENCY = Histogram(
    'rag_ray_processing_duration_seconds',
    'Time spent parsing, chunking, and generating embeddings in Ray',
    ['ticker'],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
)

# 4. Gauge: Active Kafka queue / message lag
KAFKA_MESSAGE_LAG = Gauge(
    'rag_kafka_message_lag',
    'Current estimated message lag in Kafka/Redpanda ingestion topic'
)

# 5. Counter: Estimated embedding token consumption & API cost tracker
EMBEDDING_TOKENS_CONSUMED = Counter(
    'rag_embedding_tokens_total',
    'Estimated embedding tokens processed for cost calculation',
    ['model']
)

def start_metrics_server(port: int = 8000):
    """Starts a standalone Prometheus HTTP metrics endpoint on specified port."""
    start_http_server(port)
    print(f"📊 [Prometheus] Metrics server exposed at http://localhost:{port}/metrics")
