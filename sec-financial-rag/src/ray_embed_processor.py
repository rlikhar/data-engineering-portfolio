import os
import io
import time
import uuid
import ray
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from minio import Minio
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
from langchain_text_splitters import RecursiveCharacterTextSplitter

# --- PROMETHEUS METRIC IMPORTS ---
from src.metrics import start_metrics_server, CHUNKS_UPSERTED, RAY_PROCESSING_LATENCY, EMBEDDING_TOKENS_CONSUMED

# --- CONFIGURATION ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadminpassword")
BUCKET_NAME = "sec-raw-filings"

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
COLLECTION_NAME = "financial_sec_chunks"

# --- RAY ACTOR FOR WORKER-LEVEL MODEL CACHING ---
@ray.remote(num_cpus=1)
class EmbeddingWorker:
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        print(f"[Ray Worker] Initializing embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=150,
            separators=["\n\n", "\n", " ", ""]
        )

    def process_and_embed_document(self, html_content: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        
        # 1. Clean HTML to raw text
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ")
        
        # 2. Semantic Chunking
        chunks = self.text_splitter.split_text(text)
        if not chunks:
            return {"records": [], "duration": time.time() - start_time, "token_count": 0}

        # 3. Batch Compute Embeddings
        embeddings = self.model.encode(chunks, batch_size=32, show_progress_bar=False).tolist()

        duration = time.time() - start_time
        total_words = sum(len(c.split()) for c in chunks)
        approx_tokens = int(total_words * 1.3)

        # 4. Format vector records
        records = []
        for idx, (chunk_text, vector) in enumerate(zip(chunks, embeddings)):
            record = {
                "id": str(uuid.uuid4()),
                "vector": vector,
                "payload": {
                    "text": chunk_text,
                    "chunk_index": idx,
                    "ticker": metadata["ticker"],
                    "year": metadata["year"],
                    "accession_number": metadata["accession_number"],
                    "form_type": metadata.get("form_type", "10-K")
                }
            }
            records.append(record)

        return {
            "records": records,
            "duration": duration,
            "token_count": approx_tokens
        }

# --- MAIN CONTROLLER ENGINE ---
def run_distributed_embedding_job():
    print("[Ray] Initializing local Ray cluster...")
    ray.init(ignore_reinit_error=True)

    # Initialize MinIO and Qdrant Clients
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, check_compatibility=False)

    # List files from S3 Lakehouse
    objects = list(minio_client.list_objects(BUCKET_NAME, recursive=True))
    if not objects:
        print("No documents found in MinIO bucket!")
        return

    print(f"Found {len(objects)} raw documents in S3 data lake.")

    # Spawn Ray Worker Pool
    num_workers = 1
    workers = [EmbeddingWorker.remote() for _ in range(num_workers)]
    
    futures = []
    doc_metadata_list = []

    for idx, obj in enumerate(objects):
        # Extract partition keys: raw/year=2024/ticker=AAPL/...
        parts = obj.object_name.split("/")
        year = parts[1].split("=")[1]
        ticker = parts[2].split("=")[1]
        accession = parts[3].replace(".html", "")

        metadata = {
            "ticker": ticker,
            "year": year,
            "accession_number": accession,
            "form_type": "10-K"
        }
        doc_metadata_list.append(metadata)

        # Read object from MinIO
        response = minio_client.get_object(BUCKET_NAME, obj.object_name)
        html_content = response.read().decode("utf-8")
        response.close()

        # Dispatch task to Ray Worker Actor
        worker = workers[idx % num_workers]
        future = worker.process_and_embed_document.remote(html_content, metadata)
        futures.append(future)

    # Gather parallel execution results
    print("[Ray] Processing chunks and embeddings concurrently...")
    results = ray.get(futures)

    # Flatten, record metrics, and Upsert into Qdrant Vector DB
    total_vectors = 0
    for res_dict, meta in zip(results, doc_metadata_list):
        doc_records = res_dict["records"]
        duration = res_dict["duration"]
        tokens = res_dict["token_count"]

        # Record Metrics safely on Driver side
        RAY_PROCESSING_LATENCY.labels(ticker=meta["ticker"]).observe(duration)
        EMBEDDING_TOKENS_CONSUMED.labels(model="all-MiniLM-L6-v2").inc(tokens)

        if not doc_records:
            continue
        
        points = [
            models.PointStruct(
                id=r["id"],
                vector=r["vector"],
                payload=r["payload"]
            )
            for r in doc_records
        ]
        
        # Batch upsert to Qdrant
        qdrant_client.upsert(
            collection_name=COLLECTION_NAME,
            points=points
        )
        
        # Record Metric: Chunks upserted count per ticker and year
        CHUNKS_UPSERTED.labels(ticker=meta["ticker"], year=meta["year"]).inc(len(points))
        total_vectors += len(points)

    print(f"\n✅ Pipeline Complete! Upserted {total_vectors} vectors into Qdrant collection '{COLLECTION_NAME}'.")
    ray.shutdown()

if __name__ == "__main__":
    # Start Prometheus metrics server on port 8001
    start_metrics_server(port=8001)

    run_distributed_embedding_job()

    print("\n[Metrics] Processing complete. Keeping Prometheus metrics server alive on port 8001...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping Ray metrics server.")
