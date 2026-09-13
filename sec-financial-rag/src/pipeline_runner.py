import time
import os
from src.metrics import start_metrics_server
from src.ingest_sec import process_ticker_ingestion, get_minio_client, get_kafka_producer
from src.ray_embed_processor import run_distributed_embedding_job

def run_full_pipeline():
    print("🚀 [Master Pipeline] Starting Prometheus Metrics Server on Port 8000...")
    # Single unified server for all pipeline metrics
    start_metrics_server(port=8000)

    # ----------------------------------------------------
    # STAGE 1: SEC Document Ingestion to S3 / Kafka
    # ----------------------------------------------------
    print("\n--- STAGE 1: Executing SEC Ingestion Job ---")
    minio_client = get_minio_client()
    producer = get_kafka_producer()
    target_tickers = ["AAPL", "NVDA", "MSFT", "AMZN"]

    for ticker in target_tickers:
        try:
            process_ticker_ingestion(ticker, minio_client, producer)
            time.sleep(1)
        except Exception as e:
            print(f"Error processing ingestion for {ticker}: {e}")

    print("✅ [STAGE 1 COMPLETE] SEC Filings downloaded and pushed to MinIO.")

    # ----------------------------------------------------
    # STAGE 2: Distributed Chunking & Vector Upserts via Ray
    # ----------------------------------------------------
    print("\n--- STAGE 2: Executing Ray Embedding & Qdrant Upsert Job ---")
    try:
        run_distributed_embedding_job()
    except Exception as e:
        print(f"Error executing Ray distributed job: {e}")

    print("\n🎉 [Pipeline Complete] All ingestion and vector embedding tasks finished successfully.")
    print("📊 Metrics server remaining active at http://localhost:8000/metrics (Press Ctrl+C to stop)")

    # Keep master process alive so Prometheus can continuously scrape metrics
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Master Pipeline metrics server.")

if __name__ == "__main__":
    run_full_pipeline()
