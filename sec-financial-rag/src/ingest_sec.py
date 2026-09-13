import os
import io
import json
import time
import requests
from datetime import datetime
from minio import Minio
from kafka import KafkaProducer
from pydantic import BaseModel, Field

# --- PROMETHEUS METRICS IMPORTS ---
from src.metrics import start_metrics_server, DOCUMENTS_INGESTED

# --- CONFIGURATION ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadminpassword")
BUCKET_NAME = "sec-raw-filings"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:19092")
KAFKA_TOPIC = "raw-document-events"

# SEC User Agent requirement (Replace with your info)
USER_AGENT = os.getenv("SEC_USER_AGENT", "FinancialDataEngineer candidate@portfolio.com")

# --- PYDANTIC EVENT SCHEMA ---
class SECDocumentEvent(BaseModel):
    ticker: str
    cik: str
    form_type: str
    filing_date: str
    accession_number: str
    s3_url: str
    ingested_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

# --- INITIALIZE CLIENTS ---
def get_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Created MinIO bucket: '{BUCKET_NAME}'")
    return client

def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# --- SEC EDGAR DOWNLOADER ---
def fetch_latest_10k_metadata(ticker: str):
    """Fetch recent 10-K filings metadata from SEC API."""
    headers = {"User-Agent": USER_AGENT}

    # 1. Map ticker to CIK
    ticker_res = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers)
    ticker_res.raise_for_status()
    data = ticker_res.json()

    cik = None
    for entry in data.values():
        if entry["ticker"].upper() == ticker.upper():
            cik = str(entry["cik_str"]).zfill(10)
            break

    if not cik:
        raise ValueError(f"Ticker {ticker} not found in SEC database.")

    # 2. Query Submissions API for 10-K filings
    sub_res = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=headers)
    sub_res.raise_for_status()
    recent = sub_res.json()["filings"]["recent"]

    forms = recent["form"]
    accession_numbers = recent["accessionNumber"]
    filing_dates = recent["filingDate"]
    primary_docs = recent["primaryDocument"]

    # Extract recent 10-K filing
    for i, form in enumerate(forms):
        if form == "10-K":
            acc_num = accession_numbers[i].replace("-", "")
            doc_name = primary_docs[i]
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_num}/{doc_name}"
            return {
                "ticker": ticker.upper(),
                "cik": cik,
                "form_type": "10-K",
                "filing_date": filing_dates[i],
                "accession_number": accession_numbers[i],
                "doc_url": doc_url,
                "file_name": doc_name
            }
    return None

# --- MAIN INGESTION PIPELINE ---
def process_ticker_ingestion(ticker: str, minio_client: Minio, kafka_producer: KafkaProducer):
    print(f"\n[Ingest] Querying SEC EDGAR for {ticker}...")
    meta = fetch_latest_10k_metadata(ticker)

    if not meta:
        print(f"No recent 10-K found for {ticker}")
        return

    # Fetch document content
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(meta["doc_url"], headers=headers)
    response.raise_for_status()
    doc_bytes = response.content

    # Partitioned S3 Key: raw/year=YYYY/ticker=TICKER/accession.html
    year = meta["filing_date"].split("-")[0]
    s3_key = f"raw/year={year}/ticker={meta['ticker']}/{meta['accession_number']}.html"

    try:
        # Upload to MinIO S3
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=s3_key,
            data=io.BytesIO(doc_bytes),
            length=len(doc_bytes),
            content_type="text/html"
        )
        s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
        print(f"[MinIO] Successfully uploaded: {s3_uri}")

        # Create & publish event message to Redpanda (Kafka)
        event = SECDocumentEvent(
            ticker=meta["ticker"],
            cik=meta["cik"],
            form_type=meta["form_type"],
            filing_date=meta["filing_date"],
            accession_number=meta["accession_number"],
            s3_url=s3_uri
        )

        kafka_producer.send(KAFKA_TOPIC, value=event.model_dump())
        kafka_producer.flush()
        print(f"[Redpanda] Published event to '{KAFKA_TOPIC}' for {ticker}")

        # --- PROMETHEUS METRIC: SUCCESS ---
        DOCUMENTS_INGESTED.labels(
            ticker=meta["ticker"],
            form_type=meta["form_type"],
            status="success"
        ).inc()

    except Exception as e:
        # --- PROMETHEUS METRIC: FAILURE ---
        DOCUMENTS_INGESTED.labels(
            ticker=ticker,
            form_type="10-K",
            status="failure"
        ).inc()
        print(f"Failed to process/ingest {ticker}: {e}")
        raise e

if __name__ == "__main__":
    # Start Prometheus server on port 8000
    start_metrics_server(port=8000)

    minio_client = get_minio_client()
    producer = get_kafka_producer()

    target_tickers = ["AAPL", "NVDA", "MSFT", "AMZN"]

    for ticker in target_tickers:
        try:
            process_ticker_ingestion(ticker, minio_client, producer)
            time.sleep(1)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    print("\n[Metrics] Ingestion complete. Keeping Prometheus metrics server alive on port 8000...")
    # Keep process running so Prometheus can scrape metrics
    try:
        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        print("Stopping metrics server.")
