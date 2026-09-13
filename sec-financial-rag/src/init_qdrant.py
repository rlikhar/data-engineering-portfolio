import os
from qdrant_client import QdrantClient
from qdrant_client.http import models

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
COLLECTION_NAME = "financial_sec_chunks"

def initialize_qdrant():
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    # 1. Create or recreate collection
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME in collections:
        print(f"Collection '{COLLECTION_NAME}' already exists. Skipping creation.")
    else:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=384,  # Dimension for 'all-MiniLM-L6-v2'
                distance=models.Distance.COSINE
            ),
            # HNSW Index config for fast approximate nearest neighbor search
            hnsw_config=models.HnswConfigDiff(
                m=16,
                ef_construct=100
            )
        )
        print(f"Created Qdrant collection: '{COLLECTION_NAME}' (dim=384, Cosine)")

    # 2. Create Payload Indexing for Hybrid Filtering
    payload_fields = ["ticker", "year", "form_type", "accession_number"]
    for field in payload_fields:
        client.create_payload_index(
            collection_name=COLLECTION_NAME,
            field_name=field,
            field_schema=models.PayloadSchemaType.KEYWORD
        )
        print(f"Indexed payload field: '{field}'")

if __name__ == "__main__":
    initialize_qdrant()
