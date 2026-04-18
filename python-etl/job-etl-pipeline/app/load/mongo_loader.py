import json
from pymongo import MongoClient
from app.config.config import MONGO_URI, DB_NAME, COLLECTION_NAME


INPUT_FILE = "data/processed/final_jobs.json"


def load_to_mongo():
    print("Connecting to MongoDB...")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print("Connected to MongoDB")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    if not jobs:
        print("❌ No data to insert")
        return

    # Optional: clear old data
    collection.delete_many({})

    result = collection.insert_many(jobs)

    print(f"✅ Inserted {len(result.inserted_ids)} documents into MongoDB")


if __name__ == "__main__":
    load_to_mongo()