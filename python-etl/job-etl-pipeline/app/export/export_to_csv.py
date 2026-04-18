from pymongo import MongoClient
import pandas as pd
from app.config.config import MONGO_URI, DB_NAME, COLLECTION_NAME


def export_to_csv():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    data = list(collection.find({}, {"_id": 0}))

    df = pd.DataFrame(data)

    df.to_csv("data/processed/jobs_output.csv", index=False)

    print("✅ Data exported to CSV")


if __name__ == "__main__":
    export_to_csv()