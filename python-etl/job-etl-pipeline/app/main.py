from app.extract.scraper import fetch_jobs, save_raw_data
from app.transform.cleaner import process_jobs
from app.transform.nlp_processor import process_nlp
from app.load.mongo_loader import load_to_mongo


def run_pipeline():
    print("🚀 Starting ETL Pipeline...")

    # Step 1: Extract
    jobs = fetch_jobs()
    save_raw_data(jobs)

    # Step 2: Transform
    process_jobs()

    # Step 3: NLP
    process_nlp()

    # Step 4: Load
    load_to_mongo()

    print("✅ Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()