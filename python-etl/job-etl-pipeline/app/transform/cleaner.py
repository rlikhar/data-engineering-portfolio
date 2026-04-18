import re
import json
import os


RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"


def get_latest_file():
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".json")]

    if not files:
        raise Exception("No raw files found")

    files.sort(reverse=True)
    return os.path.join(RAW_DIR, files[0])


def clean_text(text):
    if not text:
        return ""

    # remove HTML tags
    text = re.sub(r"<.*?>", "", text)

    # remove special characters
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)

    # normalize spaces
    text = re.sub(r"\s+", " ", text)

    return text.strip().lower()


def extract_seniority(title):
    title = title.lower()

    if "senior" in title:
        return "senior"
    elif "junior" in title:
        return "junior"
    elif "lead" in title:
        return "lead"
    else:
        return "mid"


def extract_role(title):
    title = title.lower()

    if "data engineer" in title:
        return "data engineer"
    elif "data analyst" in title:
        return "data analyst"
    elif "data scientist" in title:
        return "data scientist"
    elif "machine learning" in title:
        return "ml engineer"
    else:
        return "other"


def process_jobs():
    input_file = get_latest_file()
    print(f"Processing file: {input_file}")

    with open(input_file, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    processed_jobs = []
    seen = set()

    for job in jobs:
        try:
            title = clean_text(job.get("job_title"))
            company = clean_text(job.get("company"))
            location = clean_text(job.get("location"))
            description = clean_text(job.get("description"))

            salary_min = job.get("salary_min")
            salary_max = job.get("salary_max")

            # deduplication key
            key = (title, company)

            if key in seen:
                continue
            seen.add(key)

            processed_jobs.append({
                "job_title": title,
                "company": company,
                "location": location,
                "description": description,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "seniority": extract_seniority(title),
                "role_category": extract_role(title),
                "source": job.get("source"),
                "scraped_at": job.get("scraped_at")
            })

        except Exception as e:
            print("Error processing job:", e)

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    output_file = os.path.join(PROCESSED_DIR, "cleaned_jobs.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(processed_jobs, f, indent=4)

    print(f"✅ Processed {len(processed_jobs)} jobs")
    print(f"Saved to {output_file}")


if __name__ == "__main__":
    process_jobs()