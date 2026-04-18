import requests
import time
import json
from datetime import datetime
import os

# 🔐 Add your credentials here
APP_ID = "5cef8898"
APP_KEY = "01bc25e33fbf3aedf3a57d24525570d1"

BASE_URL = "https://api.adzuna.com/v1/api/jobs/in/search/1"


def is_relevant_job(title):
    keywords = [
        "data engineer",
        "data analyst",
        "data scientist",
        "machine learning",
        "etl",
        "big data",
        "analytics"
    ]

    title = str(title).lower()

    return any(keyword in title for keyword in keywords)


def fetch_jobs(keyword="data engineer", location="india"):
    print(f"Fetching jobs for: {keyword}")

    time.sleep(1)

    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 50,
        "what": keyword,
        "where": location,
        "content-type": "application/json"
    }

    response = requests.get(BASE_URL, params=params)

    print("Status Code:", response.status_code)

    if response.status_code != 200:
        print("❌ Failed to fetch data")
        print(response.text)
        return []

    data = response.json().get("results", [])

    jobs = []

    for job in data:
        try:
            title = job.get("title")

            # 🔥 Filter relevant jobs
            if not is_relevant_job(title):
                continue

            jobs.append({
                "job_title": title,
                "company": job.get("company", {}).get("display_name"),
                "location": job.get("location", {}).get("display_name"),
                "description": job.get("description"),
                "salary_min": job.get("salary_min"),
                "salary_max": job.get("salary_max"),
                "redirect_url": job.get("redirect_url"),
                "source": "Adzuna API",
                "scraped_at": str(datetime.now())
            })

        except Exception as e:
            print("Error parsing job:", e)

    print(f"✅ Total jobs extracted: {len(jobs)}")

    return jobs


def save_raw_data(jobs):
    os.makedirs("data/raw", exist_ok=True)

    filename = f"data/raw/jobs_{int(time.time())}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=4)

    print(f"Saved raw data to {filename}")


if __name__ == "__main__":
    # 🔥 Try multiple keywords (IMPORTANT)
    all_jobs = []

    keywords = [
        "data engineer",
        "data analyst",
        "python etl",
        "big data engineer"
    ]

    for kw in keywords:
        jobs = fetch_jobs(keyword=kw)
        all_jobs.extend(jobs)

    print(f"\n✅ Total combined jobs: {len(all_jobs)}")

    save_raw_data(all_jobs)