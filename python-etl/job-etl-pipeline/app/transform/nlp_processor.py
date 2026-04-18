import json
import os
import re

INPUT_FILE = "data/processed/cleaned_jobs.json"
OUTPUT_FILE = "data/processed/final_jobs.json"


# 🔥 Master skill list (expandable)
SKILL_KEYWORDS = [
    "python", "sql", "aws", "azure", "gcp",
    "spark", "hadoop", "airflow", "etl",
    "pandas", "numpy", "power bi", "tableau",
    "docker", "kubernetes", "snowflake",
    "mongodb", "postgresql", "mysql",
    "machine learning", "deep learning"
]


def extract_skills(text):
    if not text:
        return []

    text = text.lower()

    found_skills = []

    for skill in SKILL_KEYWORDS:
        # exact word match
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, text):
            found_skills.append(skill)

    return list(set(found_skills))  # remove duplicates


def process_nlp():
    if not os.path.exists(INPUT_FILE):
        raise Exception("❌ Cleaned data not found. Run Step 5 first.")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    final_jobs = []

    for job in jobs:
        try:
            description = job.get("description", "")

            extracted_skills = extract_skills(description)

            job["skills_extracted"] = extracted_skills

            final_jobs.append(job)

        except Exception as e:
            print("Error in NLP processing:", e)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_jobs, f, indent=4)

    print(f"✅ NLP processed {len(final_jobs)} jobs")
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    process_nlp()