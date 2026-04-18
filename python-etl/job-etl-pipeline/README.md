# Job Market Analytics ETL Pipeline

A production-style ETL pipeline that ingests job market data, transforms and enriches it using NLP, and enables analytics through Power BI.

## 🚀 Overview

This project demonstrates an end-to-end data engineering workflow:
- Extract job listings via API (Adzuna)
- Clean and standardize raw data
- Apply NLP for skill extraction
- Store structured data in MongoDB Atlas
- Visualize insights using Power BI

## 🧠 Key Highlights

- API-driven data ingestion (reliable over web scraping)
- Modular ETL architecture (extract → transform → load)
- NLP-based skill extraction from job descriptions
- Cloud storage using MongoDB Atlas
- Analytics-ready dataset for BI tools

## 🏗️ Architecture

```mermaid
flowchart LR
    A[Adzuna API] --> B[ETL Pipeline]
    B --> C[Data Cleaning]
    C --> D[NLP Skill Extraction]
    D --> E[MongoDB Atlas]
    E --> F[Power BI Dashboard]
```

## ⚙️ Tech Stack

- Python (Requests, Pandas, PyMongo)
- MongoDB Atlas
- Power BI
- Rule-based NLP

## 📊 Insights Enabled

- In-demand skills (Python, SQL, AWS, etc.)
- Job role distribution (Data Engineer, Analyst, etc.)
- Seniority trends
- Company hiring patterns

## 💡 What This Project Demonstrates

- End-to-end ETL pipeline design
- API integration and data ingestion
- Data cleaning and transformation
- Feature engineering using NLP
- Cloud data storage and analytics preparation

## 🔗 Usage

Run the full pipeline:

```bash
python -m app.main
```

## 👨‍💻 Author

Rahul Likhar
