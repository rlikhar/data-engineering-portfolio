# 🔹 CSV to SQLite ETL Pipeline (Airflow Basic Project)

## 📌 Overview

This project demonstrates a **basic ETL pipeline using Apache Airflow**, designed to showcase foundational concepts such as task orchestration, data ingestion, transformation, and loading.

The pipeline processes a CSV dataset (Titanic dataset) and stores cleaned data into a SQLite database.

---

# 🎯 Objective

To build a simple, automated ETL workflow that:

* Downloads data from an external source
* Cleans and transforms the dataset
* Loads processed data into a database

---

# ⚙️ Pipeline Workflow

```text
Download CSV → Clean Data → Load to SQLite
```

---

# 🏗️ Tech Stack

* Apache Airflow
* Python
* Pandas
* SQLite
* BashOperator
* PythonOperator

---

# 📂 Project Structure

```bash
airflow-basic/
│
├── dags/
│   └── csv_to_sqlite_dag.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── db/
│   └── titanic.db
│
└── README.md
```

---

# 🔄 DAG Details

## 1. Data Ingestion (BashOperator)

* Downloads Titanic dataset using `wget`

## 2. Data Transformation (PythonOperator)

* Handles missing values
* Drops irrelevant columns
* Encodes categorical variables

## 3. Data Loading (PythonOperator)

* Loads cleaned dataset into SQLite database

---

# 📊 Output

* Cleaned CSV file
* SQLite database table: `titanic_data`

---

# 🚀 How to Run

### 1. Start Airflow

```bash
airflow standalone
```

### 2. Place DAG

```bash
cp dags/csv_to_sqlite_dag.py ~/airflow/dags/
```

### 3. Trigger DAG

* Open: http://localhost:8080
* Run `csv_to_sqlite_etl`

---

# ⚠️ Notes

* SQLite is used for development purposes only
* Ensure required dependencies:

```bash
pip install pandas
```

---

# 🧠 Key Learnings

* Understanding Airflow DAG structure
* Task orchestration and dependencies
* File-based ETL pipelines
* Basic data cleaning techniques

---

# 📈 Future Improvements

* Add data validation step
* Use PostgreSQL instead of SQLite
* Add logging and error handling
* Introduce scheduling

---

# ⭐ Summary

This project provides a strong foundation in:

* Airflow fundamentals
* ETL pipeline design
* Data processing using Python

Ideal as a starting point for more advanced data engineering workflows.
