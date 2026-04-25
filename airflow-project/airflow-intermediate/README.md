# 🔹 Employee Attendance & Productivity Pipeline (Airflow Intermediate Project)

## 📌 Overview

This project implements an **intermediate-level ETL pipeline using Apache Airflow** to process employee attendance data and generate productivity insights.

The pipeline demonstrates **real-world data engineering practices**, including validation, transformation, KPI generation, and database loading.

---

# 🎯 Problem Statement

Organizations need automated systems to track employee attendance and productivity. Manual tracking is inefficient, error-prone, and lacks scalability.

---

# 🎯 Objective

Build a data pipeline that:

* Ingests employee attendance data
* Validates data quality
* Transforms raw logs into structured data
* Generates business KPIs
* Stores results for analytics

---

# ⚙️ Pipeline Workflow

```text
Validate Data → Transform Data → Generate KPIs → Load to Database
```

---

# 🏗️ Tech Stack

* Apache Airflow
* Python
* Pandas
* SQLite
* PythonOperator

---

# 📂 Project Structure

```bash
airflow-intermediate/
│
├── dags/
│   └── attendance_pipeline.py
│
├── scripts/
│   ├── validate.py
│   ├── transform.py
│   ├── metrics.py
│   ├── load.py
│
├── data/
│   ├── raw/
│   ├── processed/
│
├── db/
│   └── attendance.db
│
└── README.md
```

---

# 🔄 DAG Details

## 1. Data Validation

* Checks for missing values
* Validates timestamps
* Ensures data consistency

## 2. Data Transformation

* Converts time formats
* Calculates working hours
* Handles null values

## 3. KPI Generation

* Average working hours per employee
* Late arrival count
* Absenteeism tracking

## 4. Data Loading

* Stores processed KPI data into SQLite database

---

# 📊 Output

### Generated Files:

* `cleaned.csv`
* `kpi.csv`

### Database Table:

```sql
employee_kpi
```

---

# 📈 Sample KPIs

* Average working hours per employee
* Total late entries
* Total absences

---

# 🚀 How to Run

### 1. Start Airflow

```bash
airflow standalone
```

### 2. Place DAG

```bash
cp dags/attendance_pipeline.py ~/airflow/dags/
```

### 3. Trigger DAG

* Open: http://localhost:8080
* Run `employee_attendance_pipeline`

---

# ⚠️ Notes

* SQLite is used for development only
* Ensure dependencies:

```bash
pip install pandas
```

---

# 🔥 Key Features

* Modular pipeline design
* Data validation layer
* Business KPI generation
* Clean code separation (scripts)
* Retry handling in DAG

---

# 🧠 Key Learnings

* Building structured ETL pipelines
* Data validation techniques
* Business-driven transformations
* Modular Airflow architecture

---

# 🚀 Future Enhancements

* PostgreSQL integration
* Incremental data loading
* TaskGroups for better DAG structure
* XCom usage for inter-task communication
* Alerting and monitoring
* Dockerized deployment

---

# ⭐ Summary

This project demonstrates:

* Intermediate-level Airflow capabilities
* Real-world ETL pipeline design
* Business-focused data engineering

It bridges the gap between basic workflows and production-grade pipelines.
