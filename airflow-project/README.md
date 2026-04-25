# 🚀 Airflow Data Engineering Project

## 📌 Overview

This repository contains **end-to-end ETL pipelines built using Apache Airflow**, designed to simulate real-world data engineering workflows. The projects demonstrate core orchestration concepts, modular pipeline design, and business-driven data processing.

The goal of this repository is to showcase **practical, industry-relevant Airflow implementations** ranging from basic ETL to intermediate-level pipelines with validation, transformation, and analytics.

---

# 🏗️ Tech Stack

* **Apache Airflow** (Workflow orchestration)
* **Python** (Data processing)
* **Pandas** (Data transformation)
* **SQLite** (Database layer)
* **BashOperator & PythonOperator**
* **WSL Ubuntu Environment**

---

# 📂 Repository Structure

```
airflow-project/
│
├── airflow-basic/
│   ├── dags/
│   ├── data/
│   └── scripts/
│
├── airflow-intermediate/
│   ├── dags/
│   ├── data/
│   ├── scripts/
│   └── db/
│
└── README.md
```

---

# 🔹 Project 1: CSV to SQLite ETL Pipeline (Basic)

## 📌 Description

A simple ETL pipeline that:

* Downloads a CSV dataset using BashOperator
* Cleans and transforms data using Python
* Loads processed data into SQLite

## ⚙️ Workflow

```
Download CSV → Clean Data → Load to SQLite
```

## 🧠 Key Learnings

* Airflow DAG structure
* BashOperator usage
* PythonOperator basics
* File-based ETL pipeline

---

# 🔹 Project 2: Employee Attendance & Productivity Pipeline (Intermediate)

## 📌 Problem Statement

Organizations need automated systems to monitor employee attendance and productivity. Manual tracking is inefficient and does not scale.

## 🎯 Objective

Build a pipeline that:

* Ingests attendance data
* Validates data quality
* Transforms raw logs into meaningful metrics
* Stores KPIs for reporting

---

## ⚙️ Workflow

```
Validate Data → Transform Data → Generate KPIs → Load to Database
```

---

## 📊 Key Metrics Generated

* Average working hours per employee
* Late arrival count
* Absenteeism count
* Productivity insights

---

## 🧠 Key Features

* Data validation layer (quality checks)
* Modular pipeline design
* Business logic implementation
* KPI aggregation
* SQLite integration

---

# 🚀 How to Run

## 1. Start Airflow

```
airflow standalone
```

## 2. Place DAGs

```
~/airflow/dags/
```

## 3. Trigger DAG

* Open: http://localhost:8080
* Enable and trigger DAG

---

# ⚠️ Notes

* This project uses **SQLite for development purposes only**
* For production, use **PostgreSQL or MySQL**
* Ensure required Python packages are installed:

```
pip install pandas
```

---

# 🔥 Key Concepts Demonstrated

* DAG orchestration
* Task dependencies
* Modular ETL design
* Data validation strategies
* Business-driven data transformation
* Pipeline structuring for scalability

---

# 📈 Future Enhancements

* PostgreSQL integration
* Incremental data loading
* TaskGroups for better DAG structure
* XCom for inter-task communication
* Alerting & monitoring
* Docker-based Airflow setup

---

# 👨‍💻 Author

**Rahul Likhar**
Data Engineer | Python | SQL | Airflow | Azure

---

# ⭐ Summary

This repository demonstrates:

* Practical Airflow usage
* Real-world ETL pipelines
* Clean and scalable project structure

It is designed to showcase **data engineering skills in a production-oriented approach**.

---

👉 *More projects (Intermediate & Advanced) coming soon...*
