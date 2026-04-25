from datetime import datetime
import os
import pandas as pd
import sqlite3

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Base path (dynamic, works in your setup)
BASE_PATH = os.path.expanduser("~//data-engineering-portfolio/airflow-project/airflow-basic/")
RAW_PATH = f"{BASE_PATH}/data/raw/titanic.csv"
PROCESSED_PATH = f"{BASE_PATH}/data/processed/titanic_cleaned.csv"
DB_PATH = f"{BASE_PATH}/data/titanic.db"


# -------------------------------
# CLEANING FUNCTION
# -------------------------------
def clean_csv():
    df = pd.read_csv(RAW_PATH)

    # 1. Handle missing Age → fill with median
    df["Age"].fillna(df["Age"].median(), inplace=True)

    # 2. Drop useless columns
    df.drop(columns=["Cabin", "Ticket", "Name"], inplace=True)

    # 3. Encode categorical columns
    df["Sex"] = df["Sex"].map({"male": 0, "female": 1})

    # Embarked encoding (handle missing first)
    df["Embarked"].fillna(df["Embarked"].mode()[0], inplace=True)
    df["Embarked"] = df["Embarked"].map({"S": 0, "C": 1, "Q": 2})

    # Save cleaned data
    df.to_csv(PROCESSED_PATH, index=False)

    print("Data cleaned and saved")


# -------------------------------
# LOAD FUNCTION
# -------------------------------
def load_to_sqlite():
    df = pd.read_csv(PROCESSED_PATH)

    conn = sqlite3.connect(DB_PATH)

    # Write to SQLite table
    df.to_sql("titanic_data", conn, if_exists="replace", index=False)

    conn.close()

    print("Data loaded into SQLite")


# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="csv_to_sqlite_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["basic", "etl"],
) as dag:

    download_csv = BashOperator(
        task_id="download_csv",
        bash_command=f"""
        mkdir -p {BASE_PATH}/data/raw &&
        wget -O {RAW_PATH} "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
        """,
    )

    clean_task = PythonOperator(
        task_id="clean_csv",
        python_callable=clean_csv,
    )

    load_task = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=load_to_sqlite,
    )

    download_csv >> clean_task >> load_task
