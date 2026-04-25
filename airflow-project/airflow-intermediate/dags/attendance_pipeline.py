from datetime import datetime
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Import custom modules
from scripts.validate import validate_data
from scripts.transform import transform_data
from scripts.metrics import calculate_metrics
from scripts.load import load_to_sqlite

BASE_PATH = os.path.expanduser("~/data-engineering-portfolio/airflow-project/airflow-intermediate")

RAW_FILE = f"{BASE_PATH}/data/raw/employees_attendence_dataset.csv"
CLEAN_FILE = f"{BASE_PATH}/data/processed/cleaned.csv"
KPI_FILE = f"{BASE_PATH}/data/processed/kpi.csv"
DB_FILE = f"{BASE_PATH}/db/attendance.db"

default_args = {
    "owner": "airflow",
    "retries": 2
}

with DAG(
    dag_id="employee_attendance_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["intermediate", "etl", "attendance"]
) as dag:

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_args=[RAW_FILE],
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_args=[RAW_FILE, CLEAN_FILE],
    )

    metrics_task = PythonOperator(
        task_id="calculate_metrics",
        python_callable=calculate_metrics,
        op_args=[CLEAN_FILE, KPI_FILE],
    )

    load_task = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_sqlite,
        op_args=[KPI_FILE, DB_FILE],
    )

    validate_task >> transform_task >> metrics_task >> load_task
