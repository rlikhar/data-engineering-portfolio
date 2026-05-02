from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {

    "owner": "rahul",

    "depends_on_past": False,

    "email_on_failure": False,

    "email_on_retry": False,

    "retries": 2,

    "retry_delay": timedelta(minutes=2),
}


with DAG(

    dag_id="gold_analytics_pipeline",

    default_args=default_args,

    description="Fraud Gold Analytics Pipeline",

    schedule="*/5 * * * *",

    start_date=datetime(2026, 5, 1),

    catchup=False,

    tags=["fraud", "gold", "analytics"]

) as dag:

    run_gold_analytics = BashOperator(

        task_id="run_gold_analytics",

        bash_command="""
        cd /opt/project && \
        python -m spark.jobs.gold_analytics
        """
    )