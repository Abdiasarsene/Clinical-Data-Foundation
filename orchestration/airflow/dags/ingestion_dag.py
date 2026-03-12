# dags/ingestion_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="healthcare_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "ingestion", "cli"],
) as dag:

    ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command="python -m src.orchestration.pipelines.run_ingestion --dataset healthcare",
    )