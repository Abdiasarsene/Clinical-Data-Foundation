# dags/modeling_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="healthcare_modeling",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "modeling", "cli"],
) as dag:

    modeling = BashOperator(
        task_id="run_modeling",
        bash_command="python -m src.orchestration.pipelines.run_modeling --dataset healthcare",
    )