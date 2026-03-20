# dags/full_pipeline_dag.py
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="healthcare_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "full_pipeline", "cli"],
) as dag:

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="healthcare_ingestion"
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="healthcare_silver"
    )

    trigger_modeling = TriggerDagRunOperator(
        task_id="trigger_modeling",
        trigger_dag_id="healthcare_modeling"
    )

    trigger_ingestion >> trigger_silver >> trigger_modeling