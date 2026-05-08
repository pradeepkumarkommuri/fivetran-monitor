"""DAG 1: Ingest Fivetran raw data → MinIO every hour."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="fivetran_ingest",
    description="Ingest Fivetran connector data to MinIO",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fivetran", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    def _ingest(**context):
        from src.ingestion.ingest import run_ingestion

        result = run_ingestion()
        context["ti"].xcom_push(key="ingestion_result", value=result)
        if result["failed"] > 0:
            raise ValueError(
                f"Ingestion had {result['failed']} failures "
                f"(succeeded: {result['succeeded']})"
            )
        return result

    ingest = PythonOperator(task_id="ingest_fivetran_data", python_callable=_ingest)

    start >> ingest >> end
