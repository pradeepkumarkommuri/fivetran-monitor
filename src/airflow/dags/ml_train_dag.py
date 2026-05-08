"""DAG 3: Daily ML model training (Isolation Forest + Prophet)."""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": True,
}

with DAG(
    dag_id="ml_model_training",
    description="Daily ML model training — Isolation Forest + Prophet",
    schedule="0 2 * * *",    # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ml", "training"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    def _train(**context):
        from src.ml.trainer import run_training
        summary = run_training()
        context["ti"].xcom_push(key="training_summary", value=summary)
        total_trained = (summary["isolation_forest"]["trained"]
                         + summary["prophet"]["trained"])
        if total_trained == 0:
            raise ValueError("No models were trained — check feature store has data.")
        return summary

    train = PythonOperator(
        task_id="train_ml_models",
        python_callable=_train,
        execution_timeout=timedelta(hours=2),
    )

    start >> train >> end
