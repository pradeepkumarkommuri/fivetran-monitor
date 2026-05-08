"""DAG 4: Hourly ML inference — score latest sync data."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="ml_prediction",
    description="Hourly ML inference — anomaly detection + forecasting",
    schedule="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ml", "prediction"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    def _predict(**context):
        from src.ml.predictor import run_prediction

        partition = context["ds"] + "/" + context["execution_date"].strftime("%H")
        anomalies = run_prediction(partition=partition)
        context["ti"].xcom_push(key="ml_anomalies", value=anomalies)
        context["ti"].xcom_push(key="anomaly_count", value=len(anomalies))
        return {"anomaly_count": len(anomalies)}

    predict = PythonOperator(
        task_id="run_ml_inference",
        python_callable=_predict,
        execution_timeout=timedelta(minutes=30),
    )

    start >> predict >> end
