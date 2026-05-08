"""DAG 5: Alert dispatcher — rule-based + ML alerts → ntfy.sh + Outlook email."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
}

with DAG(
    dag_id="alert_dispatcher",
    description="Rule-based + ML alert dispatcher",
    schedule="45 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["alerts"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    def _rule_based_check(**context):
        from src.alerts.dispatcher import check_connector_health, check_row_anomalies
        from src.ingestion.fivetran_client import FivetranClient

        client = FivetranClient()
        connectors = client.list_connectors()
        health = []
        row_alerts = []
        for conn in connectors:
            health.extend(check_connector_health(conn))
            history = client.get_sync_history(conn["id"], limit=5)
            row_alerts.extend(check_row_anomalies(conn, history))
        context["ti"].xcom_push(
            key="health_issues", value=[h.__dict__ for h in health]
        )
        context["ti"].xcom_push(
            key="row_anomalies", value=[r.__dict__ for r in row_alerts]
        )
        return {"health": len(health), "row_anomalies": len(row_alerts)}

    rule_check = PythonOperator(
        task_id="rule_based_checks",
        python_callable=_rule_based_check,
    )

    def _merge_alerts(**context):
        ti = context["ti"]
        health = ti.xcom_pull(task_ids="rule_based_checks", key="health_issues") or []
        row_alerts = (
            ti.xcom_pull(task_ids="rule_based_checks", key="row_anomalies") or []
        )
        from src.alerts.dispatcher import load_ml_alerts

        ml_alerts = load_ml_alerts()
        all_alerts = {
            "health": health,
            "row": row_alerts,
            "ml": ml_alerts,
            "total": len(health) + len(row_alerts) + len(ml_alerts),
        }
        ti.xcom_push(key="all_alerts", value=all_alerts)
        return all_alerts

    merge = PythonOperator(task_id="merge_alerts", python_callable=_merge_alerts)

    def _dispatch(**context):
        from src.alerts.dispatcher import dispatch_all

        ti = context["ti"]
        all_alerts = ti.xcom_pull(task_ids="merge_alerts", key="all_alerts") or {}
        sent = dispatch_all(all_alerts)
        return {"alerts_sent": sent}

    dispatch = PythonOperator(task_id="dispatch_alerts", python_callable=_dispatch)

    start >> rule_check >> merge >> dispatch >> end
