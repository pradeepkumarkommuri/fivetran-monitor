"""DAG 2: Trigger PySpark feature engineering on Kubernetes."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

SPARK_JOB_SPEC = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: fivetran-feature-eng-{{ ds_nodash }}-{{ ts_nodash }}
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "{{ var.value.spark_image }}"
  imagePullPolicy: Always
  mainApplicationFile: "local:///app/src/spark/feature_engineering.py"
  arguments:
    - "--partition"
    - "{{ macros.ds_format(ds, '%Y-%m-%d') }}/{{ execution_date.strftime('%H') }}"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 2
    onFailureRetryInterval: 10
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    envSecretKeyRefs:
      MINIO_ACCESS_KEY:
        name: minio-credentials
        key: access-key
      MINIO_SECRET_KEY:
        name: minio-credentials
        key: secret-key
  executor:
    cores: 2
    instances: 2
    memory: "2g"
"""

with DAG(
    dag_id="spark_feature_engineering",
    description="PySpark feature engineering on Kubernetes",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["spark", "features"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    submit_spark = SparkKubernetesOperator(
        task_id="submit_feature_spark_job",
        namespace="spark",
        application_file=SPARK_JOB_SPEC,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    wait_for_spark = SparkKubernetesSensor(
        task_id="wait_for_spark_completion",
        namespace="spark",
        application_name=(
            "{{ task_instance.xcom_pull("
            "task_ids='submit_feature_spark_job')['metadata']['name'] }}"
        ),
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        poke_interval=30,
        timeout=1800,
    )

    start >> submit_spark >> wait_for_spark >> end
