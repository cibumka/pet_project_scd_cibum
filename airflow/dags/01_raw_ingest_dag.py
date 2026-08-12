"""DAG 1/5: pull products from the Fake Store API and land them as Parquet
in the MinIO "raw" bucket, partitioned by ingestion date.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from producer.fetch_products import fetch_and_upload

with DAG(
    dag_id="01_raw_ingest",
    description="Fake Store API -> MinIO raw bucket (Parquet)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["raw", "ingest"],
) as dag:
    fetch_products_to_raw = PythonOperator(
        task_id="fetch_products_to_raw",
        python_callable=fetch_and_upload,
        op_kwargs={"ds": "{{ ds }}"},
    )
