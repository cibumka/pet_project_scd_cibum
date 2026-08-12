"""DAG 2/5: Spark job cleans a day's raw Parquet and upserts it into the
Iceberg table stg.shop.products (current known state per product).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="02_raw_to_stg",
    description="Spark: MinIO raw -> Iceberg stg.shop.products",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stg", "spark", "iceberg"],
) as dag:
    raw_to_stg_spark_job = BashOperator(
        task_id="raw_to_stg_spark_job",
        bash_command=(
            "docker exec spark-iceberg spark-submit "
            "/opt/spark_jobs/raw_to_stg.py --ds {{ ds }}"
        ),
    )
