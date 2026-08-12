"""DAG 4/5: Spark job copies the mart Iceberg tables (SCD2 dimension +
daily aggregate) into ClickHouse landing tables via JDBC.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="04_mart_to_clickhouse",
    description="Spark (JDBC): mart Iceberg -> ClickHouse landing tables",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mart", "clickhouse", "spark"],
) as dag:
    mart_to_clickhouse_spark_job = BashOperator(
        task_id="mart_to_clickhouse_spark_job",
        bash_command="docker exec spark-iceberg spark-submit /opt/spark_jobs/mart_to_clickhouse.py",
    )
