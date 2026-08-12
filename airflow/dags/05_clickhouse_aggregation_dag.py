"""DAG 5/5: runs SQL directly in ClickHouse to build final aggregate
tables (category_summary) from the replicated mart landing tables.
"""
import os
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator

AGG_SQL_DIR = Path("/opt/airflow/include/clickhouse_agg")


def run_aggregation_sql() -> None:
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DB"],
    )
    for sql_file in sorted(AGG_SQL_DIR.glob("*.sql")):
        statements = [s.strip() for s in sql_file.read_text().split(";") if s.strip()]
        for stmt in statements:
            client.command(stmt)
        print(f"Executed {len(statements)} statement(s) from {sql_file.name}")


with DAG(
    dag_id="05_clickhouse_aggregation",
    description="ClickHouse: build category_summary from current SCD2 state",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["clickhouse", "aggregation"],
) as dag:
    run_aggregation = PythonOperator(
        task_id="run_aggregation_sql",
        python_callable=run_aggregation_sql,
    )
