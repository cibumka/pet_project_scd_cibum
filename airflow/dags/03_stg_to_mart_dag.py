"""DAG 3/5: dbt-spark builds the mart layer - an SCD2 snapshot dimension
(dim_product_scd2) and a daily price aggregate (mart_price_daily_agg),
both Iceberg tables in the mart catalog.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_BASE = "docker exec spark-iceberg dbt {sub} --project-dir /opt/dbt/pet_dwh"

with DAG(
    dag_id="03_stg_to_mart",
    description="dbt-spark: stg.shop.products -> mart Iceberg (SCD2 dim + daily agg)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mart", "dbt", "scd2"],
) as dag:
    dbt_snapshot_scd2 = BashOperator(
        task_id="dbt_snapshot_scd2",
        bash_command=DBT_BASE.format(sub="snapshot"),
    )
    dbt_run_mart_models = BashOperator(
        task_id="dbt_run_mart_models",
        bash_command=DBT_BASE.format(sub="run"),
    )

    dbt_snapshot_scd2 >> dbt_run_mart_models
