"""Spark job (DAG 4): copies the mart Iceberg tables into ClickHouse
landing tables via JDBC (full overwrite each run - the mart tables
already hold the complete current state: full SCD2 history and the
full accumulated daily aggregate).

    docker exec spark-iceberg spark-submit /opt/spark_jobs/mart_to_clickhouse.py
"""
import os

from pyspark.sql import SparkSession

JDBC_URL = f"jdbc:clickhouse://{os.environ['CLICKHOUSE_HOST']}:8123/{os.environ['CLICKHOUSE_DB']}"
JDBC_PROPS = {
    "user": os.environ["CLICKHOUSE_USER"],
    "password": os.environ["CLICKHOUSE_PASSWORD"],
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

TABLES = [
    ("mart.shop.dim_product_scd2", "products_scd2"),
    ("mart.shop.mart_price_daily_agg", "price_daily_agg"),
]


def _truncate(spark: SparkSession, ch_table: str) -> None:
    # Spark's generic JDBC writer doesn't know ClickHouse's MergeTree DDL
    # (its overwrite-mode fallback tries to DROP/CREATE without an ORDER
    # BY clause and fails), so truncate explicitly via a raw JDBC call and
    # append the fresh data instead - the landing tables already exist
    # with the right schema (see clickhouse/init/01_create_mart_tables.sql).
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, JDBC_PROPS["user"], JDBC_PROPS["password"]
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(f"TRUNCATE TABLE {os.environ['CLICKHOUSE_DB']}.{ch_table}")
        stmt.close()
    finally:
        conn.close()


def main() -> None:
    spark = SparkSession.builder.appName("mart_to_clickhouse").getOrCreate()

    for iceberg_table, ch_table in TABLES:
        df = spark.table(iceberg_table)
        _truncate(spark, ch_table)
        df.write.mode("append").jdbc(url=JDBC_URL, table=ch_table, properties=JDBC_PROPS)
        print(f"Wrote {df.count()} rows from {iceberg_table} to ClickHouse.{ch_table}")

    spark.stop()


if __name__ == "__main__":
    main()
