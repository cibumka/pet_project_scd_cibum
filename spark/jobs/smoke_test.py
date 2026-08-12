"""Diagnostic script: verifies Spark can talk to MinIO and Iceberg (Hadoop
catalog) works end to end. Run with:

    docker exec spark-iceberg spark-submit /opt/spark_jobs/smoke_test.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke_test").getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS stg.smoke")
spark.sql("DROP TABLE IF EXISTS stg.smoke.ping")
spark.sql(
    """
    CREATE TABLE stg.smoke.ping (id INT, msg STRING)
    USING iceberg
    """
)
spark.sql("INSERT INTO stg.smoke.ping VALUES (1, 'hello from iceberg+minio')")

rows = spark.sql("SELECT * FROM stg.smoke.ping").collect()
assert len(rows) == 1 and rows[0]["id"] == 1, f"unexpected rows: {rows}"
print("SMOKE TEST OK:", rows[0])

spark.sql("DROP TABLE stg.smoke.ping")
spark.stop()
