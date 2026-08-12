"""Spark job (DAG 2): reads a day's raw Parquet products dump from MinIO,
cleans/casts it, and upserts it into the Iceberg table stg.shop.products
(current known state per product_id).

    docker exec spark-iceberg spark-submit /opt/spark_jobs/raw_to_stg.py --ds <YYYY-MM-DD>
"""
import argparse

from pyspark.sql import SparkSession, functions as F

STG_TABLE = "stg.shop.products"


def main(ds: str) -> None:
    spark = SparkSession.builder.appName(f"raw_to_stg_{ds}").getOrCreate()

    raw_path = f"s3a://raw/products/dt={ds}/products.parquet"
    raw_df = spark.read.parquet(raw_path)

    clean_df = (
        raw_df.select(
            F.col("id").cast("int").alias("product_id"),
            F.col("title").cast("string").alias("title"),
            F.col("price").cast("double").alias("price"),
            F.col("category").cast("string").alias("category"),
            F.col("description").cast("string").alias("description"),
            F.col("`rating.rate`").cast("double").alias("rating_rate"),
            F.col("`rating.count`").cast("int").alias("rating_count"),
        )
        .dropDuplicates(["product_id"])
        .withColumn("stg_updated_at", F.current_timestamp())
    )
    clean_df.createOrReplaceTempView("raw_products")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS stg.shop")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {STG_TABLE} (
            product_id      INT,
            title           STRING,
            price           DOUBLE,
            category        STRING,
            description     STRING,
            rating_rate     DOUBLE,
            rating_count    INT,
            stg_updated_at  TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        MERGE INTO {STG_TABLE} t
        USING raw_products s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    count = spark.table(STG_TABLE).count()
    print(f"{STG_TABLE} now has {count} rows after merging ds={ds}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    args = parser.parse_args()
    main(args.ds)
