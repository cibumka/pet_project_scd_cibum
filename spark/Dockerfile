FROM tabulario/spark-iceberg

# ClickHouse JDBC driver, used by the mart -> ClickHouse Spark job.
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar /opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar

# hadoop-aws (s3a://) - the base image ships hadoop-client-api/runtime
# (3.3.4) but not the S3A connector, needed for reading/writing s3a:// paths.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Postgres JDBC driver, for Iceberg's JDBC catalog (metadata lives in
# Postgres instead of being listed off S3 - see spark-defaults.conf).
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar

# dbt (dbt-spark, "session" method - runs against the local SparkSession
# in-process, no Thrift server needed) for the stg -> mart transformations.
RUN pip install --no-cache-dir dbt-core==1.8.7 "dbt-spark[session]==1.8.0"

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
