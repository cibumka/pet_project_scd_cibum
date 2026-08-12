# pet_project_scd_cibum

Учебный ETL пет-проект: полный пайплайн от сырого API до агрегатов в ClickHouse, с SCD2
измерением в mart-слое. Вся инфраструктура поднимается локально в Docker.

## Архитектура (в разработке)

```
Fake Store API
   -> MinIO/raw   (Parquet)
   -> Spark       -> MinIO/stg   (Iceberg)
   -> dbt-spark   -> MinIO/mart  (Iceberg: SCD2 dim + агрегат)
   -> Spark (JDBC) -> ClickHouse (landing)
   -> ClickHouse SQL -> ClickHouse (агрегаты)
```

Оркестрация: Apache Airflow, 5 DAG'ов (по одному на каждый шаг).

Статус: проект строится пошагово, см. историю коммитов.
