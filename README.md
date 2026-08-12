# pet_project_scd_cibum

Учебный ETL пет-проект: полный пайплайн от сырого API до агрегатов в ClickHouse,
с SCD2-измерением в mart-слое. Вся инфраструктура поднимается локально в Docker
одной командой.

## Архитектура

```
Fake Store API (fakestoreapi.com)
   │  Python (boto3 + pyarrow)                              DAG 1: 01_raw_ingest
   ▼
MinIO bucket "raw"        Parquet, партиции по дате: products/dt=<ds>/products.parquet
   │  Spark (spark-submit)                                  DAG 2: 02_raw_to_stg
   ▼
MinIO bucket "stg"        Iceberg-таблица stg.shop.products (MERGE по product_id,
   │                                       текущее состояние каждого товара)
   │  dbt-spark (snapshot + run)                             DAG 3: 03_stg_to_mart
   ▼
MinIO bucket "mart"       Iceberg-таблицы:
   │                        - mart.shop.dim_product_scd2      (dbt snapshot, SCD2)
   │                        - mart.shop.mart_price_daily_agg  (incremental-агрегат)
   │  Spark (JDBC)                                            DAG 4: 04_mart_to_clickhouse
   ▼
ClickHouse                mart.products_scd2, mart.price_daily_agg (landing)
   │  ClickHouse SQL (clickhouse-connect)                     DAG 5: 05_clickhouse_aggregation
   ▼
ClickHouse                mart.category_summary (финальный агрегат)
```

Оркестрация - Apache Airflow (LocalExecutor), 5 независимых DAG'ов, по одному на
каждый шаг. Табличный формат - Apache Iceberg (Hadoop-подобные каталоги на JDBC,
см. "Особенности и грабли" ниже). S3-хранилище - MinIO. Трансформации stg → mart -
dbt-spark.

## Стек

| Слой | Технология |
|---|---|
| Оркестрация | Apache Airflow 2.10 (LocalExecutor, Postgres) |
| Object storage | MinIO (S3-совместимый) |
| Обработка | Apache Spark 3.5 (PySpark, `local[*]`) |
| Табличный формат | Apache Iceberg (JDBC-каталоги `stg` и `mart`, backend - Postgres) |
| Трансформации | dbt-core + dbt-spark (`method: session`) |
| Аналитическая БД | ClickHouse |
| Источник данных | [Fake Store API](https://fakestoreapi.com) |

## Быстрый старт

Требуется Docker Desktop (проверялось на Windows). Стеку нужно ощутимо памяти -
если параллельно крутятся другие тяжёлые docker-compose проекты, освободите
ресурсы (`docker compose down` в других проектах) перед первым запуском.

```bash
git clone git@github.com:cibumka/pet_project_scd_cibum.git
cd pet_project_scd_cibum
cp .env.example .env
# сгенерировать реальный Fernet key для Airflow и вставить в .env вместо заглушки:
python -c "import os, base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

docker compose up -d
```

Первый запуск соберёт кастомные образы Airflow и Spark (несколько минут) и
скачает образы MinIO/Postgres/ClickHouse. Дождитесь, пока все контейнеры станут
`healthy`:

```bash
docker compose ps
```

### Точки входа

| Сервис | URL | Логин |
|---|---|---|
| Airflow UI | http://localhost:8090 | airflow / airflow |
| MinIO Console | http://localhost:9101 | minioadmin / minioadmin123 |
| ClickHouse (HTTP) | http://localhost:8123 | default / clickhouse123 |
| ClickHouse (native TCP) | localhost:9002 | default / clickhouse123 |

Порты 8090/9100/9101 намеренно смещены от стандартных 8080/9000/9001 - на случай,
если рядом уже крутится другой Airflow/MinIO.

### Запуск пайплайна

В Airflow UI включите (unpause) все 5 DAG'ов и запустите `01_raw_ingest` -
дальше можно запускать `02` → `03` → `04` → `05` вручную по очереди (в таком
порядке, каждый следующий зависит от результата предыдущего), либо всё то же
самое через CLI:

```bash
docker exec pet_project_scd_cibum-airflow-scheduler-1 airflow dags trigger 01_raw_ingest
# дождаться success, затем:
docker exec pet_project_scd_cibum-airflow-scheduler-1 airflow dags trigger 02_raw_to_stg
docker exec pet_project_scd_cibum-airflow-scheduler-1 airflow dags trigger 03_stg_to_mart
docker exec pet_project_scd_cibum-airflow-scheduler-1 airflow dags trigger 04_mart_to_clickhouse
docker exec pet_project_scd_cibum-airflow-scheduler-1 airflow dags trigger 05_clickhouse_aggregation
```

Проверить статус: `airflow dags list-runs -d <dag_id>`.

## Как проверить, что SCD2 действительно работает

`mart.shop.dim_product_scd2` - это dbt snapshot (`strategy=check`), а не просто
копия текущих данных: при изменении цены товара в `stg.shop.products` старая
версия строки закрывается (`dbt_valid_to` проставляется), а открывается новая
(`dbt_valid_to IS NULL`), и обе версии остаются в таблице.

Проверить руками:

```bash
# 1. Посмотреть текущую цену товара 1
docker exec spark-iceberg spark-sql -e \
  "SELECT product_id, price FROM stg.shop.products WHERE product_id = 1;"

# 2. Поменять её
docker exec spark-iceberg spark-sql -e \
  "UPDATE stg.shop.products SET price = 1.23 WHERE product_id = 1;"

# 3. Пересчитать snapshot
docker exec spark-iceberg dbt snapshot --project-dir /opt/dbt/pet_dwh

# 4. Увидеть обе версии
docker exec spark-iceberg spark-sql -e \
  "SELECT product_id, price, dbt_valid_from, dbt_valid_to
   FROM mart.shop.dim_product_scd2 WHERE product_id = 1 ORDER BY dbt_valid_from;"
```

Ожидаемый результат - две строки: старая цена с проставленным `dbt_valid_to`,
новая цена с `dbt_valid_to = NULL`.

## Структура репозитория

```
pet_project_scd_cibum/
├── docker-compose.yml
├── airflow/
│   ├── Dockerfile              # + docker CLI (для `docker exec` из тасков)
│   ├── requirements.txt
│   └── dags/                   # 5 DAG'ов, 01..05
├── spark/
│   ├── Dockerfile              # tabulario/spark-iceberg + clickhouse-jdbc + dbt-spark
│   ├── conf/spark-defaults.conf
│   └── jobs/                   # raw_to_stg.py, mart_to_clickhouse.py, smoke_test.py
├── dbt/pet_dwh/                # dbt-проект: staging, mart-модели, SCD2-snapshot
├── clickhouse/
│   ├── init/                   # DDL landing-таблиц (mart.products_scd2, mart.price_daily_agg)
│   └── agg/                    # SQL финальной агрегации (mart.category_summary)
├── postgres/init/              # доп. БД iceberg_catalog для JDBC-каталога Iceberg
└── producer/                   # Python-клиент Fake Store API
```

## Особенности и грабли (на будущее)

Несколько нетривиальных решений, принятых по ходу разработки - на случай, если
захочется что-то поменять:

- **Iceberg-каталоги - JDBC (Postgres), не Hadoop.** Изначально использовались
  Hadoop-каталоги (файловые, без метастора). Оказалось, что
  `HadoopCatalog.listNamespaces()` (его использует, в частности, `SHOW DATABASES`,
  а значит и каждый запуск dbt) в этой версии Iceberg ломается на S3A/MinIO с
  ошибкой `path must be absolute` - воспроизводится даже без dbt, одним `spark-sql`.
  Переключились на JDBC-каталог поверх существующего Postgres (отдельная БД
  `iceberg_catalog`) - это и чинит проблему, и является рекомендуемым Iceberg
  выбором для object storage.
- **`spark.sql.defaultCatalog=mart`, а не `stg`.** dbt-spark плохо работает с
  "фальшивым" многокаталожным адресом вида `schema: "stg.shop"` для *своих же*
  управляемых объектов (моделей/snapshot'ов) - конкретно ломается проверка
  "существует ли уже таблица" (важна для инкрементальной логики snapshot'а), хотя
  для чтения источников (`sources.yml`) этот трюк с точкой в имени схемы работает
  нормально. Поэтому у dbt схема простая (`shop`), резолвится через default-каталог
  `mart`; а `stg.shop.products` как источник читается по полному имени.
- **Образ `tabulario/spark-iceberg` запускает лишнее.** Его `entrypoint.sh` по
  умолчанию поднимает Spark master, worker, history-server и thrift-server (4
  лишних JVM, несколько GB RAM) при любом старте контейнера - хотя в этом пайплайне
  ничего из этого не используется (`spark-submit` работает в `local[*]`, dbt-spark -
  через `method: session`). В docker-compose.yml `entrypoint` полностью
  переопределён на простое `tail -f /dev/null`, а Airflow общается с контейнером
  через `docker exec`.
- **`mart_price_daily_agg` - инкрементальная модель (`merge` по `category, agg_date`),
  не обычная table.** Обычная table означала бы полную пересборку при каждом
  запуске - и потерю агрегатов за предыдущие дни.
