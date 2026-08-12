CREATE TABLE IF NOT EXISTS mart.products_scd2
(
    product_id      UInt32,
    title           String,
    price           Float64,
    category        String,
    description     String,
    rating_rate     Float64,
    rating_count    UInt32,
    stg_updated_at  DateTime,
    dbt_scd_id      String,
    dbt_updated_at  DateTime,
    dbt_valid_from  DateTime,
    dbt_valid_to    Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY (product_id, dbt_valid_from);

CREATE TABLE IF NOT EXISTS mart.price_daily_agg
(
    category        String,
    agg_date        Date,
    product_count   UInt32,
    avg_price       Float64,
    min_price       Float64,
    max_price       Float64,
    avg_rating      Float64
)
ENGINE = MergeTree
ORDER BY (category, agg_date);
