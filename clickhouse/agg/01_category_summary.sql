CREATE TABLE IF NOT EXISTS mart.category_summary
(
    category        String,
    product_count   UInt32,
    avg_price       Float64,
    min_price       Float64,
    max_price       Float64,
    avg_rating      Float64,
    total_ratings   UInt64,
    computed_at     DateTime
)
ENGINE = MergeTree
ORDER BY category;

TRUNCATE TABLE mart.category_summary;

INSERT INTO mart.category_summary
SELECT
    category,
    count()                     AS product_count,
    round(avg(price), 2)        AS avg_price,
    min(price)                  AS min_price,
    max(price)                  AS max_price,
    round(avg(rating_rate), 2)  AS avg_rating,
    sum(rating_count)           AS total_ratings,
    now()                       AS computed_at
FROM mart.products_scd2
WHERE dbt_valid_to IS NULL
GROUP BY category;
