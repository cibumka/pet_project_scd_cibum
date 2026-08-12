select
    category,
    current_date() as agg_date,
    count(*)                    as product_count,
    round(avg(price), 2)        as avg_price,
    min(price)                  as min_price,
    max(price)                  as max_price,
    round(avg(rating_rate), 2)  as avg_rating
from {{ ref('stg_products') }}
group by category
