{{ config(materialized='ephemeral') }}

select
    product_id,
    title,
    price,
    category,
    description,
    rating_rate,
    rating_count,
    stg_updated_at
from {{ source('stg_shop', 'products') }}
