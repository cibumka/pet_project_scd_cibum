{% snapshot dim_product_scd2 %}

{{
    config(
        target_schema='shop',
        unique_key='product_id',
        strategy='check',
        check_cols=['title', 'price', 'category', 'rating_rate', 'rating_count'],
        file_format='iceberg',
    )
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}
