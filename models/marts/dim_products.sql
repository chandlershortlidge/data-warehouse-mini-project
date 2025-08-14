-- models/marts/dim_products.sql
{{
    config(
        materialized='table'
    )
}}

SELECT 
    product_id,
    product_name,
    category,
    price
FROM {{ source('ecommerce_raw', 'product') }}
