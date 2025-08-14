-- models/marts/dim_payments.sql
{{
    config(
        materialized='table'
    )
}}

SELECT 
    payment_id,
    payment_method
FROM {{ source('ecommerce_raw', 'payment_method') }}
