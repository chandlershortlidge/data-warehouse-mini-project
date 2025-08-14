-- models/marts/dim_customers.sql
{{
    config(
        materialized='table'
    )
}}

SELECT 
    c.client_id AS customer_id,
    c.client_name AS customer_name,
    c.email,
    c.phone_number,
    c.address,
    ct.type_name AS customer_type,
    cs.status_name AS customer_status,
    c.registration_date
FROM {{ source('ecommerce_raw', 'client') }} c
LEFT JOIN {{ source('ecommerce_raw', 'client_type') }} ct 
    ON c.type_id = ct.type_id
LEFT JOIN {{ source('ecommerce_raw', 'client_status') }} cs 
    ON c.status_id = cs.status_id
