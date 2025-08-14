-- models/marts/fact_sales.sql
{{
    config(
        materialized='table'
    )
}}

WITH sales_data AS (
    SELECT 
        op.order_product_id,
        op.order_id,
        o.client_id AS customer_id,
        op.product_id,
        o.payment_id,
        o.order_date,
        op.quantity,
        op.price_unit,
        (op.quantity * op.price_unit) AS total_sales_amount,
        o.status AS order_status
    FROM {{ source('ecommerce_raw', 'order_product') }} op
    JOIN {{ source('ecommerce_raw', 'orders') }} o 
        ON op.order_id = o.order_id
),

final AS (
    SELECT 
        s.order_product_id AS sales_id,
        s.customer_id,
        s.product_id,
        s.payment_id,
        t.time_id,
        s.order_date,
        s.quantity,
        s.price_unit,
        s.total_sales_amount,
        s.order_status
    FROM sales_data s
    LEFT JOIN {{ ref('dim_time') }} t 
        ON DATE(s.order_date) = t.date_value
)

SELECT * FROM final
