-- models/marts/fact_sales_external.sql
{{ config(materialized='table', tags=['external','pii_safe']) }}

with sales as (
    select
        date_trunc('month', fs.order_date)  as sales_month,
        fs.customer_id,
        fs.product_id,
        fs.total_sales_amount,
        fs.quantity
    from {{ ref('fact_sales') }} fs
),
joined as (
    select
        s.sales_month,
        dc.customer_type,
        dp.category as product_category,
        s.total_sales_amount,
        s.quantity
    from sales s
    left join {{ ref('dim_customers') }} dc on s.customer_id = dc.customer_id
    left join {{ ref('dim_products') }}  dp on s.product_id  = dp.product_id
)

select
    sales_month,
    customer_type,
    product_category,
    count(*)                as number_of_orders,   -- as required
    sum(total_sales_amount) as total_revenue,
    avg(total_sales_amount) as avg_order_value,
    sum(quantity)           as total_quantity
from joined
group by 1,2,3
order by 1,2,3
