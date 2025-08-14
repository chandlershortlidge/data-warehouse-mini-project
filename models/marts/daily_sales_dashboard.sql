{{ config(materialized='table', tags=['cube']) }}

with base as (
    select
        date(fs.order_date)                    as order_day,
        to_char(fs.order_date, 'DY')           as day_name,   -- MON, TUE, ...
        fs.customer_id,
        fs.total_sales_amount
    from {{ ref('fact_sales') }} fs
)

select
    order_day,
    day_name,
    sum(total_sales_amount)          as total_revenue,
    count(*)                         as number_of_orders,      -- line-level count per day
    avg(total_sales_amount)          as avg_order_value,
    count(distinct customer_id)      as unique_customers_per_day
from base
group by order_day, day_name
order by order_day
