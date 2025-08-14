{{ config(materialized='table', tags=['cube']) }}

-- 1) Pull only what we need from the fact
with sales as (
    select
        fs.customer_id,
        cast(fs.order_date as date)      as order_date,
        fs.total_sales_amount
    from {{ ref('fact_sales') }} fs
),

-- 2) Per-customer metrics (CLV, orders, avg days between orders)
gaps as (
    select
        customer_id,
        order_date,
        datediff('day',
                 lag(order_date) over (partition by customer_id order by order_date),
                 order_date)              as days_since_prev
    from sales
),
per_customer as (
    select
        s.customer_id,
        sum(s.total_sales_amount)                 as clv,                  -- lifetime value to date
        count(*)                                  as orders_count,         -- line-level count (treat as orders per spec)
        avg(g.days_since_prev)                    as avg_days_between_orders
    from sales s
    left join gaps g
      on g.customer_id = s.customer_id
     and g.order_date  = s.order_date
    group by 1
),

-- 3) Join to customer attributes (anonymized: no names/emails)
enriched as (
    select
        dc.customer_type,
        dc.customer_status,
        date_trunc('quarter', dc.registration_date) as registration_quarter,
        pc.clv,
        pc.orders_count,
        pc.avg_days_between_orders
    from per_customer pc
    join {{ ref('dim_customers') }} dc
      on pc.customer_id = dc.customer_id
)

-- 4) Segment-level aggregates
select
    registration_quarter,
    customer_type,
    customer_status,
    avg(clv)                       as avg_customer_lifetime_value,
    avg(orders_count)              as avg_orders_per_customer,
    avg(avg_days_between_orders)   as avg_days_between_orders
from enriched
group by 1,2,3
order by 1,2,3
