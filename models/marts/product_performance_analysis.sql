{{ config(materialized='table', tags=['cube']) }}

-- If your fact uses time_id instead of order_date, see the note below.

with base as (
    select
        date_trunc('month', fs.order_date) as sales_month,
        fs.customer_id,
        fs.product_id,
        fs.quantity,
        fs.price_unit,
        fs.total_sales_amount
    from {{ ref('fact_sales') }} fs
),

joined as (
    select
        b.sales_month,
        dp.category as product_category,
        b.customer_id,
        b.quantity,
        b.price_unit,
        b.total_sales_amount
    from base b
    left join {{ ref('dim_products') }} dp
      on b.product_id = dp.product_id
),

agg as (
    select
        sales_month,
        product_category,
        sum(total_sales_amount) as total_revenue,
        sum(quantity) as total_quantity,
        nullif(sum(total_sales_amount),0) / nullif(sum(quantity),0) as avg_price_per_unit,
        count(distinct customer_id) as unique_customers_per_category
    from joined
    group by 1,2
)

select
    a.*,
    lag(total_revenue) over (partition by product_category order by sales_month) as prev_month_revenue,
    case when lag(total_revenue) over (partition by product_category order by sales_month) = 0
         then null
         else (total_revenue - lag(total_revenue) over (partition by product_category order by sales_month))
              / lag(total_revenue) over (partition by product_category order by sales_month)
    end as revenue_mom_pct
from agg a
order by sales_month, product_category
