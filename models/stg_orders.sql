{{ config(materialized='view') }}

-- minimal stub so ref('stg_orders') resolves
select
  order_id,
  client_id,
  payment_id,
  order_date,
  status,
  total_amount
from {{ source('ecommerce_raw', 'orders') }};
