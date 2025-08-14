-- models/marts/dim_customers_anonymous.sql
{{
    config(
        materialized='table',
        tags=['anonymized', 'analytics_safe']
    )
}}

SELECT 
    customer_id,
    {{ data_anonymization('customer_name') }} as customer_name_hash,
    {{ data_anonymization('email') }} as email_hash,
    customer_type,         -- Keep business data
    customer_status,       -- Keep business data
    registration_date      -- Keep business data
    -- Personal info is now hashed and unrecoverable
FROM {{ ref('dim_customers') }}

