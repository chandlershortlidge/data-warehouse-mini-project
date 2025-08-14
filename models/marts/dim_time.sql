-- models/marts/dim_time.sql
{{
    config(
        materialized='table'
    )
}}

WITH date_range AS (
  -- Generate a sequence of dates starting from 2020-01-01
  SELECT 
    DATEADD(day, SEQ4(), '2020-01-01'::DATE) AS date_value
  FROM 
    TABLE(GENERATOR(ROWCOUNT => 2000))  -- Creates 2000 rows = ~5.5 years of dates
),

time_details AS (
  -- Break down each date into useful parts
  SELECT 
    date_value,
    EXTRACT(year FROM date_value) AS year,
    EXTRACT(month FROM date_value) AS month,
    EXTRACT(day FROM date_value) AS day,
    EXTRACT(quarter FROM date_value) AS quarter,
    TO_CHAR(date_value, 'MMMM') AS month_name,    -- January, February, etc.
    TO_CHAR(date_value, 'DY') AS day_name         -- Mon, Tue, Wed, etc.
  FROM date_range
  WHERE date_value <= CURRENT_DATE()  -- Only dates up to today
)

SELECT 
  ROW_NUMBER() OVER (ORDER BY date_value) AS time_id,  -- Unique ID for each date
  date_value,
  year,
  month,
  day,
  quarter,
  month_name,
  day_name
FROM time_details
