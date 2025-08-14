
# Data Warehouse Mini Project

Mini end-to-end DW on **Snowflake + dbt**: ingest raw CSVs → build clean dims/facts → create reporting “cubes” → anonymize PII.

## Stack
- **Warehouse:** Snowflake
- **Transforms/Tests:** dbt (profile: `ecommerce`)
- **Ingest:** S3 → Snowflake (Snowpipe) — configured outside this repo

## Project Structure
models/
raw/ # (sources.yml only)
dimensions/ # dims
marts/ # facts + cubes
macros/ # anonymization macro

Key models:
- **Dimensions:** `dim_customers`, `dim_products`, `dim_time`, `dim_payments`
- **Fact:** `fact_sales`
- **Cubes (pre-aggregates):**  
  - `daily_sales_dashboard` (daily revenue / orders / AOV / unique customers)  
  - `product_performance_analysis` (monthly by category, MoM %)  
  - `customer_segmentation_report` (type/status cohort metrics)
- **External/Anonymized:** `fact_sales_external`, `customers_anonymous`
- **Tests (examples):** `dim_customers` (not_null on key), `dim_payments` (accepted_values), etc.

## Common Commands
dbt run  -s fact_sales_external
dbt run  -s daily_sales_dashboard
dbt test -s dim_customers


## Quick Start
```bash
# from repo root
dbt deps
dbt build

