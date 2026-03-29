## Setup / Deployment Instructions

SQL objects must be executed in the correct order because later layers depend on earlier ones.

Execution flow:

RAW → STAGING → ANALYTICS → BUSINESS QUERIES → DASHBOARD

---

## Step 1 — RAW Layer

Run:

raw/01_raw_layer.sql

Purpose:

Creates external tables that read source data files from the data lake.

Tables created:

- raw.users
- raw.orders
- raw.products
- raw.events

These tables contain unprocessed source data.

Key characteristics:

- data stored in parquet/csv format
- no transformations applied
- schema defined using CREATE EXTERNAL TABLE
- acts as source for staging layer

Example structure:

CREATE EXTERNAL TABLE raw.users  
CREATE EXTERNAL TABLE raw.orders  
CREATE EXTERNAL TABLE raw.products  
CREATE EXTERNAL TABLE raw.events  

---

## Step 2 — STAGING Layer

Run:

staging/02_staging_layer.sql

Purpose:

Standardizes and cleans raw data before modelling.

Transformations performed:

- consistent column naming
- datatype conversions
- timestamp formatting
- null handling
- removal of duplicates

Objects created:

- staging.stg_users
- staging.stg_orders
- staging.stg_products
- staging.stg_events

Staging layer acts as clean source for analytics layer.

---

## Step 3 — ANALYTICS Layer

Run:

analytics/03_analytics_layer.sql

Purpose:

Creates reporting-ready tables optimized for Power BI.

Objects created:

### Dimension tables

dim_users
- user attributes
- signup_date
- plan_type
- country

dim_products
- product metadata
- category

---

### Fact tables

fct_orders
- order transactions
- revenue metrics
- product level sales

fct_user_funnel
- tracks customer journey stages:
  product view → add to cart → checkout → purchase

---

### Aggregated tables

agg_daily_revenue
- daily revenue totals
- order counts
- revenue trends

agg_product_performance
- product revenue
- product profitability
- units sold

---

## Step 4 — BUSINESS QUERY Layer

Run SQL files in:

queries/

Queries support Power BI metrics:

- Month-over-Month Revenue Growth %
- Cohort analysis
- Funnel drop-off by device
- At-risk customers
- Top products by revenue

---

## Execution Order (Important)

Run scripts in this order:

1. raw/01_raw_layer.sql
2. staging/02_staging_layer.sql
3. analytics/03_analytics_layer.sql
4. queries files
5. open Power BI dashboard and refresh data

Running scripts out of order may result in missing dependencies.


Link to Live Dashboard -- [https://app.powerbi.com](https://app.powerbi.com/links/qllUmxoADD?ctid=2e30fe38-be4c-4a4a-a7e9-35b0cad54323&pbi_source=linkShare)
