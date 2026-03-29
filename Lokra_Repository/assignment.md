# Analytics Engineering Assignment — Senior Developer

## Important Notice — Academic Integrity

> **The use of AI tools (including but not limited to GitHub Copilot, ChatGPT, Claude, Gemini, Cursor, Tabnine, or any other AI-assisted coding tool) is strictly prohibited for this assignment.**
>
> All submitted work must be entirely your own — queries, models, designs, and documentation.
>
> **Any submission found to contain AI-generated content will be immediately disqualified.** We actively screen submissions for AI-generated patterns. By submitting, you confirm the work is solely your own.

---

## Overview

You are joining the **Analytics Engineering** team. Your responsibility is to build the analytics layer that powers the product dashboard — without ever modifying or writing to the **source/raw tables**.

This assignment evaluates your ability to:
- Design a clean, scalable analytics data model
- Write performant SQL for large datasets
- Make the right architectural choices on a cloud analytics platform
- Surface meaningful insights through a dashboard

---

## Platform Choice

Choose **one** of the following platforms for your submission:

| Platform | Relevant Services |
|---|---|
| **Snowflake** | Snowflake (Warehouse, Views, Materialized Views, Tasks, Streams) |
| **Google BigQuery** | BigQuery (Datasets, Views, Materialized Views, Scheduled Queries, Partitioning, Clustering) |
| **Azure** | Azure Synapse Analytics + Azure Data Factory + Power BI / Azure Data Studio |

> You must justify your platform choice in your submission. Explain why it is suitable for a dataset at the scale described below.

---

## Business Context & Data Scenario

You are working for a **SaaS e-commerce platform**. The engineering team has set up raw ingestion pipelines and the following **source tables** are available in the `raw` schema (or dataset/database depending on your platform). These are append-only ingestion tables and **must never be altered, truncated, or written to**.

### Source Tables (Read-Only)

#### `raw.orders`
| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Unique order identifier |
| `user_id` | STRING | Reference to the customer |
| `product_id` | STRING | Reference to the product |
| `status` | STRING | `pending`, `confirmed`, `shipped`, `delivered`, `cancelled`, `refunded` |
| `quantity` | INTEGER | Number of units ordered |
| `unit_price` | FLOAT | Price per unit at time of order |
| `discount_amount` | FLOAT | Discount applied to the order |
| `created_at` | TIMESTAMP | Order creation time |
| `updated_at` | TIMESTAMP | Last status update time |
| `region` | STRING | Geographic region (`NA`, `EU`, `APAC`, `LATAM`) |

#### `raw.users`
| Column | Type | Description |
|---|---|---|
| `user_id` | STRING | Unique user identifier |
| `email` | STRING | User email (PII) |
| `name` | STRING | Full name (PII) |
| `plan_type` | STRING | `free`, `starter`, `pro`, `enterprise` |
| `signup_date` | DATE | Date of account creation |
| `country` | STRING | ISO country code |
| `is_active` | BOOLEAN | Whether the account is currently active |
| `created_at` | TIMESTAMP | Row creation timestamp |

#### `raw.products`
| Column | Type | Description |
|---|---|---|
| `product_id` | STRING | Unique product identifier |
| `name` | STRING | Product name |
| `category` | STRING | Product category |
| `sub_category` | STRING | Product sub-category |
| `base_price` | FLOAT | Standard listed price |
| `cost_price` | FLOAT | Internal cost (used for margin calculation) |
| `is_active` | BOOLEAN | Whether the product is currently listed |
| `created_at` | TIMESTAMP | Row creation timestamp |

#### `raw.events`
| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | Unique event identifier |
| `user_id` | STRING | User who triggered the event |
| `session_id` | STRING | Browser/app session |
| `event_type` | STRING | e.g., `page_view`, `product_view`, `add_to_cart`, `checkout_start`, `purchase`, `search` |
| `product_id` | STRING | Associated product (nullable) |
| `page` | STRING | Page or screen name |
| `referrer` | STRING | Traffic source / referrer URL |
| `device_type` | STRING | `mobile`, `tablet`, `desktop` |
| `created_at` | TIMESTAMP | Event timestamp |

> **Scale context:** Assume `raw.orders` contains **500 million+ rows**, `raw.events` contains **5 billion+ rows**, and both grow by millions of rows daily. Design with this scale in mind.

---

## Assignment Tasks

### Task 1 — Staging Layer (Bronze → Silver)

Create a **staging layer** in a schema/dataset named `staging`. This layer should:

- Select only necessary columns from raw tables
- Cast and normalize data types
- Apply basic data quality filters (e.g., exclude rows with null primary keys)
- Deduplicate records where needed (e.g., `raw.events` may have duplicates due to at-least-once delivery)
- **Mask or exclude PII columns** (`email`, `name`) from all downstream layers — these must not appear in any view beyond staging

**Deliverables:**
- `staging.stg_orders`
- `staging.stg_users` (PII masked/excluded)
- `staging.stg_products`
- `staging.stg_events`

For each staging view/table, include a **comment** explaining any transformation decisions made.

---

### Task 2 — Analytics / Mart Layer (Silver → Gold)

Create the following **analytical models** in a schema/dataset named `analytics`. These models must be built on top of the `staging` layer only — never directly on `raw`.

#### 2.1 `analytics.fct_orders`
A facts table with one row per order enriched with product and user dimensions. Must include:
- Revenue calculations: `gross_revenue`, `net_revenue` (after discount), `profit_margin`
- Order status normalized/categorized
- Partitioned/clustered by `created_at` and `region` for query performance

#### 2.2 `analytics.dim_users`
A user dimension table. Must include:
- Days since signup (`customer_age_days`)
- Cohort month derived from `signup_date`
- Plan tier segmentation

#### 2.3 `analytics.dim_products`
A product dimension table with category hierarchy and margin classification (high / mid / low based on `(base_price - cost_price) / base_price`).

#### 2.4 `analytics.agg_daily_revenue`
A daily aggregated revenue summary with:
- Total orders, gross revenue, net revenue, and average order value — broken down by `date`, `region`, and `plan_type`
- This model should be a **Materialized View** (or equivalent) refreshed on a schedule — justify the refresh frequency

#### 2.5 `analytics.agg_product_performance`
A product-level performance summary:
- Total units sold, revenue, and profit per product over the **last 30 days**, **last 90 days**, and **all time**
- Use window functions or CTEs — do not create separate tables for each time window

#### 2.6 `analytics.fct_user_funnel`
A funnel analysis model using `stg_events`:
- For each user, capture the **first occurrence** of each funnel step: `product_view → add_to_cart → checkout_start → purchase`
- Calculate time-to-convert (seconds) between each step
- Flag users who dropped off at each stage

---

### Task 3 — Advanced SQL Challenge

Write standalone SQL queries (do not create permanent objects) for the following:

#### 3.1 — Month-over-Month Revenue Growth
Return a result set showing each month's total net revenue and its percentage change compared to the previous month, for the last 12 months. Use window functions.

#### 3.2 — Cohort Retention Analysis
Using signup cohort month, calculate the **percentage of users who placed at least one order** in their:
- Month 0 (signup month)
- Month 1, Month 2, Month 3 (months after signup)

Output a cohort retention matrix table.

#### 3.3 — Top Products by Region with Ranking
For each region, return the **top 5 products by net revenue** in the last 90 days. Include rank, product name, category, and revenue. Handle ties correctly.

#### 3.4 — Identifying At-Risk Customers
Define an "at-risk" customer as a `pro` or `enterprise` plan user who:
- Has placed at least 3 orders historically, AND
- Has not placed any order in the last 60 days

Write a query to return these users with their last order date and total lifetime revenue.

#### 3.5 — Funnel Drop-off Rate
Using `analytics.fct_user_funnel`, calculate the drop-off rate (%) at each step of the funnel, broken down by `device_type`.

---

### Task 4 — Performance & Scalability Design

Write a short technical document (`DESIGN.md`) covering:

1. **Partitioning & Clustering Strategy**
   - Which columns did you choose to partition/cluster on in `fct_orders` and `stg_events`? Why?
   - How does this reduce query cost and improve performance at 5B+ rows?

2. **Materialized Views vs. Regular Views**
   - When did you choose a Materialized View over a regular View in your solution?
   - What are the trade-offs in terms of freshness, cost, and storage on your chosen platform?

3. **Incremental Processing**
   - How would you make `analytics.fct_orders` process incrementally rather than doing a full refresh daily?
   - Describe the pattern (e.g., merge/upsert, insert-overwrite partition, Snowflake Streams) you would use.

4. **Query Cost Optimization**
   - Identify one query in your solution that could be expensive at scale.
   - Explain the optimization technique you applied or would apply (pruning, approximate aggregation, pre-aggregation, etc.).

5. **Data Freshness SLA**
   - The dashboard requires **revenue data to be no more than 1 hour stale**.
   - Propose an architecture (scheduling, streaming, or micro-batch) to meet this SLA on your chosen platform.

---

### Task 5 — Dashboard

Build a dashboard using one of the following tools:

| Tool | Notes |
|---|---|
| **Power BI** | Connect to Snowflake / Azure Synapse / BigQuery |
| **Looker Studio** | Free, connects natively to BigQuery |
| **Metabase** | Open-source, supports all three platforms |
| **Tableau Public** | Free tier available |
| **Preset / Apache Superset** | Open-source option |

The dashboard must consume data **only from the `analytics` layer** (never directly from `raw` or `staging`).

#### Required Dashboard Panels

| Panel | Source Model | Chart Type |
|---|---|---|
| Total Revenue (MTD vs prior MTD) | `agg_daily_revenue` | KPI card with delta |
| Daily Revenue Trend (last 90 days) | `agg_daily_revenue` | Line chart |
| Revenue by Region | `fct_orders` | Bar or donut chart |
| MoM Revenue Growth % | Custom query / `agg_daily_revenue` | Line chart |
| Top 10 Products by Net Revenue | `agg_product_performance` | Horizontal bar |
| Funnel Drop-off by Device | `fct_user_funnel` | Funnel chart |
| At-Risk Customer Count by Plan | Custom query | KPI card |
| Cohort Retention Heatmap | Custom query | Heatmap / matrix |

All panels must include:
- A clear title and axis labels
- Date filter applicable across the dashboard
- Region filter (where applicable)

---

## Deliverables & Submission

### Repository Structure

```
analytics-assignment/
├── staging/
│   ├── stg_orders.sql
│   ├── stg_users.sql
│   ├── stg_products.sql
│   └── stg_events.sql
├── analytics/
│   ├── fct_orders.sql
│   ├── dim_users.sql
│   ├── dim_products.sql
│   ├── agg_daily_revenue.sql
│   ├── agg_product_performance.sql
│   └── fct_user_funnel.sql
├── queries/
│   ├── mom_revenue_growth.sql
│   ├── cohort_retention.sql
│   ├── top_products_by_region.sql
│   ├── at_risk_customers.sql
│   └── funnel_dropoff_by_device.sql
├── DESIGN.md
├── DASHBOARD_SCREENSHOTS/
│   └── (screenshots or exported PDF of the dashboard)
└── README.md              ← Setup, platform choice, assumptions
```

### README Must Include

- Platform chosen and justification
- How to run/deploy all SQL objects (setup order matters)
- Any assumptions made about data quality or business logic
- Link to the **live dashboard** (shared/public view) if available

---

## Evaluation Criteria

| Criteria | Weight |
|---|---|
| Correctness of SQL logic and transformations | 25% |
| Data model design (layering, naming, separation of concerns) | 20% |
| Performance and scalability decisions (partitioning, clustering, incremental) | 20% |
| Advanced SQL proficiency (window functions, CTEs, aggregations) | 15% |
| Dashboard quality and insight clarity | 10% |
| DESIGN.md depth and architectural reasoning | 10% |

---

## Key Constraints (Strictly Enforced)

- **Do not write to, alter, truncate, or drop any table in the `raw` schema/dataset.**
- All downstream models must be built via **Views, Materialized Views, or separate tables** derived from `staging` or `analytics` layers only.
- PII fields (`email`, `name`) must not appear in any model beyond the `staging` layer.
- SQL must be written for your chosen platform's dialect (BigQuery Standard SQL, Snowflake SQL, or T-SQL for Synapse).
- No use of `SELECT *` in any final model — all columns must be explicitly listed.

---

**Time Estimate:** 6–10 hours  
**Deadline:** 7 days from receipt of this assignment
