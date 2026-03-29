-- Script  : 03_analytics_layer
-- Database: analytics_db  |  Connect to: Built-in
-- Purpose : Analytical models built on top of staging layer.
--           NEVER reads directly from raw.* tables.
-- Models  : analytics.fct_orders
-- ============================================================


-- ============================================================
-- MODEL 1: analytics.fct_orders
-- One row per order, enriched with product + user dimensions
-- Source : staging.stg_orders (base)
--          JOIN staging.stg_products ON product_id
--          JOIN staging.stg_users    ON user_id
-- Revenue: gross_revenue, net_revenue, profit_margin
-- Status : Normalized into business-friendly category labels
-- Perf   : Logical partition columns: created_at, region
-- ============================================================

CREATE OR ALTER VIEW analytics.fct_orders AS
SELECT

    -- --------------------------------------------------------
    -- PRIMARY KEYS & FOREIGN KEYS
    -- --------------------------------------------------------
    o.order_id,
    o.user_id,
    o.product_id,

    -- --------------------------------------------------------
    -- ORDER DETAILS
    -- --------------------------------------------------------
    o.quantity,
    o.unit_price,
    o.discount_amount,

    -- --------------------------------------------------------
    -- REVENUE CALCULATIONS
    -- gross_revenue : total revenue before any discounts
    -- net_revenue   : revenue after discount is applied
    -- profit_margin : (net_revenue - cost) / net_revenue * 100
    -- --------------------------------------------------------
    CAST(o.quantity * o.unit_price AS FLOAT)
        AS gross_revenue,

    CAST((o.quantity * o.unit_price) - o.discount_amount AS FLOAT)
        AS net_revenue,

    CASE
        WHEN ((o.quantity * o.unit_price) - o.discount_amount) = 0
            THEN 0.0
        ELSE
            ROUND(
                (
                    ((o.quantity * o.unit_price) - o.discount_amount)
                    - (o.quantity * p.cost_price)
                )
                / ((o.quantity * o.unit_price) - o.discount_amount)
                * 100,
                2
            )
    END AS profit_margin,

    -- --------------------------------------------------------
    -- ORDER STATUS: Normalized into business-friendly labels
    -- Raw values from staging are already UPPER-cased
    -- --------------------------------------------------------
    o.status AS status_raw,
    CASE o.status
        WHEN 'DELIVERED'  THEN 'Completed'
        WHEN 'SHIPPED'    THEN 'In Transit'
        WHEN 'PROCESSING' THEN 'Pending'
        WHEN 'CANCELLED'  THEN 'Cancelled'
        WHEN 'RETURNED'   THEN 'Returned'
        ELSE                   'Unknown'
    END AS status_category,

    -- --------------------------------------------------------
    -- PRODUCT DIMENSION (from staging.stg_products)
    -- --------------------------------------------------------
    p.product_name,
    p.category          AS product_category,
    p.subcategory       AS product_subcategory,
    p.base_price        AS product_base_price,
    p.cost_price        AS product_cost_price,

    -- --------------------------------------------------------
    -- USER DIMENSION (from staging.stg_users)
    -- --------------------------------------------------------
    u.plan_type         AS user_plan_type,
    u.country           AS user_country,
    u.signup_date       AS user_signup_date,
    u.is_active         AS user_is_active,

    -- --------------------------------------------------------
    -- TIME & PARTITIONING DIMENSIONS
    -- Logical partition columns: created_at, region
    -- Used for query pruning in downstream analytics queries
    -- --------------------------------------------------------
    o.created_at,
    CAST(o.created_at AS DATE)          AS order_date,
    YEAR(o.created_at)                  AS order_year,
    MONTH(o.created_at)                 AS order_month,
    DAY(o.created_at)                   AS order_day,
    DATENAME(WEEKDAY, o.created_at)     AS order_day_name,
    DATEPART(QUARTER, o.created_at)     AS order_quarter,

    o.region

FROM staging.stg_orders o

LEFT JOIN staging.stg_products p
    ON o.product_id = p.product_id

LEFT JOIN staging.stg_users u
    ON o.user_id = u.user_id;

-- ============================================================
-- VERIFY: Run this to confirm the view works correctly
SELECT TOP 10 * FROM analytics.fct_orders
ORDER BY created_at DESC;
-- ============================================================


-- ============================================================
-- SECTION 2.2: analytics.dim_users
-- User dimension table with lifecycle & segmentation fields
-- ============================================================
CREATE OR ALTER VIEW analytics.dim_users AS

SELECT

    -- identity
    u.user_id,
    
    -- since name and email are PII so not available here
    -- attributes
    u.country,
    u.plan_type,
    u.signup_date,
    u.is_active,

    -- days since signup
    DATEDIFF(
        DAY,
        u.signup_date,
        CAST(GETDATE() AS DATE)
    ) AS customer_age_days,

    -- cohort month
    DATEFROMPARTS(
        YEAR(u.signup_date),
        MONTH(u.signup_date),
        1
    ) AS cohort_month,

    -- plan segmentation
    CASE
        WHEN u.plan_type = 'enterprise'
            THEN 'Enterprise'

        WHEN u.plan_type = 'pro'
            THEN 'Premium'

        WHEN u.plan_type = 'starter'
            THEN 'Basic'

        WHEN u.plan_type = 'free'
            THEN 'Free'

        ELSE 'Unknown'
    END AS plan_tier_segment

FROM staging.stg_users u;

-- ============================================================
-- VERIFY: Run this to confirm the view works correctly
 SELECT TOP 10 * FROM analytics.dim_users
 ORDER BY signup_date DESC;
-- ============================================================


-- ============================================================
-- SECTION 2.3: analytics.dim_products
-- Product dimension table with category hierarchy
-- and margin classification
-- ============================================================

CREATE OR ALTER VIEW analytics.dim_products
AS SELECT 

p.product_id,
p.product_name,

-- category heirarchy
p.category,
p.subcategory,

-- margin classification
(p.base_price-p.cost_price)/p.base_price as margin_ratio,

CASE WHEN
(p.base_price-p.cost_price)/p.base_price>=0.50 THEN 'High'
WHEN
(p.base_price-p.cost_price)/p.base_price>=0.25 THEN 'Medium'
ELSE
'Low'
END AS margin_class

FROM staging.stg_products p;


-- ============================================================
-- VERIFY: Run this to confirm the view works correctly
 SELECT TOP 10 * FROM analytics.dim_products
 ORDER BY margin_ratio DESC;
-- ============================================================


-- ============================================================
-- SECTION 2.4: analytics.agg_daily_revenue
-- Daily aggregated revenue summary
-- Dimensions : order_date x region x plan_type
-- Metrics     : total_orders, gross_revenue, net_revenue, AOV
--
-- MODEL TYPE  : Materialized View (Dedicated SQL Pool)
--
-- REFRESH JUSTIFICATION
-- Revenue dashboards are typically consumed once per business
-- day (morning stand-ups, overnight batch reports). Source data
-- in staging.stg_orders is itself loaded via a nightly ELT
-- pipeline, so a daily refresh (00:30 UTC, after ELT completes)
-- is sufficient and avoids redundant recomputation.
-- If near-real-time SLA is ever required, switch the pipeline
-- to an hourly incremental MERGE + scheduled refresh.
-- ============================================================

-- NOTE: Dedicated SQL Pool required for MATERIALIZED VIEW.
-- In Serverless SQL (Built-in), use a regular VIEW or CETAS.
-- The block below uses MATERIALIZED VIEW syntax for Dedicated Pool.

IF OBJECT_ID('analytics.agg_daily_revenue', 'V') IS NOT NULL
    DROP VIEW analytics.agg_daily_revenue;
GO

-- ---- Dedicated SQL Pool: replace VIEW with MATERIALIZED VIEW ----
-- CREATE MATERIALIZED VIEW analytics.agg_daily_revenue
-- WITH (DISTRIBUTION = HASH(order_date))
-- AS
-- -----------------------------------------------------------------

CREATE OR ALTER VIEW analytics.agg_daily_revenue
AS SELECT 
    fo.order_date,
    fo.region,
    fo.user_plan_type,

    COUNT(fo.order_id) AS order_count,
    SUM(fo.gross_revenue) as total_gross_revenue,
    SUM(fo.net_revenue) as total_net_revenue,
    AVG(fo.net_revenue) as Avg_net_revenue

 FROM analytics.fct_orders fo
 GROUP BY
 fo.order_date,
 fo.region,
 fo.user_plan_type;   
-- ============================================================
-- VERIFY: Run this to confirm the view works correctly
 SELECT TOP 10 *
 FROM   analytics.agg_daily_revenue
 ORDER  BY order_date DESC, total_gross_revenue DESC;
-- ============================================================

--## 2.5 `analytics.agg_product_performance`
--A product-level performance summary:
--- Total units sold, revenue, and profit per product over the **last 30 days**, **last 90 days**, and **all time**
--- Use window functions or CTEs — do not create separate tables for each time window

CREATE OR ALTER VIEW analytics.agg_product_performance AS
SELECT
fo.product_id,
fo.product_name,
-- LAST 30 DAYS
SUM(
CASE
WHEN fo.order_date >= DATEADD(day,-30,CAST(GETDATE() AS DATE))
THEN fo.quantity
ELSE 0
END
) AS units_last_30d,

SUM(
CASE
WHEN fo.order_date >= DATEADD(day,-30,CAST(GETDATE() AS DATE))
THEN fo.net_revenue
ELSE 0
END
) AS revenue_last_30d,

SUM(
CASE
WHEN fo.order_date >= DATEADD(day,-30,CAST(GETDATE() AS DATE))
THEN fo.net_revenue - (fo.quantity * fo.product_cost_price)
ELSE 0
END
) AS profit_last_30d,

-- LAST 90 DAYS
SUM(
CASE
WHEN fo.order_date >= DATEADD(day,-90,CAST(GETDATE() AS DATE))
THEN fo.quantity
ELSE 0
END
) AS units_last_90d,

SUM(
CASE
WHEN fo.order_date >= DATEADD(day,-90,CAST(GETDATE() AS DATE))
THEN fo.net_revenue
ELSE 0
END
) AS revenue_last_90d,

SUM(
CASE
WHEN fo.order_date >= DATEADD(day,-90,CAST(GETDATE() AS DATE))
THEN fo.net_revenue - (fo.quantity * fo.product_cost_price)
ELSE 0
END
) AS profit_last_90d,

-- ALL TIME
SUM(fo.quantity) AS units_all_time,
SUM(fo.net_revenue) AS revenue_all_time,
SUM(fo.net_revenue - (fo.quantity * fo.product_cost_price)) AS profit_all_time
FROM analytics.fct_orders fo
GROUP BY product_id,product_name;

-- ============================================================
-- VERIFY: Run this to confirm the view works correctly
 SELECT TOP 5 *
 FROM   analytics.agg_product_performance;

-- ============================================================

--### 2.6 `analytics.fct_user_funnel`
--A funnel analysis model using `stg_events`:
--- For each user, capture the **first occurrence** of each funnel step: `product_view → add_to_cart → checkout_start → purchase`
--- Calculate time-to-convert (seconds) between each step
--- Flag users who dropped off at each stage

CREATE OR ALTER VIEW analytics.fct_user_funnel AS

WITH first_events AS (
SELECT
    e.user_id,
    -- device used when user first viewed product
    MIN(
        CASE 
            WHEN e.event_type = 'productview'
            THEN e.device_type
        END
    ) AS device_type,

    -- first time user viewed product
    MIN(
        CASE 
            WHEN e.event_type = 'productview'
            THEN e.created_at
        END
    ) AS first_view_time,

    -- first time user added to cart
    MIN(
        CASE 
            WHEN e.event_type = 'addtocart'
            THEN e.created_at
        END
    ) AS first_cart_time,

    -- first time checkout started
    MIN(
        CASE 
            WHEN e.event_type = 'checkoutstart'
            THEN e.created_at
        END
    ) AS first_checkout_time,

    -- first time purchase completed
    MIN(
        CASE 
            WHEN e.event_type = 'purchase'
            THEN e.created_at
        END
    ) AS first_purchase_time
FROM staging.stg_events e
GROUP BY 
    e.user_id
)
SELECT
    user_id,
    device_type,
    first_view_time,
    first_cart_time,
    first_checkout_time,
    first_purchase_time,
    -- time taken between funnel steps

    DATEDIFF(second, first_view_time, first_cart_time)
        AS seconds_view_to_cart,
    DATEDIFF(second, first_cart_time, first_checkout_time)
        AS seconds_cart_to_checkout,
    DATEDIFF(second, first_checkout_time, first_purchase_time)
        AS seconds_checkout_to_purchase,
        
    -- drop-off flags

    CASE
        WHEN first_view_time IS NOT NULL
             AND first_cart_time IS NULL
        THEN 1 ELSE 0
    END AS dropped_after_view,

    CASE
        WHEN first_cart_time IS NOT NULL
             AND first_checkout_time IS NULL
        THEN 1 ELSE 0
    END AS dropped_after_cart,

    CASE
        WHEN first_checkout_time IS NOT NULL
             AND first_purchase_time IS NULL
        THEN 1 ELSE 0
    END AS dropped_after_checkout

FROM first_events;

--validating the view
SELECT *
FROM analytics.fct_user_funnel
ORDER BY first_view_time DESC;

Select event_type from staging.stg_events;

SELECT 
user_id,
event_type,
created_at
FROM staging.stg_events
ORDER BY created_at;
