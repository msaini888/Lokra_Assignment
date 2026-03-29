-- ============================================================
-- STAGING LAYER (Task 1)
-- Script  : 02_staging_layer
-- Database: analytics_db  |  Connect to: Built-in
-- Purpose : Clean, cast, filter and mask raw source data.
--           All 4 views read from raw.* tables ONLY.
--           PII (email, name) must NOT appear beyond this layer.
-- Status  : ALL VIEWS ALREADY CREATED - kept here for reference.
-- ============================================================


-- ============================================================
-- VIEW 1: staging.stg_orders
-- Source : raw.orders
-- Transformation decisions:
--   - Cast order_id, user_id, product_id to NVARCHAR(50)
--     for consistent string handling across joins
--   - Cast quantity to INT (source is VARCHAR in CSV)
--   - Cast unit_price, discount_amount to FLOAT
--   - UPPER(status) normalises values e.g. 'delivered' -> 'DELIVERED'
--     ensures consistent filtering in analytics layer
--   - Cast created_at to DATETIME2 for proper date partitioning
--   - WHERE order_id IS NOT NULL removes incomplete/corrupt rows
-- ============================================================

CREATE VIEW staging.stg_orders AS
SELECT
    CAST(order_id AS NVARCHAR(50)) AS order_id,
    CAST(user_id AS NVARCHAR(50)) AS user_id,
    CAST(product_id AS NVARCHAR(50)) AS product_id,

    UPPER(status) AS status,   -- normalize text values

    quantity,
    unit_price,
    discount_amount,

    CAST(created_at AS DATETIME2) AS created_at,
    CAST(region AS NVARCHAR(10)) AS region

FROM raw.orders
WHERE order_id IS NOT NULL;
--(Already created - kept here for reference only)

Select  TOP 5 * from  staging.stg_orders;

-- ============================================================
-- VIEW 2: staging.stg_users
-- Source : raw.users
-- Transformation decisions:
--   - email column EXCLUDED entirely: PII field per assignment rules
--     must not appear in any model beyond this staging layer
--   - name column EXCLUDED entirely: PII field, same rule
--   - Cast signup_date to DATE (used for cohort calculations later)
--   - CASE WHEN converts 'true'/'false' string from CSV to BIT
--     (1 = active, 0 = inactive) for easier filtering
--   - WHERE user_id IS NOT NULL removes rows with no identifier
-- ============================================================

CREATE OR ALTER VIEW staging.stg_users AS
SELECT
    CAST(user_id AS NVARCHAR(50)) AS user_id,
    LOWER(plan_type) AS plan_type,
    CAST(signup_date AS DATE) AS signup_date,
    UPPER(country) AS country,
    CAST(created_at AS DATETIME2) AS created_at,
    CAST(CASE
        WHEN LOWER(is_active) = 'true' THEN 1
        ELSE 0
    END AS BIT) AS is_active
FROM raw.users
WHERE user_id IS NOT NULL;
-- (Already created - kept here for reference only)

Select * from staging.stg_users;

-- ============================================================
-- VIEW 3: staging.stg_products
-- Source : raw.products
-- Transformation decisions:
--   - No PII in this table, all columns retained
--   - Renamed 'name' to 'product_name' to avoid reserved word
--     conflicts and improve clarity in downstream joins
--   - Cast base_price, cost_price to FLOAT for margin calculations
--   - CASE WHEN converts is_active string to BIT (1/0)
--   - WHERE product_id IS NOT NULL removes incomplete rows
-- ============================================================

CREATE VIEW staging.stg_products AS
SELECT
    CAST(product_id AS NVARCHAR(50)) AS product_id,
    name AS product_name,
    UPPER(category) AS category,
    UPPER(subcategory) AS subcategory,
    base_price,
    cost_price,
    CAST(created_at AS DATETIME2) AS created_at,
    CASE
        WHEN LOWER(is_active) = 'true' THEN 1
        ELSE 0
    END AS is_active
FROM raw.products
WHERE product_id IS NOT NULL;
-- (Already created - kept here for reference only)

-- ============================================================
-- VIEW 4: staging.stg_events
-- Source : raw.events
-- Transformation decisions:
--   - raw.events can have DUPLICATE event_ids due to at-least-once
--     delivery in the ingestion pipeline (stated in assignment)
--   - ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY created_at)
--     assigns rank 1 to the earliest occurrence of each event_id
--   - Outer WHERE row_num = 1 keeps only first occurrence
--     this is the deduplication strategy
--   - product_id kept nullable (can be NULL per assignment spec)
--   - All string casts applied for type consistency
--   - WHERE event_id IS NOT NULL in inner query removes bad rows
-- ============================================================

CREATE OR ALTER VIEW staging.stg_events AS
WITH DEDUPED AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY TRY_CAST(created_at AS DATETIME2) DESC
        ) AS row_num
    FROM raw.events
    WHERE event_id IS NOT NULL
)
SELECT
    event_id,
    user_id,
    session_id,
    LOWER(event_type) AS event_type,
    product_id,
    page,
    referrer,
    device_type,
    -- Simple force cast
    TRY_CAST(created_at AS DATETIME2) AS created_at
FROM DEDUPED
WHERE row_num = 1;

-- ============================================================
-- VERIFY: Run these to confirm all 4 staging views work
-- Uncomment one at a time and click Run
-- ============================================================

SELECT TOP 5 * FROM staging.stg_orders;
SELECT TOP 5 * FROM staging.stg_users;
SELECT TOP 5 * FROM staging.stg_products;
SELECT TOP 5 * FROM staging.stg_events;


-- ============================================================
-- NEXT STEPS: analytics layer 
-- ============================================================
-- analytics.fct_orders          - fact table, revenue calcs
-- analytics.dim_users           - user dimension, cohort month
-- analytics.dim_products        - product dimension, margin class
-- analytics.agg_daily_revenue   - daily revenue (materialized)
-- analytics.agg_product_performance - product performance 30/90/all
-- analytics.fct_user_funnel     - funnel drop-off analysis
-- ============================================================