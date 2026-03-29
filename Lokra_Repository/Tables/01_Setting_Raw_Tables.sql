-- ============================================================
-- ANALYTICS ENGINEERING ASSIGNMENT - FULL SETUP SCRIPT
-- Platform  : Azure Synapse Analytics (Serverless SQL Pool)
-- Database  : analytics_db
-- Author    : Mohit Saini
-- Description: This script sets up the complete raw layer.
--              Run each section ONE AT A TIME in order.
--              Make sure 'Connect to' = Built-in throughout.
-- ============================================================


-- ============================================================
-- SECTION 1: CREATE DATABASE
-- Run this while 'Use database' = master
-- A database is the top-level container for all our objects.
-- ============================================================

-- CREATE DATABASE analytics_db;
-- (Already created - kept here for reference only)


-- ============================================================
-- SECTION 2: CREATE SCHEMAS
-- Run these while 'Use database' = analytics_db
-- Schemas are sub-folders inside the database.
-- raw      = source data, read-only, never modified
-- staging  = cleaned and filtered version of raw
-- analytics = final models for dashboards and queries
-- ============================================================

CREATE SCHEMA raw;       
CREATE SCHEMA staging;   
CREATE SCHEMA analytics; 


-- ============================================================
-- SECTION 3: CREATE EXTERNAL DATA SOURCE
-- This is a named shortcut pointing to the Azure Data Lake
-- container where our CSV files are stored.
-- URL format: https://<storage-account>.dfs.core.windows.net/<container>
-- ============================================================

CREATE EXTERNAL DATA SOURCE DataLakeData
WITH (
    LOCATION = 'https://mohitsynapsestorage123.dfs.core.windows.net/data'
);


-- ============================================================
-- SECTION 4: CREATE FILE FORMAT
-- Tells Synapse how to read the CSV files:
--   FIELD_TERMINATOR = comma separates each column
--   STRING_DELIMITER = double quotes wrap text values
--   FIRST_ROW = 2     means skip row 1 (the header row)
-- ============================================================

CREATE EXTERNAL FILE FORMAT CsvWithHeader
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);
-- (Already created - kept here for reference only)


-- ============================================================
-- SECTION 5: CREATE EXTERNAL TABLES (raw layer)
-- External tables are NOT physical tables.
-- They are windows that read data directly from CSV files.
-- Data stays in the CSV - Synapse reads it on demand.
-- Each LOCATION points to a folder inside the data container.
-- ============================================================

-- TABLE 1: raw.orders
-- Contains one row per customer order.
-- Primary key: order_id
-- Foreign keys: user_id -> raw.users, product_id -> raw.products

CREATE EXTERNAL TABLE raw.orders (
    order_id        VARCHAR(50),   -- unique order identifier
    user_id         VARCHAR(50),   -- which customer placed the order
    product_id      VARCHAR(50),   -- which product was ordered
    status          VARCHAR(20),   -- pending/confirmed/shipped/delivered/cancelled/refunded
    quantity        INT,           -- number of units ordered
    unit_price      FLOAT,         -- price per unit at time of order
    discount_amount FLOAT,         -- discount applied
    created_at      DATETIME2,     -- when the order was placed
    region          VARCHAR(10)    -- NA / EU / APAC / LATAM
)
WITH (
    LOCATION    = '/raw/orders/',
    DATA_SOURCE = DataLakeData,
    FILE_FORMAT = CsvWithHeader
);


-- TABLE 2: raw.users
-- Contains one row per registered user.
-- Primary key: user_id
-- NOTE: email and name are PII - must NOT appear beyond staging layer

CREATE EXTERNAL TABLE raw.users (
    user_id     VARCHAR(50),    -- unique user identifier
    email       VARCHAR(200),   -- PII - masked in staging
    name        VARCHAR(200),   -- PII - excluded in staging
    plan_type   VARCHAR(20),    -- free / starter / pro / enterprise
    signup_date DATE,           -- date user registered
    country     VARCHAR(10),    -- ISO country code
    created_at  DATETIME2,      -- row creation timestamp
    is_active   VARCHAR(10)     -- true / false
)
WITH (
    LOCATION    = '/raw/users/',
    DATA_SOURCE = DataLakeData,
    FILE_FORMAT = CsvWithHeader
);

--drop EXTERNAL table raw.users;
--select * from raw.users;

-- TABLE 3: raw.products
-- Contains one row per product in the catalog.
-- Primary key: product_id

CREATE EXTERNAL TABLE raw.products (
    product_id   VARCHAR(50),   -- unique product identifier
    name         VARCHAR(200),  -- product name
    category     VARCHAR(100),  -- top-level category e.g. Electronics
    subcategory  VARCHAR(100),  -- sub-level e.g. Audio
    base_price   FLOAT,         -- standard listed price
    cost_price   FLOAT,         -- internal cost for margin calculation
    created_at   DATETIME2,     -- row creation timestamp
    is_active    VARCHAR(10)    -- true / false
)
WITH (
    LOCATION    = '/raw/products/',
    DATA_SOURCE = DataLakeData,
    FILE_FORMAT = CsvWithHeader
);

--select * from raw.products;
--drop external table raw.products;

-- TABLE 4: raw.events
-- Contains one row per user behaviour event (pageview, click, purchase etc.)
-- Primary key: event_id
-- Foreign keys: user_id -> raw.users, product_id -> raw.products
-- NOTE: This table can have duplicates due to at-least-once delivery

CREATE EXTERNAL TABLE raw.events (
    event_id    VARCHAR(50),    -- unique event identifier
    user_id     VARCHAR(50),    -- which user triggered the event
    session_id  VARCHAR(50),    -- browser/app session
    event_type  VARCHAR(50),    -- productview/addtocart/checkoutstart/purchase/search
    product_id  VARCHAR(50),    -- associated product (nullable)
    page        VARCHAR(200),   -- page or screen name
    referrer    VARCHAR(500),   -- traffic source URL
    device_type VARCHAR(20),    -- mobile / tablet / desktop
    created_at  VARCHAR(50)       -- event timestamp
)
WITH (
    LOCATION    = '/raw/events/',
    DATA_SOURCE = DataLakeData,
    FILE_FORMAT = CsvWithHeader
);

Select * from raw.events;

-- ============================================================
-- SECTION 6: VERIFY DATA IS READABLE
-- Run these SELECT queries to confirm each table works.
-- All 4 should return rows from their respective CSV files.
-- ============================================================

SELECT TOP 5 * FROM raw.orders;
SELECT TOP 5 * FROM raw.users;
SELECT TOP 5 * FROM raw.products;
SELECT TOP 5 * FROM raw.events;


-- 1. Create the 'Keychain' (StorageKeyCredential) and put your key inside it
CREATE DATABASE SCOPED CREDENTIAL [StorageKeyCredential]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'gS7yWqfU00jiWXCA1Tsg1pR8AR7nSvnRrvIADlJyZ/oGXqyYRdnTKAD1D0FS3tkn8OBTbiam7Y3s+AStPTXC7w==';

-- 2. Tell the 'Door' (DataLakeData) to use that specific 'Keychain'
ALTER EXTERNAL DATA SOURCE [DataLakeData]
WITH (CREDENTIAL = [StorageKeyCredential]);
-- ============================================================
-- NEXT STEPS 
-- ============================================================
-- STAGING LAYER: Create views in staging schema
--   staging.stg_orders   - clean orders, cast types
--   staging.stg_users    - clean users, MASK email, REMOVE name
--   staging.stg_products - clean products, cast types
--   staging.stg_events   - clean events, DEDUPLICATE rows
--
-- ANALYTICS LAYER: Create views/models in analytics schema
--   analytics.fct_orders          - fact table with revenue calcs
--   analytics.dim_users           - user dimension
--   analytics.dim_products        - product dimension
--   analytics.agg_daily_revenue   - daily revenue summary
--   analytics.agg_product_performance - product performance
--   analytics.fct_user_funnel     - funnel analysis
-- ============================================================