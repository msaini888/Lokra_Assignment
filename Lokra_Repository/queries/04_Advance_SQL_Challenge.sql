--#### 3.1 — Month-over-Month Revenue Growth
--Return a result set showing each month's total net revenue and 
--its percentage change compared to the previous month, for the last 12 months.
--Use window functions.

WITH monthly_revenue AS
(
    SELECT
        -- convert date into month format (2026-01)
        FORMAT(order_date, 'yyyy-MM') AS month,

        -- total revenue for each month
        SUM(net_revenue) AS total_revenue

    FROM analytics.fct_orders

    -- last 12 months filter
    WHERE order_date >= DATEADD(month,-12,CAST(GETDATE() AS DATE))
    GROUP BY FORMAT(order_date, 'yyyy-MM')
)

SELECT
    month,
    total_revenue,

    -- previous month revenue using window function
    LAG(total_revenue)
    OVER (ORDER BY month)
    AS previous_month_revenue,

    -- growth percentage
    ROUND(
        (
            total_revenue
            -
            LAG(total_revenue) OVER (ORDER BY month)
        )
        * 100.0
        /
        LAG(total_revenue) OVER (ORDER BY month),
        2
    ) AS mom_growth_percentage

FROM monthly_revenue
ORDER BY month;


--#### 3.2 — Cohort Retention Analysis
--Using signup cohort month, calculate the **percentage of users who placed at least one order** in their:
--- Month 0 (signup month)
--- Month 1, Month 2, Month 3 (months after signup)

SELECT
u.cohort_month,
COUNT(DISTINCT u.user_id) AS total_users,

-- Month 0 = ordered in signup month
COUNT(DISTINCT CASE
WHEN DATEDIFF(month, u.signup_date, o.order_date) = 0
THEN u.user_id
END) * 100.0
/
COUNT(DISTINCT u.user_id) AS month0_pct,

-- Month 1 = ordered next month
COUNT(DISTINCT CASE
WHEN DATEDIFF(month, u.signup_date, o.order_date) = 1
THEN u.user_id
END) * 100.0
/
COUNT(DISTINCT u.user_id) AS month1_pct,

-- Month 2
COUNT(DISTINCT CASE
WHEN DATEDIFF(month, u.signup_date, o.order_date) = 2
THEN u.user_id
END) * 100.0
/
COUNT(DISTINCT u.user_id) AS month2_pct,

-- Month 3
COUNT(DISTINCT CASE
WHEN DATEDIFF(month, u.signup_date, o.order_date) = 3
THEN u.user_id
END) * 100.0
/
COUNT(DISTINCT u.user_id) AS month3_pct

FROM analytics.dim_users u
LEFT JOIN analytics.fct_orders o
ON u.user_id = o.user_id

GROUP BY u.cohort_month
ORDER BY u.cohort_month;

--================================================================
--#### 3.3 — Top Products by Region with Ranking
--For each region, return the **top 5 products by net revenue** in the last 90 days. 
--Include rank, product name, category, and revenue. Handle ties correctly.

WITH product_revenue AS (
SELECT
region,product_id,product_name,product_category,
SUM(net_revenue) AS total_revenue
FROM analytics.fct_orders
WHERE order_date >= DATEADD(day,-90,CAST(GETDATE() AS DATE))
GROUP BY
region,product_id,product_name,product_category
)
SELECT *FROM (
SELECT region,product_id,product_name,product_category,total_revenue,
RANK() OVER (
PARTITION BY region
ORDER BY total_revenue DESC
) AS product_rank
FROM product_revenue
) ranked
WHERE product_rank <= 5
ORDER BY region, product_rank;

--#### 3.4 — Identifying At-Risk Customers
--Define an "at-risk" customer as a `pro` or `enterprise` plan user who:
--- Has placed at least 3 orders historically, AND
--- Has not placed any order in the last 60 days

SELECT
du.user_id,du.plan_type,
COUNT(fo.order_id) AS total_orders,
MAX(fo.order_date) AS last_order_date
FROM analytics.dim_users du
JOIN analytics.fct_orders fo
ON du.user_id = fo.user_id
WHERE du.plan_type IN ('pro','enterprise')
GROUP BY
du.user_id,
du.plan_type
HAVING
COUNT(fo.order_id) >= 3
AND
MAX(fo.order_date) < DATEADD(day,-60,CAST(GETDATE() AS DATE));

--#### 3.5 — Funnel Drop-off Rate
--Using `analytics.fct_user_funnel`, calculate the drop-off rate (%) at each step of the funnel, 
--broken down by `device_type`.

SELECT
e.device_type,
COUNT(*) AS total_users,

-- drop after product view
SUM(f.dropped_after_view) * 100.0
/ COUNT(*) AS drop_after_view_pct,

-- drop after add to cart
SUM(f.dropped_after_cart) * 100.0
/ COUNT(*) AS drop_after_cart_pct,

-- drop after checkout
SUM(f.dropped_after_checkout) * 100.0
/ COUNT(*) AS drop_after_checkout_pct
FROM analytics.fct_user_funnel f
JOIN (
    SELECT DISTINCT user_id, device_type
    FROM staging.stg_events
) e
ON f.user_id = e.user_id
GROUP BY e.device_type
ORDER BY e.device_type;
