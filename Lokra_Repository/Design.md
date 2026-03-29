# Technical Design Document

This document explains the architectural and performance design decisions for the customer analytics solution built using Azure Synapse Serverless SQL and Power BI.

The solution follows a layered architecture:

RAW → STAGING → ANALYTICS → DASHBOARD

The design prioritizes:

- scalability for large datasets (5B+ rows)
- cost-efficient query execution
- modular SQL transformations
- maintainable data model
- near real-time reporting capability


------------------------------------------------------------
1. Partitioning & Clustering Strategy
------------------------------------------------------------

Since Azure Synapse Serverless SQL uses external tables over a data lake, physical partitioning is implemented at the storage level and logical partitioning is handled through derived date columns.

Partition-aware query design reduces the amount of data scanned, which directly lowers cost and improves performance.

---

fct_orders

Primary logical partition column:
order_date

Derived using:

CAST(o.created_at AS DATE) AS order_date

Reason:

Most analytical queries filter data using time conditions:

- daily revenue trends
- month-over-month growth
- cohort analysis
- recent order activity
- at-risk customer identification
- product performance trends

Partitioning by order_date allows query pruning so only relevant files are scanned.

Example:

WHERE order_date >= '2026-01-01'

At large scale (5B+ rows), this significantly reduces:

- data scanned
- query latency
- compute cost

Estimated benefit:
50–80% reduction in scanned data volume.

---

Clustering strategy (logical grouping):

user_id  
product_id  

Reason:

Most joins and aggregations are performed on:

user_id → customer behaviour analysis  
product_id → product performance analysis  

Grouping similar values improves:

- join efficiency
- aggregation performance
- scan locality
- compression effectiveness in columnar storage

---

Additional derived time columns improve performance by avoiding recalculation:

order_year  
order_month  
order_day  
order_quarter  

These allow filtering without repeated function evaluation.

---

stg_events

Partition column:
created_at

Reason:

Event tables grow rapidly and are commonly filtered by time windows:

- recent user activity
- funnel progression
- behavioural analysis
- device segmentation

Partition pruning ensures only recent event files are scanned.

---

Clustering columns:

event_type  
device_type  

Reason:

Funnel queries frequently filter by event_type:

productview  
addtocart  
checkoutstart  
purchase  

Clustering improves filter efficiency and aggregation speed.

---

Impact at 5B+ rows:

Partition pruning significantly reduces scanned data.

Logical clustering improves:

- predicate pushdown efficiency
- join performance
- aggregation performance

Overall benefits:

lower compute cost  
faster dashboard response  
better concurrency support  


------------------------------------------------------------
2. Materialized Views vs Regular Views
------------------------------------------------------------

Platform constraint:

Azure Synapse Serverless SQL pool does not support materialized views.

Because of this limitation, all transformations were implemented using regular views and pre-aggregated query logic.

---

Approach used instead of materialized views:

Performance optimization achieved using:

pre-aggregated analytics views  
layered data architecture  
partition-aware filtering  
reusable SQL logic  

---

Regular views implemented:

stg_users  
stg_orders  
stg_products  
stg_events  

dim_users  
dim_products  

fct_orders  
fct_user_funnel  

agg_daily_revenue  
agg_product_performance  

---

Why regular views are sufficient:

dataset size manageable for serverless execution  
queries filtered using order_date partition column  
aggregations scoped to relevant time windows  

Example aggregation:

SUM(net_revenue)
GROUP BY order_date

Partition pruning ensures limited data scanning.

---

Trade-offs compared to materialized views:

Advantages:

always reflects latest data  
no additional storage cost  
simpler maintenance  
fully compatible with serverless SQL  

Disadvantages:

aggregations computed during query runtime  
higher compute cost at extremely large scale  

---

Future enhancement:

If migrated to Dedicated SQL pool or Fabric Warehouse, these objects could be converted to materialized views:

agg_daily_revenue  
agg_product_performance  

This would further improve dashboard query performance.


------------------------------------------------------------
3. Incremental Processing Strategy
------------------------------------------------------------

Goal:

Avoid full refresh of analytics.fct_orders during each pipeline execution.

Approach:

incremental loading using timestamp filtering.

Since order data contains created_at timestamp, incremental logic can process only new records.

Example incremental condition:

WHERE created_at >= DATEADD(hour,-1,GETDATE())

---

Recommended incremental pattern:

Step 1:
Load new data files into raw storage.

Step 2:
Staging layer reads only newly arrived files.

Step 3:
Analytics layer processes only recent time partitions.

Step 4:
Aggregations updated for recent dates only.

---

Alternative approach:

MERGE pattern (if using dedicated SQL pool):

MERGE analytics.fct_orders AS target
USING staging.stg_orders AS source
ON target.order_id = source.order_id

WHEN NOT MATCHED THEN INSERT

WHEN MATCHED THEN UPDATE

---

Benefits:

reduced compute cost  
faster pipeline execution  
minimal data movement  
scalable ingestion strategy  


------------------------------------------------------------
4. Query Cost Optimization
------------------------------------------------------------

Potential expensive query:

Cohort analysis using fct_orders.

Operations include:

DATEDIFF calculations  
DISTINCT user counts  
GROUP BY cohort month  
aggregation across time buckets  

Distinct counts at billion-row scale can be expensive.

---

Optimization techniques applied:

Pre-calculated cohort index column:

Cohort Month Index =
DATEDIFF(user_signup_date, order_date, MONTH)

This avoids recalculating date logic repeatedly.

---

Pre-aggregated views used:

agg_daily_revenue  
agg_product_performance  

These reduce repeated heavy aggregations.

---

Additional optimizations:

avoid SELECT *
apply filter pushdown using order_date
use derived date columns
limit scan ranges
reuse aggregated views

---

Expected performance improvement:

minutes → seconds improvement at scale.


------------------------------------------------------------
5. Data Freshness SLA (1 hour)
------------------------------------------------------------

Requirement:

Revenue metrics should be no more than 1 hour stale.

---

Proposed architecture:

micro-batch ingestion pipeline.

Data flow:

Source system  
→ Data lake storage  
→ Synapse staging views  
→ analytics views  
→ Power BI dashboard  

---

Implementation:

Step 1:
New data files arrive every 15–30 minutes.

Step 2:
Pipeline processes only recent data using created_at filter.

Step 3:
agg_daily_revenue reflects latest transactions.

Step 4:
Power BI dataset refresh scheduled every 1 hour.

---

Scheduling tools:

Synapse pipelines  
Azure Data Factory  
scheduled SQL execution  

---

Latency:

data ingestion delay:
30 minutes

dashboard refresh delay:
1 hour

---

Benefits:

near real-time insights  
controlled compute cost  
predictable performance  


------------------------------------------------------------
Conclusion
------------------------------------------------------------

The design balances:

performance  
cost efficiency  
data freshness  
scalability  

The architecture supports future scaling to billions of records with minimal structural changes and can be extended with streaming ingestion or dedicated SQL pool optimization if required.
