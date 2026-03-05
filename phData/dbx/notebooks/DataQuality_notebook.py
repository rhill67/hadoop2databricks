# Databricks notebook source
# DataQuality notebook
# Purpose: Run repeatable data quality + reconciliation checks for Bronze -> Silver -> Gold
# Assumes tables:
#   demo.bronze.{customers,products,orders,sales}
#   demo.silver.{customers,products,orders,sales}
#   demo.gold.{daily_revenue,top_products,customer_360}

# COMMAND ----------

%sql
-- 0) Optional: Create a persistent DQ results table (run once)
CREATE TABLE IF NOT EXISTS demo.silver.dq_results (
  check_ts TIMESTAMP,
  layer STRING,
  table_name STRING,
  check_name STRING,
  status STRING,
  bad_count BIGINT
) USING DELTA;

# COMMAND ----------

%sql
-- 1) Bronze -> Silver row count reconciliation (Silver should be <= Bronze if you dedupe)
SELECT 'customers' AS entity,
       (SELECT COUNT(*) FROM demo.bronze.customers) AS bronze_cnt,
       (SELECT COUNT(*) FROM demo.silver.customers) AS silver_cnt
UNION ALL
SELECT 'products',
       (SELECT COUNT(*) FROM demo.bronze.products),
       (SELECT COUNT(*) FROM demo.silver.products)
UNION ALL
SELECT 'orders',
       (SELECT COUNT(*) FROM demo.bronze.orders),
       (SELECT COUNT(*) FROM demo.silver.orders)
UNION ALL
SELECT 'sales',
       (SELECT COUNT(*) FROM demo.bronze.sales),
       (SELECT COUNT(*) FROM demo.silver.sales);

# COMMAND ----------

%sql
-- 2) Primary key NOT NULL checks (expected bad = 0)
SELECT 'customers.customer_id' AS check, COUNT(*) AS bad FROM demo.silver.customers WHERE customer_id IS NULL
UNION ALL
SELECT 'products.product_id', COUNT(*) FROM demo.silver.products WHERE product_id IS NULL
UNION ALL
SELECT 'orders.order_id', COUNT(*) FROM demo.silver.orders WHERE order_id IS NULL
UNION ALL
SELECT 'sales.sale_id', COUNT(*) FROM demo.silver.sales WHERE sale_id IS NULL;

# COMMAND ----------

%sql
-- 3) Uniqueness checks (total should equal distinct_ids)
SELECT 'customers' AS table_name, COUNT(*) total, COUNT(DISTINCT customer_id) distinct_ids FROM demo.silver.customers
UNION ALL
SELECT 'products', COUNT(*), COUNT(DISTINCT product_id) FROM demo.silver.products
UNION ALL
SELECT 'orders', COUNT(*), COUNT(DISTINCT order_id) FROM demo.silver.orders
UNION ALL
SELECT 'sales', COUNT(*), COUNT(DISTINCT sale_id) FROM demo.silver.sales;

# COMMAND ----------

%sql
-- 4) Referential integrity (expected = 0)
SELECT COUNT(*) AS orphan_orders_customers
FROM demo.silver.orders o
LEFT JOIN demo.silver.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

# COMMAND ----------

%sql
SELECT COUNT(*) AS orphan_sales_orders
FROM demo.silver.sales s
LEFT JOIN demo.silver.orders o ON s.order_id = o.order_id
WHERE o.order_id IS NULL;

# COMMAND ----------

%sql
SELECT COUNT(*) AS orphan_sales_products
FROM demo.silver.sales s
LEFT JOIN demo.silver.products p ON s.product_id = p.product_id
WHERE p.product_id IS NULL;

# COMMAND ----------

%sql
-- 5) Domain/value checks
-- 5a) Order statuses (inspect that only expected values appear)
SELECT order_status, COUNT(*) AS cnt
FROM demo.silver.orders
GROUP BY order_status
ORDER BY cnt DESC;

# COMMAND ----------

%sql
-- 5b) Numeric sanity checks (expected all zeros)
SELECT
  SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS bad_qty,
  SUM(CASE WHEN unit_price < 0 THEN 1 ELSE 0 END) AS bad_unit_price,
  SUM(CASE WHEN discount_pct < 0 OR discount_pct > 1 THEN 1 ELSE 0 END) AS bad_discount,
  SUM(CASE WHEN line_total < 0 THEN 1 ELSE 0 END) AS bad_line_total
FROM demo.silver.sales;

# COMMAND ----------

%sql
-- 6) Derived-field validation: recompute line_total (expected mismatched_lines = 0)
SELECT COUNT(*) AS mismatched_lines
FROM demo.silver.sales
WHERE ABS(line_total - (quantity * unit_price * (1 - discount_pct))) > 0.01;

# COMMAND ----------

%sql
-- 7) Gold reconciliation: daily_revenue must match Silver aggregation (expected 0 rows)
WITH silver_daily AS (
  SELECT o.order_date, SUM(s.line_total) AS silver_revenue
  FROM demo.silver.orders o
  JOIN demo.silver.sales s ON o.order_id = s.order_id
  GROUP BY o.order_date
),
gold_daily AS (
  SELECT order_date, total_revenue AS gold_revenue
  FROM demo.gold.daily_revenue
)
SELECT
  COALESCE(s.order_date, g.order_date) AS order_date,
  s.silver_revenue,
  g.gold_revenue,
  (COALESCE(s.silver_revenue,0) - COALESCE(g.gold_revenue,0)) AS diff
FROM silver_daily s
FULL OUTER JOIN gold_daily g
ON s.order_date = g.order_date
WHERE ABS(COALESCE(s.silver_revenue,0) - COALESCE(g.gold_revenue,0)) > 0.01
ORDER BY order_date;

# COMMAND ----------

%sql
-- 8) Gold reconciliation: top_products must match Silver aggregation (expected 0 rows)
WITH silver_prod AS (
  SELECT product_id,
         SUM(quantity) AS silver_qty,
         SUM(line_total) AS silver_sales
  FROM demo.silver.sales
  GROUP BY product_id
),
gold_prod AS (
  SELECT product_id,
         total_qty AS gold_qty,
         total_sales AS gold_sales
  FROM demo.gold.top_products
)
SELECT
  COALESCE(s.product_id, g.product_id) AS product_id,
  s.silver_qty, g.gold_qty,
  s.silver_sales, g.gold_sales
FROM silver_prod s
FULL OUTER JOIN gold_prod g
ON s.product_id = g.product_id
WHERE ABS(COALESCE(s.silver_sales,0) - COALESCE(g.gold_sales,0)) > 0.01
   OR COALESCE(s.silver_qty,0) <> COALESCE(g.gold_qty,0)
ORDER BY product_id;

# COMMAND ----------

%sql
-- 9) Optional: persist a subset of results into dq_results (example checks)
-- PK nulls (example: orders)
INSERT INTO demo.silver.dq_results
SELECT current_timestamp(), 'silver', 'orders', 'order_id_not_null',
       CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*)
FROM demo.silver.orders
WHERE order_id IS NULL;

# COMMAND ----------

%sql
-- Orphan sales->orders (example)
INSERT INTO demo.silver.dq_results
SELECT current_timestamp(), 'silver', 'sales', 'orphan_sales_orders',
       CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*)
FROM demo.silver.sales s
LEFT JOIN demo.silver.orders o ON s.order_id = o.order_id
WHERE o.order_id IS NULL;

# COMMAND ----------

%sql
-- View latest DQ results
SELECT *
FROM demo.silver.dq_results
ORDER BY check_ts DESC
LIMIT 200;
