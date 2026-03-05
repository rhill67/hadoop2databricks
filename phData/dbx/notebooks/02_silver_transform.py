# Databricks notebook source
# 02 - Silver Transformations (clean, conformed, DQ)
from pyspark.sql.functions import col, to_date, current_timestamp, row_number
from pyspark.sql.window import Window

CATALOG = "demo"
BRONZE = "bronze"
SILVER = "silver"

customers_b = spark.table(f"{CATALOG}.{BRONZE}.customers")
orders_b    = spark.table(f"{CATALOG}.{BRONZE}.orders")
products_b  = spark.table(f"{CATALOG}.{BRONZE}.products")
sales_b     = spark.table(f"{CATALOG}.{BRONZE}.sales")

# ---- Customers ----
customers_s = (customers_b
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("signup_date", to_date(col("signup_date")))
    .dropDuplicates(["customer_id"])
)

(customers_s.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{SILVER}.customers"))

# ---- Products ----
products_s = (products_b
    .withColumn("product_id", col("product_id").cast("long"))
    .withColumn("list_price", col("list_price").cast("decimal(18,2)"))
    .withColumn("is_active", col("is_active").cast("boolean"))
    .dropDuplicates(["product_id"])
)

(products_s.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{SILVER}.products"))

# ---- Orders ----
orders_s = (orders_b
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("order_date", to_date(col("order_date")))
)

# Deduplicate orders by latest ingest (if duplicates exist)
w = Window.partitionBy("order_id").orderBy(col("_ingest_ts").desc())
orders_s = (orders_s
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn")==1)
    .drop("_rn")
)

(orders_s.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{SILVER}.orders"))

# ---- Sales (Line Items) ----
sales_s = (sales_b
    .withColumn("sale_id", col("sale_id").cast("long"))
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("product_id", col("product_id").cast("long"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("decimal(18,2)"))
    .withColumn("discount_pct", col("discount_pct").cast("decimal(5,2)"))
    .withColumn("line_total", col("line_total").cast("decimal(18,2)"))
)

(sales_s.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{SILVER}.sales"))

# ---- Basic Data Quality checks stored to a table ----
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER}.dq_results (
  check_ts TIMESTAMP,
  table_name STRING,
  check_name STRING,
  status STRING,
  bad_count BIGINT
) USING DELTA
''')

def dq_insert(table_name, check_name, bad_count_query):
    spark.sql(f'''
    INSERT INTO {CATALOG}.{SILVER}.dq_results
    SELECT current_timestamp() AS check_ts,
           '{table_name}' AS table_name,
           '{check_name}' AS check_name,
           CASE WHEN (({bad_count_query})) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
           (({bad_count_query})) AS bad_count
    ''')

dq_insert(f"{CATALOG}.{SILVER}.orders", "order_id_not_null",
          f"SELECT COUNT(*) FROM {CATALOG}.{SILVER}.orders WHERE order_id IS NULL")

dq_insert(f"{CATALOG}.{SILVER}.sales", "line_total_not_null",
          f"SELECT COUNT(*) FROM {CATALOG}.{SILVER}.sales WHERE line_total IS NULL")

display(spark.table(f"{CATALOG}.{SILVER}.dq_results").orderBy(col("check_ts").desc()))
