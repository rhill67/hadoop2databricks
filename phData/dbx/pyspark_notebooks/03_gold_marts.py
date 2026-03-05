# Databricks notebook source
# 03 - Gold Marts (analytics-ready)
from pyspark.sql.functions import col, sum as fsum, countDistinct, count

CATALOG = "demo"
SILVER = "silver"
GOLD = "gold"

orders  = spark.table(f"{CATALOG}.{SILVER}.orders")
sales   = spark.table(f"{CATALOG}.{SILVER}.sales")
products= spark.table(f"{CATALOG}.{SILVER}.products")
customers = spark.table(f"{CATALOG}.{SILVER}.customers")

# ---- Daily revenue ----
daily_rev = (orders.join(sales, "order_id", "inner")
    .groupBy("order_date")
    .agg(
        fsum(col("line_total")).alias("total_revenue"),
        count("*").alias("line_count"),
        countDistinct("order_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers")
    )
)

(daily_rev.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{GOLD}.daily_revenue"))

# ---- Top products ----
top_products = (sales.join(products, "product_id", "left")
    .groupBy("product_id", "product_name", "category")
    .agg(
        fsum(col("quantity")).alias("total_qty"),
        fsum(col("line_total")).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

(top_products.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{GOLD}.top_products"))

# ---- Customer 360 (basic) ----
cust_360 = (customers.join(orders, "customer_id", "left")
    .join(sales, "order_id", "left")
    .groupBy("customer_id", "first_name", "last_name", "email", "country")
    .agg(
        countDistinct("order_id").alias("lifetime_orders"),
        fsum(col("line_total")).alias("lifetime_spend")
    )
)

(cust_360.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .saveAsTable(f"{CATALOG}.{GOLD}.customer_360"))

display(spark.table(f"{CATALOG}.{GOLD}.daily_revenue").orderBy(col("order_date").desc()).limit(10))
