# Databricks notebook source
# 01 - Bronze Ingestion (Hadoop export files -> Bronze Delta)
# Assumes files are staged in S3 like:
#   s3://<bucket>/landing/hadoop/customers/
#   s3://<bucket>/landing/hadoop/orders/
#   s3://<bucket>/landing/hadoop/products/
#   s3://<bucket>/landing/hadoop/sales/

from pyspark.sql.functions import current_timestamp

CATALOG = "demo"
BRONZE_SCHEMA = "bronze"

# Update this bucket/prefix to your environment
LANDING_BASE = "s3://hadoop2databricks/landing/hadoop/csv/"

def ingest_csv_to_bronze(table_name: str, path_suffix: str):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(LANDING_BASE + path_suffix)
          .withColumn("_ingest_ts", current_timestamp()))
    (df.write
       .format("delta")
       .mode("append")
       .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"))

# Ingest each dataset
ingest_csv_to_bronze("customers", "customers/")
ingest_csv_to_bronze("orders", "orders/")
ingest_csv_to_bronze("products", "products/")
ingest_csv_to_bronze("sales", "sales/")

display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.sales").limit(10))
