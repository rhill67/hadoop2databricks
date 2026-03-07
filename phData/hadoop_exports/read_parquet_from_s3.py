#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pathlib import Path

# Create Spark session
spark = SparkSession.builder.appName("ReadLocalParquetTables").getOrCreate()

# Base local path where parquet folders are stored
base_path = Path("./parquet")   # change if needed

# Read each parquet table from local disk
sales_df = spark.read.parquet(str(base_path / "sales"))
orders_df = spark.read.parquet(str(base_path / "orders"))
products_df = spark.read.parquet(str(base_path / "products"))
customers_df = spark.read.parquet(str(base_path / "customers"))

# Show sample rows
print("\nSales:")
sales_df.show(5, truncate=False)

print("\nOrders:")
orders_df.show(5, truncate=False)

print("\nProducts:")
products_df.show(5, truncate=False)

print("\nCustomers:")
customers_df.show(5, truncate=False)

# Print schemas
print("\nSales Schema:")
sales_df.printSchema()

print("\nOrders Schema:")
orders_df.printSchema()

print("\nProducts Schema:")
products_df.printSchema()

print("\nCustomers Schema:")
customers_df.printSchema()

spark.stop()
