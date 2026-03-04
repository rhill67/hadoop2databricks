#!/usr/bin/env python3
"""
read_missions_parquet_pyspark.py

Run from a Linux terminal to read a parquet file with PySpark and print a nice preview.

Example:
  ./read_missions_parquet_pyspark.py missions/STS-40/flattened.parquet
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col


def main() -> int:
    parser = argparse.ArgumentParser(description="Read a parquet file with PySpark and show a preview.")
    parser.add_argument("input_path", help="Path to parquet file or directory (e.g. missions/STS-40/flattened.parquet)")
    parser.add_argument("--limit", type=int, default=25, help="Number of rows to show (default 25)")
    args = parser.parse_args()

    # Create SparkSession (this is what Databricks gives you for free)
    spark = (
        SparkSession.builder
        .appName("ReadMissionsParquet")
        .getOrCreate()
    )

    # Read parquet
    df = spark.read.parquet(args.input_path)

    # Add a nicer display column
    df_clean = (
        df.withColumn("full_name", concat_ws(" ", col("firstName"), col("lastName")))
          .select("mission_identifier", "startDate", "endDate", "institution", "role", "full_name")
    )

    # Print schema + preview
    print("\n=== Schema ===")
    df_clean.printSchema()

    print(f"\n=== Preview (top {args.limit}) ===")
    df_clean.show(args.limit, truncate=False)

    # Optional: simple summary (counts by institution)
    print("\n=== Counts by institution (top 20) ===")
    (
        df_clean.groupBy("institution")
        .count()
        .orderBy(col("count").desc())
        .show(20, truncate=False)
    )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
