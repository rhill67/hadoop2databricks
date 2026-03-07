#!/usr/bin/env python3

from pyspark.sql.functions import current_timestamp

source_path = "s3://hadoop2databricks/landing/nasa/parquet/" 
checkpoint_path = "dbfs:/checkpoints/nasa/missions_people"
target_table = "nasa_demo.missions_people"

df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "parquet")
         .load(source_path)
         .withColumn("ingest_ts", current_timestamp())
)

(
    df.writeStream
      .option("checkpointLocation", checkpoint_path)
      .trigger(availableNow=True)
      .toTable(target_table)
)
