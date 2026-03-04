#!/usr/bin/env python3

### Date : 03-04-2026
### Auth : roger hill
### Desc : Read parquet files using pySpark 

# -----------------------------------------------
# Read Flattened Mission Parquet Data
# -----------------------------------------------

# Location of parquet file
# parquet_path = "/mnt/data/flattened.parquet"

# -----------------------------
# Input discovery
# -----------------------------
def discover_json_files(input_path: str) -> list[str]:
    """
    Accepts:
      - single file path
      - directory path (loads *.json inside)
      - glob pattern (e.g., ./data/*.json)
    """
    if os.path.isdir(input_path):
        return sorted(glob.glob(os.path.join(input_path, "*.json")))

    if any(ch in input_path for ch in ["*", "?", "["]):
        return sorted(glob.glob(input_path))

    return [input_path]

# Read parquet file into Spark dataframe
# df = spark.read.parquet(parquet_path)
df = spark.read.parquet(input_path)

# Show schema (helps confirm structure)
df.printSchema()


# -----------------------------------------------
# Optional: Clean up / create nicer columns
# -----------------------------------------------

from pyspark.sql.functions import concat_ws

df_clean = df.withColumn(
    "full_name",
    concat_ws(" ", df.firstName, df.lastName)
).select(
    "mission_identifier",
    "startDate",
    "endDate",
    "institution",
    "role",
    "full_name"
)

# -----------------------------------------------
# Display table (Databricks visual grid)
# -----------------------------------------------

display(df_clean)
