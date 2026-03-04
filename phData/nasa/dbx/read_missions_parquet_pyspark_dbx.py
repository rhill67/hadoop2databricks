# Databricks notebook source
# ============================================
# read_missions_parquet_pyspark_dbx.py
# ============================================
# Goal (slick demo):
#  1) Read flattened mission Parquet(s)
#  2) Write to Delta
#  3) Register Delta table in the metastore (Unity Catalog or Hive metastore)
#  4) Enable SQL queries (create views + example SQL)
#  5) Build dashboard-ready chart queries (pin from results / add to dashboard)
#
# How to use:
#  - Import this file into Databricks as a notebook (Workspace -> Import)
#  - Or paste into a Databricks notebook.
#  - Update the widgets at the top (catalog/schema/table/path).
#
# Notes:
#  - This script assumes Databricks Runtime with Delta support (standard).
#  - If you're using Unity Catalog, set catalog/schema accordingly.
#  - If you're using the legacy Hive metastore, catalog can be omitted.
# ============================================

# COMMAND ----------
# Databricks widgets let you parameterize runs (dev/prod/demo) without code edits.

dbutils.widgets.text("parquet_path", "/mnt/data/flattened.parquet", "Input Parquet path")
dbutils.widgets.text("catalog", "", "Catalog (Unity Catalog) - optional")
dbutils.widgets.text("schema", "nasa_demo", "Schema / Database")
dbutils.widgets.text("table", "missions_people", "Table name")
dbutils.widgets.text("delta_root", "dbfs:/FileStore/nasa_demo/delta", "Delta root path")
dbutils.widgets.dropdown("mode", "overwrite", ["overwrite", "append"], "Write mode")

parquet_path = dbutils.widgets.get("parquet_path").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
table = dbutils.widgets.get("table").strip()
delta_root = dbutils.widgets.get("delta_root").strip().rstrip("/")
mode = dbutils.widgets.get("mode").strip().lower()

# Compose fully qualified names depending on whether UC catalog was provided.
if catalog:
    full_schema = f"{catalog}.{schema}"
    full_table = f"{catalog}.{schema}.{table}"
else:
    full_schema = schema
    full_table = f"{schema}.{table}"

delta_path = f"{delta_root}/{table}"

print("parquet_path:", parquet_path)
print("full_schema :", full_schema)
print("full_table  :", full_table)
print("delta_path  :", delta_path)
print("mode        :", mode)

# COMMAND ----------
# 1) Read Parquet
df = spark.read.parquet(parquet_path)
display(df)

# COMMAND ----------
# 2) Light “Silver” cleanup (types + convenience columns)
from pyspark.sql.functions import col, concat_ws, to_date, when, lit

# Your sample uses MM/DD/YYYY in startDate/endDate. Adjust if your format differs.
df_silver = (
    df
    .withColumn("start_date", to_date(col("startDate"), "MM/dd/yyyy"))
    .withColumn("end_date", to_date(col("endDate"), "MM/dd/yyyy"))
    .withColumn("full_name", concat_ws(" ", col("firstName"), col("lastName")))
    .withColumn("role", when(col("role").isNull(), lit("")).otherwise(col("role")))
    .withColumn("institution", when(col("institution").isNull(), lit("")).otherwise(col("institution")))
)

display(df_silver)

# COMMAND ----------
# 3) Write Delta + register in metastore
#
# We do BOTH:
#   - Save Delta files at delta_path
#   - CREATE SCHEMA if needed
#   - CREATE TABLE ... USING DELTA LOCATION ... (external table pattern)
#
# If you prefer a fully-managed table (no explicit LOCATION),
# you can replace the CREATE TABLE statement accordingly.

(
    df_silver.write.format("delta")
    .mode(mode)  # overwrite or append
    .option("overwriteSchema", "true" if mode == "overwrite" else "false")
    .save(delta_path)
)

print(f"Delta written to: {delta_path}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")

if mode == "overwrite":
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_table}
USING DELTA
LOCATION '{delta_path}'
""")

print(f"Registered table: {full_table}")

# COMMAND ----------
# 4) Create SQL-friendly views
spark.sql(f"""
CREATE OR REPLACE VIEW {full_schema}.vw_missions_people AS
SELECT
  mission_identifier,
  startDate,
  endDate,
  start_date,
  end_date,
  institution,
  role,
  firstName,
  lastName,
  full_name
FROM {full_table}
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {full_schema}.vw_missions_people_quality AS
SELECT
  *,
  CASE WHEN mission_identifier IS NULL OR mission_identifier = '' THEN true ELSE false END AS is_missing_mission_identifier,
  CASE WHEN full_name IS NULL OR full_name = '' THEN true ELSE false END AS is_missing_person_name,
  CASE WHEN institution IS NULL OR institution = '' THEN true ELSE false END AS is_missing_institution,
  CASE WHEN role IS NULL OR role = '' THEN true ELSE false END AS is_missing_role
FROM {full_schema}.vw_missions_people
""")

print("Views created:")
print(f" - {full_schema}.vw_missions_people")
print(f" - {full_schema}.vw_missions_people_quality")

# COMMAND ----------
# 5) Example SQL query (preview)
q_preview = f"""
SELECT
  mission_identifier, startDate, endDate, institution, role, full_name
FROM {full_schema}.vw_missions_people
ORDER BY mission_identifier, institution, role, full_name
LIMIT 50
"""
display(spark.sql(q_preview))

# COMMAND ----------
# 6) Dashboard chart queries (pin these results to a dashboard)
#
# Fast “interview demo” workflow:
#  - Run each query
#  - In results grid: + Add visualization (Bar/Pie/Line/KPI)
#  - Pin -> choose/create dashboard (e.g., "NASA Missions Demo Dashboard")

# (A) Bar chart: Investigators by institution (Top 20)
q_institution = f"""
SELECT
  institution,
  COUNT(*) AS investigator_rows
FROM {full_schema}.vw_missions_people
GROUP BY institution
ORDER BY investigator_rows DESC
LIMIT 20
"""
display(spark.sql(q_institution))

# COMMAND ----------
# (B) Bar chart: Rows by role
q_roles = f"""
SELECT
  role,
  COUNT(*) AS role_rows
FROM {full_schema}.vw_missions_people
GROUP BY role
ORDER BY role_rows DESC
"""
display(spark.sql(q_roles))

# COMMAND ----------
# (C) KPI tiles: totals
q_kpis = f"""
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CONCAT(firstName, ' ', lastName)) AS distinct_people,
  COUNT(DISTINCT institution) AS distinct_institutions
FROM {full_schema}.vw_missions_people
"""
display(spark.sql(q_kpis))

# COMMAND ----------
# (D) Data quality summary
q_quality = f"""
SELECT
  SUM(CASE WHEN is_missing_mission_identifier THEN 1 ELSE 0 END) AS missing_mission_identifier,
  SUM(CASE WHEN is_missing_person_name THEN 1 ELSE 0 END) AS missing_person_name,
  SUM(CASE WHEN is_missing_institution THEN 1 ELSE 0 END) AS missing_institution,
  SUM(CASE WHEN is_missing_role THEN 1 ELSE 0 END) AS missing_role
FROM {full_schema}.vw_missions_people_quality
"""
display(spark.sql(q_quality))

# COMMAND ----------
# 7) Optional: Delta performance optimization
#
# For larger datasets, OPTIMIZE improves read performance; ZORDER helps data skipping.
# Only run if your workspace supports it and you have permission.

spark.sql(f"OPTIMIZE {full_table} ZORDER BY (mission_identifier, institution)")
print("OPTIMIZE complete (if supported).")

# COMMAND ----------
# 8) Optional: Table comment (nice polish)
try:
    spark.sql(
        f"COMMENT ON TABLE {full_table} IS "
        "'Flattened mission-person-role data loaded from Parquet and stored as Delta for analytics.'"
    )
except Exception as e:
    print("COMMENT ON TABLE not supported or insufficient privileges:", e)

print("Done. Next: pin the visualizations above to a Dashboard in Databricks SQL.")

