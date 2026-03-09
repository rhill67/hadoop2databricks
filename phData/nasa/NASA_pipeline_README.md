
# 🚀 NASA Missions Data Pipeline

This bonus project demonstrates a **data engineering pipeline that ingests NASA mission data from a public API, transforms nested JSON into structured datasets, and loads the results into AWS S3 and Databricks for analytics.**

---

# Architecture

![Pipeline Architecture](nasa_pipeline_architecture.png)

Pipeline stages:

1. NASA API data retrieval
2. Raw JSON download
3. Mission extraction using Bash + jq
4. JSON flattening using Python ETL
5. Conversion to columnar Parquet files
6. Upload to AWS S3 data lake
7. Databricks Auto Loader ingestion

---

# Workflow

## Download Mission Metadata

wget https://osdr.nasa.gov/geode-py/ws/api/missions

Rename file

mv missions all_missions.raw

Format JSON

cat all_missions.raw | jq > all_missions.json

---

# Parse Individual Missions

Run the mission parsing script:

./parse_missions.sh 10

Directory structure created:

missions/
  STS-40/
    mission_data.json
  STS-41/
    mission_data.json

---

# Flatten JSON → CSV

Example:

./flatten_missions.py --input missions/STS-40/mission_data.json --output missions/STS-40/mission_data.csv

---

# Convert CSV → Parquet

for i in $(ls missions/)
do
 python3 flatten_missions_to_csv_parquet.py   --input missions/$i/mission_data.json   --output-csv missions/$i/flattened.csv   --output-parquet missions/$i/${i}_flattened.parquet
done

---

# Upload to AWS S3

for i in $(find missions/ -name *.parquet)
do
 aws s3 cp $i s3://hadoop2databricks/landing/nasa/parquet/
done

---

# Databricks Ingestion

Example Auto Loader pipeline:

```python
df = (
 spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","parquet")
 .load("s3://hadoop2databricks/landing/nasa/parquet/")
)

df.writeStream  .format("delta")  .option("checkpointLocation","/checkpoints/nasa")  .table("demo.bronze.nasa_missions")
```

---

# Technologies

| Technology | Purpose |
|-----------|--------|
| NASA OSDR API | Source dataset |
| Bash | Automation |
| jq | JSON parsing |
| Python | ETL |
| Parquet | Columnar storage |
| AWS S3 | Data lake |
| Databricks | Analytics platform |

---

# Author

Roger Hill  
Cloud Data Architect  

GitHub: https://github.com/rhill67
