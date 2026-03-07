CREATE SCHEMA IF NOT EXISTS nasa_demo;
CREATE TABLE IF NOT EXISTS nasa_demo.missions_people
USING DELTA
AS SELECT * FROM read_files('s3://hadoop2databricks/landing/nasa/parquet/', format => 'parquet') WHERE 1=0;
