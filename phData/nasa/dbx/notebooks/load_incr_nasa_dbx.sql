COPY INTO nasa_demo.missions_people
FROM 's3://hadoop2databricks/landing/nasa/parquet/'
FILEFORMAT = PARQUET;
