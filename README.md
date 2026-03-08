
# 🚀 Hadoop → Databricks Lakehouse Migration (AWS)

![Databricks](https://img.shields.io/badge/Platform-Databricks-red)
![AWS](https://img.shields.io/badge/Cloud-AWS-orange)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)
![Python](https://img.shields.io/badge/Python-PySpark-blue)
![SQL](https://img.shields.io/badge/Language-SQL-lightgrey)

A demonstration project showing how to migrate **Hadoop/Hive data platforms to the Databricks Lakehouse on AWS** using modern data engineering practices.

---

# 📚 Table of Contents

- Overview
- Architecture
- Medallion Architecture
- Data Model
- Repository Structure
- Migration Workflow
- Databricks Processing
- Data Quality Validation
- Dashboards
- Infrastructure as Code
- Technologies
- Project Goals
- Demo Walkthrough
- Author

---

# Overview

This repository demonstrates a **data migration and analytics pipeline from Hadoop/Hive to Databricks on AWS** using:

- Medallion Architecture
- ELT processing
- Delta Lake
- Unity Catalog governance
- Databricks Jobs orchestration
- Terraform Infrastructure as Code

The project simulates exporting **Hive tables from Hadoop**, transferring them to **AWS S3**, and transforming them into **analytics-ready datasets** inside Databricks.

---

# 🏗 Architecture

## Hadoop → Databricks Migration

```
Hadoop Hive
     │
     ▼
Hive Export (Parquet)
     │
     ▼
Secure Edge Node (DMZ)
     │
     ▼
AWS S3 Landing Zone
     │
     ▼
Databricks Lakehouse
     │
     ▼
Medallion Layers
Bronze → Silver → Gold
```

This architecture demonstrates a **typical enterprise migration pattern** used when organizations modernize Hadoop-based analytics platforms.

---

# 🥇 Medallion Architecture

| Layer | Purpose |
|------|--------|
| Bronze | Raw ingestion layer |
| Silver | Cleaned, validated datasets |
| Gold | Analytics-ready business datasets |

## Bronze Layer

Raw ingestion from Hadoop exports.

Tables:

```
demo.bronze.customers
demo.bronze.orders
demo.bronze.products
demo.bronze.sales
```

Characteristics:

- Stores raw source data
- Tracks ingestion metadata
- Supports incremental ingestion

---

## Silver Layer

Cleaned and standardized datasets.

Typical transformations:

- Data type normalization
- Deduplication
- Referential integrity checks
- Data quality validation

Tables:

```
demo.silver.customers
demo.silver.orders
demo.silver.products
demo.silver.sales
```

---

## Gold Layer

Business-ready analytics tables.

```
demo.gold.daily_revenue
demo.gold.top_products
demo.gold.customer_360
```

These tables power **BI dashboards and analytics tools**.

---

# 📊 Data Model

Retail transaction model:

```
Customers (1:N) Orders
Orders (1:N) Sales
Products (1:N) Sales
```

| Table | Type | Description |
|------|------|-------------|
| customers | Dimension | Customer information |
| products | Dimension | Product catalog |
| orders | Header | Order transactions |
| sales | Fact | Order line items |

The **Sales table acts as the central fact table**.

---

# 📂 Repository Structure

```
hadoop2databricks
│
├── phData
│   ├── hadoop_exports
│   │   ├── customers
│   │   ├── orders
│   │   ├── products
│   │   └── sales
│   │
│   ├── dbx
│   │   ├── notebooks
│   │   │   ├── bronze_ingest
│   │   │   ├── silver_transform
│   │   │   └── gold_marts
│   │   │
│   │   └── terraform
│   │       ├── providers.tf
│   │       ├── variables.tf
│   │       ├── main.tf
│   │       └── outputs.tf
│
├── DataQuality_notebook.py
├── diagrams
└── README.md
```

---

# 🔄 Migration Workflow

## Step 1 — Export Hive Tables

Example HiveQL:

```sql
INSERT OVERWRITE LOCAL DIRECTORY '/data/hadoop_exports/parquet/customers'
STORED AS PARQUET
SELECT * FROM customers;
```

---

## Step 2 — Transfer to AWS

Files are securely transferred from a **DMZ node**.

```bash
aws s3 cp /data/hadoop_exports/parquet s3://hadoop2databricks/landing/hadoop/parquet --recursive
```

---

# ⚡ Databricks Processing

## Bronze Ingestion

```python
df = spark.read.parquet("/mnt/landing/parquet/customers")

df.write.format("delta") .mode("overwrite") .saveAsTable("demo.bronze.customers")
```

---

## Silver Transformations

```sql
CREATE OR REPLACE TABLE demo.silver.orders AS
SELECT DISTINCT
order_id,
customer_id,
CAST(order_date AS DATE) AS order_date
FROM demo.bronze.orders;
```

---

## Gold Analytics

```sql
CREATE OR REPLACE TABLE demo.gold.daily_revenue AS
SELECT
o.order_date,
SUM(s.line_total) AS total_revenue
FROM demo.silver.orders o
JOIN demo.silver.sales s
ON o.order_id = s.order_id
GROUP BY o.order_date;
```

---

# 🧪 Data Quality Validation

Example validation checks.

### Row Count Validation

```sql
SELECT COUNT(*) FROM demo.bronze.sales
UNION
SELECT COUNT(*) FROM demo.silver.sales;
```

### Referential Integrity

```sql
SELECT COUNT(*)
FROM demo.silver.sales s
LEFT JOIN demo.silver.orders o
ON s.order_id = o.order_id
WHERE o.order_id IS NULL;
```

---

# 📈 Example Dashboard

## Daily Revenue

```sql
SELECT
order_date,
SUM(line_total) AS daily_revenue
FROM demo.silver.sales
GROUP BY order_date
ORDER BY order_date;
```

This dashboard visualizes **revenue trends over time**.

---

# ⚙ Infrastructure as Code

Databricks resources are deployed using **Terraform**.

```
terraform init
terraform plan
terraform apply
```

Terraform manages:

- Databricks notebooks
- pipeline orchestration
- workspace configuration

---

# 🛠 Technologies

| Technology | Purpose |
|-----------|--------|
| Hadoop | Legacy data platform |
| Hive | Hadoop SQL engine |
| AWS S3 | Data lake storage |
| Databricks | Lakehouse analytics |
| Delta Lake | Transactional data storage |
| PySpark | Data processing |
| SQL | Data analytics |
| Terraform | Infrastructure automation |

---

# 🎯 Project Goals

✔ Demonstrate Hadoop → Databricks migration architecture  
✔ Implement Medallion architecture  
✔ Build ELT pipelines using PySpark and SQL  
✔ Implement data quality validation  
✔ Deliver analytics-ready Gold tables  
✔ Deploy infrastructure using Terraform  

---

# 🎥 Demo Walkthrough

Typical demo flow:

1. Export Hive tables to Parquet
2. Transfer data to AWS S3 landing zone
3. Ingest raw data into Bronze Delta tables
4. Transform into Silver clean datasets
5. Build Gold analytics marts
6. Run data quality validation notebook
7. Visualize results using Databricks dashboards

---

# 👨‍💻 Author

**Roger Hill**  
Cloud Data Architect  

GitHub:  
https://github.com/rhill67
