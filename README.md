# Pinterest_Data_Pipeline

---

## Table of Contents

- [Description](#description)
- [Project Aim](#project-aim)
- [Architecture Overview](#architecture-overview)
- [Technologies Used](#technologies-used)
- [File Structure](#file-structure)

---

## Description

This project simulates a scalable, real-world data pipeline inspired by platforms like Pinterest. It demonstrates how to ingest, process, and store user activity data using both **batch (Kafka)** and **streaming (Kinesis)** paradigms. The system is fully orchestrated using Apache Airflow and processes data using Databricks notebooks.

**Key focus areas:**
- Building batch and streaming pipelines side-by-side
- Integrating AWS services (RDS, Kinesis, S3, EC2, MWAA)
- Automating ETL workflows
- Writing clean, queryable Delta Lake outputs

---

## Project Aim

The goal of this project is to showcase the full lifecycle of modern data engineering pipelines, covering both **streaming and batch ingestion** using industry-standard tools.

It aims to:

- Simulate the ingestion of `user`, `pin`, and `geo` data in both batch and real-time
- Demonstrate data ingestion via **Kafka REST Proxy** and **Kinesis Streams**
- Use **Airflow (MWAA)** to orchestrate processing logic in Databricks
- Store clean and transformed data in **Delta Lake** for analytics

---

## Architecture Overview
![Image](https://github.com/user-attachments/assets/0bdd8a4c-c8f6-402f-afaf-690870fde2e9)

---

## Technologies Used

- **AWS Services**
  - RDS (PostgreSQL) – Source data
  - API Gateway – Handles data ingestion endpoints
  - EC2 – Hosts Kafka cluster and REST proxy
  - Kinesis Data Streams – Real-time data streaming
  - S3 – Storage for Kafka batch data (via Kafka Connect)
  - MWAA (Managed Workflows for Apache Airflow) – DAG orchestration
- **Data Processing**
  - Apache Kafka – Batch event handling
  - Confluent Kafka REST Proxy – For HTTP-based Kafka ingestion
  - Databricks (Apache Spark) – ETL and data analytics
  - Delta Lake – Unified data lake format
- **Languages & Libraries**
  - Python, PySpark, SQLAlchemy, YAML
  - Apache Airflow DAGs

---

## Workflow Breakdown

### 1. Data Generation
- `user_posting_emulation.py` fetches mock data from Amazon RDS.
- It sends three types of records — **`user`**, **`geo`**, and **`pin`** — to:
  - **Kafka (batch)** via the REST Proxy endpoint
  - **Kinesis (streaming)** via API Gateway PUT requests

### 2. Kafka Pipeline (Batch)
- The Kafka REST Proxy publishes `user`, `geo`, and `pin` data to separate Kafka topics hosted on an EC2 instance.
- Kafka Connect writes this data to **Amazon S3** as batch files.
- The `user_id_dag.py` Airflow DAG triggers the **Kafka_ETL** notebook daily on Databricks.
- The notebook processes all three datasets, performs cleaning and transformation, and stores the results as Delta Tables.

### 3. Kinesis Pipeline (Streaming)
- The same `user`, `geo`, and `pin` records are streamed to **Amazon Kinesis Data Streams** via API Gateway.
- The **Kinesis_ETL** notebook on Databricks continuously consumes and processes these records in near real-time.
- Transformed data is also written to Delta Tables for downstream analysis.

### 4. Data Analysis
- Cleaned and structured Delta Tables for all three datasets (`user`, `geo`, `pin`) are available in Databricks.
- Analysts and notebooks query this data to answer business questions, regardless of whether the source was batch or stream.


---
