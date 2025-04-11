# Pinterest_Data_Pipeline

This project simulates a scalable data engineering pipeline for processing user activity data similar to that from Pinterest. It incorporates both streaming (Kinesis) and batch (Kafka/S3) processing architectures, with integration across AWS, Databricks, and Apache Airflow (MWAA) for orchestration and transformation.

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
  - Python, PySpark, SQLAlchemy, Boto3, YAML
  - Apache Airflow DAGs

---

## Architecture Overview
![Image](https://github.com/user-attachments/assets/0bdd8a4c-c8f6-402f-afaf-690870fde2e9)

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
