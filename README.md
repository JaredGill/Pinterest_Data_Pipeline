# Pinterest_Data_Pipeline

---

## Table of Contents

- [Description](#description)
- [Project Aim](#project-aim)
- [Architecture Overview](#architecture-overview)
- [Technologies Used](#technologies-used)
- [File Structure](#file-structure)
- [Future Improvements](#future-improvements)

---

## Description

This project simulates a scalable, real-world data pipeline inspired by platforms like Pinterest. It demonstrates how to ingest, process, and store user activity data using both **batch (Kafka)** and **streaming (Kinesis)** paradigms. One Databricks notebook is orchestrated using Apache Airflow (via MWAA) to process the batch data from Kafka, while other components are triggered manually or run on a streaming basis.

The pipeline uses mock social media-style data to represent user activity on a content-sharing platform. The dataset includes:
- **User table**: Contains basic user information such as `user_name`, `age`, and `date_joined`.
- **Geo table**: Stores geolocation data, including `country`, geographic `coordinates`, and `timestamp` of the activity.
- **Pin table**: Represents individual pieces of content posted by users. It includes fields like `title`, `description`, `follower_count`, `poster_name`, `tag_list`, whether the content is an image or video, a source URL, save location, and category.

This data structure is used to emulate a realistic user interaction flow and serves as the foundation for both the batch and streaming data pipelines built in the project.

---

## Project Aim

The goal of this project is to showcase the full lifecycle of a data engineering pipelines, covering both **streaming and batch ingestion** using cloud tools.

It aims to:

- Simulate the ingestion of `user`, `pin`, and `geo` data in both batch and real-time
- Demonstrate data ingestion via **Kafka REST Proxy** and **Kinesis Streams**
- Use **Airflow (MWAA)** to orchestrate processing logic in Databricks for Kafka data
- Store clean and transformed data in **Delta Table** for analysis

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
  - Delta tables – Used for storing cleaned and transformed data
- **Languages & Libraries**
  - Python, PySpark, SQL, SQLAlchemy, YAML
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
- Cleaned and structured Delta Tables for all three datasets (`user`, `geo`, `pin`) which are available in Databricks in seperate tables for batch or stream data.
- Notebooks query this data to answer business questions.


---

## File Strucutre

```
Pinterest Data Pipeline
├── Databricks_Notebooks
│   ├── Cleaning_Sandbox.ipynb
│   ├── Kafka_ETL.ipynb
│   ├── Kinesis_ETL.ipynb
│   ├── Queries.ipynb
│   ├── Table_Schemas.ipynb
│   └── utils.ipynb
├── 038444ac863e_dag.py
├── README.md
└── user_posting_emulation.py
```

---

## Future Improvements

A few things to clean up on, first in the utils & Cleaning Sandbox notebooks the filter_mostly_numeric_strings does not work as intended only detecting isolated substrings of numerical characters and not searching the whole string for mostly or entirely numerical characters.
In the streaming to Kinesis the /records PUT method was never actually tested either. Ultimately the dag used was just a scheduler which likely could have been organised in just Databricks so would want to look further into what it is capable of. 

The main things to work on though are to recreate the queries using purely PySpark and not any SQL, and study and see how the pipeline could be made with Azure further down the line.
