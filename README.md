# Real-Time-Data-Pipeline with Airflow, Kaafka, Spark & Cassandra
## Real-Time User Data Streaming Pipeline
## Overview

This project implements a real-time end-to-end data streaming platform using Apache Airflow, Apache Kafka, Apache Spark, Cassandra, PostgreSQL, Docker, and Confluent components.

The solution continuously ingests user data from an external API, streams the data through Kafka, processes the stream using Spark Structured Streaming, and persists the transformed records into Cassandra for real-time analytics and downstream consumption.

The entire platform is containerized using Docker Compose, enabling reproducible deployments and simplified environment management.

## Architecture

                   ┌─────────────────┐
                   │  External API   │
                   │ (Random User)   │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │ Apache Airflow  │
                   │ DAG Scheduler   │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │ Apache Kafka    │
                   │ users_created   │
                   │     Topic       │
                   └────────┬────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │ Apache Spark        │
                 │ Structured Streaming│
                 └────────┬────────────┘
                          │
                          ▼
                 ┌─────────────────────┐
                 │ Apache Cassandra    │
                 │ created_users Table │
                 └─────────────────────┘


## Technologies
| Component       | Purpose                          |
| --------------- | -------------------------------- |
| Python          | Data ingestion and orchestration |
| Apache Airflow  | Workflow orchestration           |
| PostgreSQL      | Airflow metadata database        |
| Apache Kafka    | Event streaming platform         |
| Schema Registry | Schema management                |
| Apache Spark    | Stream processing                |
| Cassandra       | NoSQL data storage               |
| Kafdrop         | Kafka monitoring UI              |
| Docker Compose  | Container orchestration          |

## Features

### Real-Time Streaming

Continuously ingests user records from an API and streams them into Kafka.

### Event-Driven Architecture

Uses Kafka as the central message broker to decouple producers from consumers.

### Distributed Stream Processing

Spark Structured Streaming consumes Kafka events and processes data in micro-batches.

### Scalable NoSQL Storage

Processed records are stored in Cassandra for high-write throughput and horizontal scalability.

### Containerized Deployment

All services are deployed and managed through Docker Compose.

## Project Structure
<img width="172" height="336" alt="Screenshot 2026-06-02 075047" src="https://github.com/user-attachments/assets/9b2f6b80-9a67-4658-a6e8-04c6804a6f9b" />

## How to Run

### Clone Repository
git clone https://github.com/yourusername/realtime-user-streaming-pipeline.git

- cd realtime-user-streaming-pipeline
### Build Containers
docker compose build --no-cache
### Start Platform
docker compose up -d

- Verify:

docker ps
# Access Services

| Service         | URL                                            |
| --------------- | ---------------------------------------------- |
| Airflow         | [http://localhost:8080](http://localhost:8080) |
| Kafdrop         | [http://localhost:9000](http://localhost:9000) |
| Schema Registry | [http://localhost:8081](http://localhost:8081) |
| Spark UI        | [http://localhost:8888](http://localhost:8888) |

## Trigger Airflow DAG

- Run manually:

docker exec airflow-webserver airflow dags trigger api_to_kafka_streaming

- Or trigger from Airflow UI.

## Submit Spark Job

- Enter Spark container:

docker exec -it spark-master bash

- Run:

spark-submit \
--master spark://spark-master:7077 \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/opt/spark_stream/spark_stream.py

### Verify Data in Cassandra

- Connect:

docker exec -it cassandra cqlsh

- Query:

SELECT * FROM spark_streams.created_users LIMIT 10;

### Expected Result:

<img width="1023" height="340" alt="Screenshot 2026-05-22 041216" src="https://github.com/user-attachments/assets/e888d5b9-6d0e-40ea-9fef-a705de3d5a6f" />

##Monitoring
### Airflow

- Monitor DAG execution status.

Success
Failed
Running
Queued
Kafdrop

## Kafdrop
- Monitor:

Topics
Partitions
Consumer Groups
Messages
Spark UI

## Spark UI
- Monitor:

Executors
Jobs
Stages
Streaming Queries
Cassandra

## Cassandra
- Monitor:

SELECT COUNT(*) FROM spark_streams.created_users;
<img width="476" height="123" alt="Screenshot 2026-06-02 072413" src="https://github.com/user-attachments/assets/e6beb77e-4ecc-4b02-8ebd-2a6ecd77f3d3" />

## Engineering Concepts Demonstrated
Event-Driven Architecture
Distributed Systems
Stream Processing
Workflow Orchestration
Schema Evolution
NoSQL Data Modeling
Containerization
Service Networking
Real-Time Data Pipelines
Fault-Tolerant Processing

## Author

Daniel Idowu Damilare

Data Engineer | Business Intelligence Analyst | Cloud Dataplatforms (AWS & DataBricks) | End to End data Pipelines & Solutions

### Email: danielidowudamilare@gmail.com

LinkedIn: https://www.linkedin.com/in/idowu-daniel-ba99b1270/



