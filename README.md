# Real-Time-Data-Pipeline with Airflow, Kaafka, Spark & Cassandra
## Real-Time User Data Streaming Pipeline
## Overview

This project implements a real-time end-to-end data streaming platform using Apache Airflow, Apache Kafka, Apache Spark, Cassandra, PostgreSQL, Docker, and Confluent components.

The solution continuously ingests user data from an external API, streams the data through Kafka, processes the stream using Spark Structured Streaming, and persists the transformed records into Cassandra for real-time analytics and downstream consumption.

The entire platform is containerized using Docker Compose, enabling reproducible deployments and simplified environment management.

## Pipeline Architecture

<img width="1536" height="1024" alt="ChatGPT Image Jun 2, 2026, 06_30_30 AM" src="https://github.com/user-attachments/assets/e829ec42-44f9-4e52-8b9a-9cd2f967d834" />



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
<img width="129" height="297" alt="Screenshot 2026-06-02 080214" src="https://github.com/user-attachments/assets/a7590002-a936-41ed-a195-b8ed17a8ce2a" />

## How to Run

### Clone Repository
git clone https://github.com/danielidowu1/Real-Time-Data-Pipeline.git

- cd realtime-user-streaming-pipeline
### Build Containers
docker compose build --no-cache
### Start Platform
docker compose up -d

<img width="516" height="193" alt="Screenshot 2026-06-03 134413" src="https://github.com/user-attachments/assets/a4971310-d87d-4f72-98c2-0fc1198da6f6" />


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
<img width="1342" height="471" alt="Screenshot 2026-06-03 115425" src="https://github.com/user-attachments/assets/3958634c-2646-4481-aeb5-1fd0e8803daf" />


## Submit Spark Job

- Enter Spark container:

 docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 /opt/spark_stream/spark_stream.py
 
### Verify Data in Cassandra

- Connect:

docker exec -it cassandra cqlsh

- Query:

SELECT * FROM spark_streams.created_users LIMIT 10;

### Expected Result:

<img width="1023" height="340" alt="Screenshot 2026-05-22 041216" src="https://github.com/user-attachments/assets/e888d5b9-6d0e-40ea-9fef-a705de3d5a6f" />

## Monitoring
### Airflow

- Monitor DAG execution status.

Success
Failed
Running
Queued

<img width="1358" height="541" alt="Screenshot 2026-06-03 115510" src="https://github.com/user-attachments/assets/f9ac9b95-2be8-46ec-8d1a-60349b767cc5" />


## Kafdrop
- Monitor:

Topics
Partitions
Consumer Groups
Messages

<img width="1188" height="613" alt="Screenshot 2026-06-03 115758" src="https://github.com/user-attachments/assets/6a9a2310-f49f-4898-9c43-f7d428582af6" />
<img width="1155" height="452" alt="Screenshot 2026-06-03 115836" src="https://github.com/user-attachments/assets/1f91a3d0-6729-4fb3-b2b4-23d9fe7995f3" />


## Spark UI
- Monitor:

Executors
Jobs
Stages
Streaming Queries

<img width="1336" height="444" alt="Screenshot 2026-06-03 150034" src="https://github.com/user-attachments/assets/2693400a-0f6d-4690-a7fd-e815b53a550d" />


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

Email: danielidowudamilare@gmail.com

LinkedIn: https://www.linkedin.com/in/idowu-daniel-ba99b1270/



