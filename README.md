# Real-Time-Data-Pipeline with Airflow, Kaafka, Spark & Cassandra
Real-time streaming data pipeline using Airflow, Kafka, Spark Structured Streaming, Cassandra, and Docker.

This project demonstrates a real-time streaming data pipeline built using Apache Airflow, Apache Kafka, Apache Spark Structured Streaming, and Cassandra.

The pipeline streams real-time user data from an API into Kafka, processes the stream using Spark, and stores the transformed data into Cassandra.

## Architecture

API → Airflow → Kafka → Spark Streaming → Cassandra

![Architecture](screenshots/architecture.png)

## Technologies

- Apache Airflow
- Apache Kafka
- Apache Spark
- Cassandra
- Docker
- Python
- Kafka Streams

## Features

- Real-time API data ingestion
- Kafka message streaming
- Spark Structured Streaming processing
- Cassandra real-time storage
- Airflow DAG orchestration
- Dockerized infrastructure

## How to Run

### 1. Clone Repository

git clone https://github.com/yourusername/real-time-data-pipeline.git

### 2. Start Docker Containers

docker compose up -d

### 3. Verify Kafka Topic

Open:
http://localhost:9000

### 4. Create Cassandra Keyspace & Table

docker exec -it cassandra cqlsh

Then run:

CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS spark_streams.created_users (
    id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);

### 5. Run Spark Streaming Job

docker exec -it spark-master bash

spark-submit \
--master spark://spark-master:7077 \
--packages com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 \
/opt/spark_stream/spark_stream.py

### 6. Trigger Airflow DAG

Open:
http://localhost:8080
