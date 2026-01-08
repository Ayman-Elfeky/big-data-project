# 📊 Real-Time Player Performance Analytics – Big Data

A real-time big data analytics system designed to ingest, process, and analyze player performance metrics at scale using distributed streaming technologies.

---

## 🚀 Project Overview

This project implements a **real-time big data pipeline** for analyzing live player performance data.  
It leverages **event-driven streaming**, **automated data ingestion**, and **distributed processing** to generate actionable insights in near real time.

---

## 🛠️ Technologies Used

- Apache Kafka – Real-time event streaming
- Apache NiFi – Data ingestion and flow orchestration
- Apache Spark – Distributed stream processing and analytics
- PostgreSQL – Persistent data storage
- Python – Producer & consumer implementation
- Apache ZooKeeper – Kafka coordination

---

## 🧱 System Architecture

```
Kafka Producer (Python)
        ↓
     Apache Kafka
        ↓
     Apache NiFi
        ↓
 Apache Spark Streaming
        ↓
    PostgreSQL
```

---

## 📋 Prerequisites

Make sure the following are installed and running:

- Java 8+
- Python 3.8+
- Apache ZooKeeper
- Apache Kafka
- Apache NiFi
- Apache Spark
- PostgreSQL

---

## ⚙️ Setup & Installation

### 1️⃣ Start ZooKeeper
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

---

### 2️⃣ Start Apache Kafka
```bash
bin/kafka-server-start.sh config/server.properties
```

Create a Kafka topic:
```bash
bin/kafka-topics.sh --create \
  --topic player-performance \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

### 3️⃣ Start Apache NiFi
```bash
bin/nifi.sh start
```

Access the NiFi UI:
```
http://localhost:8080/nifi
```

Configure NiFi to consume data from Kafka and forward it to Spark.

> ⚠️ Do NOT hardcode credentials.  
> Use environment variables or configuration files.

---

### 4️⃣ Setup PostgreSQL

Create database and table:
```sql
CREATE DATABASE player_analytics;

CREATE TABLE player_metrics (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50),
    metric_name VARCHAR(100),
    metric_value FLOAT,
    event_time TIMESTAMP
);
```

---

## ▶️ Running the Project

### Kafka Producer
Streams player performance events to Kafka:
```bash
spark-submit kafka_producer.py
```

---

### Spark Consumer
Consumes Kafka streams, processes data, and stores results in PostgreSQL:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,\
org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.1,\
org.postgresql:postgresql:42.7.3 \
  spark_consumer.py
```

---

## 📈 Output

- Real-time processed player performance metrics
- Structured data stored in PostgreSQL
- Scalable and fault-tolerant streaming pipeline

---

## 🔒 Security Notes

- Never commit usernames or passwords to GitHub
- Use `.env` files or environment variables
- Add `.env` to `.gitignore`

---

## 📽️ Demo
- Video Demo: [https://drive.google.com/file/d/1zoSeHsiQJi_xYmCBGOshto6U8VfHs3Gs/view?usp=sharing](Link)

