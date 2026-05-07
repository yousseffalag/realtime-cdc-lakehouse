# 📡 Debezium CDC Connector (PostgreSQL → Kafka)

This module is responsible for capturing real-time changes from PostgreSQL and streaming them into Apache Kafka using Debezium (Kafka Connect).

---

## 🚀 Overview

Debezium enables Change Data Capture (CDC) by reading PostgreSQL WAL (Write-Ahead Logs) and publishing database changes as events into Kafka topics.

### 🔁 Data Flow

```
PostgreSQL (cdc_db)
        ↓  WAL (logical replication)
Debezium Connector
        ↓
Kafka Topics (cdc.public.*)
        ↓
Stream Processing / Lakehouse / Analytics
```

---

## ⚙️ Prerequisites

Ensure the following services are running:

* PostgreSQL (with logical replication enabled)
* Apache Kafka
* Zookeeper
* Kafka Connect (Debezium container)

Check Kafka Connect availability:

```bash
curl http://localhost:8083
```

---

## 📦 Connector Configuration

The connector is registered using:

* `register-connector.sh`

Main configuration includes:

* PostgreSQL source database (`cdc_db`)
* Logical replication plugin (`pgoutput`)
* Topic prefix: `cdc`
* Replication slot: `cdc_slot`
* Snapshot mode: `never`

---

## 🧩 Available Scripts

### 1. Register Connector

```bash
./register-connector.sh
```

---

### 2. Check Connector Status

```bash
./check-connectors.sh
```

Or manually:

```bash
curl http://localhost:8083/connectors/postgres-cdc/status
```

---

### 3. Delete Connector

```bash
./delete-connector.sh
```

---

## 📊 Kafka Topics

Debezium automatically generates topics using:

```
cdc.<schema>.<table>
```

### Example topics:

* `cdc.public.customers`
* `cdc.public.orders`
* `cdc.public.products`

List topics:

```bash
kafka-topics --list --bootstrap-server kafka:9092
```

---

## 🧪 Testing CDC Pipeline

### 1. Start Kafka consumer

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.public.customers \
  --from-beginning
```

---

### 2. Insert data into PostgreSQL

```sql
INSERT INTO customers (full_name, city, segment)
VALUES ('CDC Test User', 'Agadir', 'vip');
```

---

### 3. Expected Kafka output

```json
{
  "op": "c",
  "after": {
    "full_name": "CDC Test User",
    "city": "Agadir",
    "segment": "vip"
  }
}
```

---

## ⚠️ Common Issues

### ❌ Permission denied for database

```sql
GRANT CONNECT ON DATABASE cdc_db TO debezium;
GRANT ALL PRIVILEGES ON DATABASE cdc_db TO debezium;
```

---

### ❌ Connector task FAILED

Check:

* PostgreSQL user permissions
* Table ownership
* Replication slot availability

---

### ❌ No messages in Kafka

Verify:

* Connector status = RUNNING
* Topic exists
* New INSERT performed after connector start

---

## 🧠 Key Concepts

* Debezium reads PostgreSQL WAL (not polling)
* Only new changes are streamed (real-time CDC)
* Each table = Kafka topic
* Events include: `c` (insert), `u` (update), `d` (delete)

---

## 🏗️ Recommended Next Steps

* Kafka → Spark Structured Streaming
* Data Lake storage (MinIO / Iceberg)
* Transform events into clean models
* Schema Registry (Avro/Protobuf)
* Real-time dashboards (Superset)

---

## 👨‍💻 Project

CDC Real-Time Lakeho
