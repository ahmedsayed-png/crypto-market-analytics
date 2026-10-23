# Pipeline Operations and Architecture Runbook

This guide documents operational procedures, Spark job submission commands, HDFS data lake layout, and Airflow orchestration for the **Crypto Big Data Engineering** platform.

---

## 1. High-Level Architecture Overview

```
                      +-----------------------------+
                      |   Binance Market Endpoints  |
                      |  (REST API & WebSocket API) |
                      +--------------+--------------+
                                     |
                                     v
                        +-------------------------+
                        |   Producer Service      |
                        |   (Python Kafka Client) |
                        +------------+------------+
                                     |
                                     v
                        +-------------------------+
                        |   Apache Kafka (KRaft)  |
                        |   - crypto_klines_hist  |
                        |   - crypto_klines_stream|
                        +------------+------------+
                                     |
                  +------------------+------------------+
                  |                                     |
                  v                                     v
     +--------------------------+          +--------------------------+
     | Spark Batch Ingestion    |          | Spark Structured         |
     | (historical_processing)  |          | Streaming Processing     |
     +------------+-------------+          +------------+-------------+
                  |                                     |
                  +------------------+------------------+
                                     |
        +----------------------------+----------------------------+
        |                                                         |
        v                                                         v
+-------------------------------+                         +---------------+
| Hadoop HDFS Data Lake         |                         | PostgreSQL DW |
| - /data/crypto/historical/    |                         | (fact_klines) |
| - /data/crypto/streaming/     |                         +-------+-------+
| - /data/crypto/rejected/      |                                 |
| - /data/crypto/features/      |                                 v
+---------------+---------------+                         +---------------+
                |                                         | Apache        |
                v                                         | Superset      |
+-------------------------------+                         +---------------+
| Feature Engineering (MLlib)   |
| Spark SQL Analytical Queries  |
+-------------------------------+
```

---

## 2. HDFS Data Lake Architecture

Data in the Hadoop HDFS lake is stored in Apache Parquet format with Snappy compression, partitioned by cryptocurrency symbol and date:

| HDFS Directory Path | Purpose | Format | Partitioning Scheme |
| :--- | :--- | :--- | :--- |
| `/data/crypto/historical/klines` | Golden historical batch Kline dataset | Parquet (Snappy) | `/symbol=<SYMBOL>/kline_date=<YYYY-MM-DD>/` |
| `/data/crypto/historical/rejected` | Quarantined malformed / invalid messages | JSON lines | Timestamped error payloads |
| `/data/crypto/streaming/klines` | Golden real-time Kline dataset | Parquet (Snappy) | `/symbol=<SYMBOL>/kline_date=<YYYY-MM-DD>/` |
| `/data/crypto/checkpoints/` | Persistent Spark Structured Streaming checkpoints | Internal Spark checkpoint | `commits/`, `offsets/`, `sources/`, `metadata` |
| `/data/crypto/features/klines` | MLlib Feature engineered vectors & technical indicators | Parquet (Snappy) | `/symbol=<SYMBOL>/kline_date=<YYYY-MM-DD>/` |

### Inspecting HDFS from Host

```bash
# List top-level lake directories
docker exec namenode hdfs dfs -ls /data/crypto

# List historical partitions
docker exec namenode hdfs dfs -ls /data/crypto/historical/klines

# View quarantined rejected records (if any)
docker exec namenode hdfs dfs -cat /data/crypto/historical/rejected/*
```

---

## 3. Spark Applications Submission Guide

All Spark applications must be submitted using `/spark/bin/spark-submit` inside the `spark-master` container (or via Airflow Bash/Docker operators). Required dependencies include the Spark Kafka integration package and the PostgreSQL JDBC driver.

### A. Historical Batch Ingestion (`historical_processing.py`)

Consumes historical records from Kafka, validates schema and price sanity, writes clean records to HDFS Parquet, routes corrupt records to quarantine, and merges clean records idempotently into PostgreSQL DW `fact_klines_min`.

```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 \
    --conf spark.sql.session.timeZone=UTC \
    /opt/spark-apps/historical_processing.py \
        --kafka-bootstrap kafka:9092 \
        --kafka-topic crypto_klines_historical \
        --starting-offsets earliest \
        --hdfs-namenode hdfs://namenode:9000 \
        --hdfs-output-path /data/crypto/historical/klines \
        --hdfs-rejected-path /data/crypto/historical/rejected \
        --enable-postgres \
        --postgres-host postgres-dw \
        --postgres-db crypto_dw \
        --postgres-user crypto_admin \
        --postgres-password crypto_password
```

### B. Real-Time Structured Streaming (`streaming_processing.py`)

Consumes live Kline events with event-time watermarking (`10 minutes`), writes micro-batches to partitioned HDFS Parquet, commits offsets, and optionally syncs to PostgreSQL DW.

**Smoke-Test Mode (Bounded execution, processes available messages and exits):**
```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 \
    --conf spark.sql.session.timeZone=UTC \
    /opt/spark-apps/streaming_processing.py \
        --kafka-bootstrap kafka:9092 \
        --kafka-topic crypto_klines_streaming \
        --starting-offsets earliest \
        --hdfs-namenode hdfs://namenode:9000 \
        --hdfs-output-path /data/crypto/streaming/klines \
        --hdfs-checkpoint-path /data/crypto/checkpoints/streaming_klines \
        --smoke-test
```

**Continuous Streaming Mode (daemon / production):**
```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 \
    --conf spark.sql.session.timeZone=UTC \
    /opt/spark-apps/streaming_processing.py \
        --kafka-bootstrap kafka:9092 \
        --kafka-topic crypto_klines_streaming \
        --trigger-interval "10 seconds" \
        --hdfs-namenode hdfs://namenode:9000 \
        --hdfs-output-path /data/crypto/streaming/klines \
        --hdfs-checkpoint-path /data/crypto/checkpoints/streaming_klines
```

### C. Analytical Queries & Aggregation (`spark_sql.py`)

Reads Parquet data directly from the HDFS data lake, executes analytical aggregations (OHLCV metrics, trade volumes, daily summaries), and prints formatted results:

```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.5.4 \
    /opt/spark-apps/spark_sql.py \
        --hdfs-namenode hdfs://namenode:9000 \
        --hdfs-input-path /data/crypto/historical/klines
```

### D. MLlib Feature Engineering (`feature_engineering.py`)

Computes statistical and technical indicators over historical Kline series:
- Simple Moving Averages: SMA(7), SMA(20), SMA(50)
- Rolling Price Volatility (20-period standard deviation)
- Bollinger Bands (Upper, Lower, Bandwidth)
- Momentum / Price Rate of Change (ROC)
- Rolling Volume ratios

Output is persisted back to `/data/crypto/features/klines` on HDFS:

```bash
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/feature_engineering.py \
        --hdfs-namenode hdfs://namenode:9000 \
        --hdfs-input-path /data/crypto/historical/klines \
        --hdfs-output-path /data/crypto/features/klines
```

---

## 4. Airflow Workflow Orchestration

Airflow manages scheduled ingestion batches and automated data quality audits.

### Configured DAGs

| DAG ID | Schedule | Purpose |
| :--- | :--- | :--- |
| `crypto_historical_batch_pipeline` | Manual (`None`) | Orchestrates bounded producer ingestion, Spark batch processing, and Postgres DW sync |
| `crypto_data_quality_audit` | `@hourly` | Audits row counts, asserts zero NULLs, checks price boundaries ($High \ge Low$), and validates HDFS status |

### Operating Airflow via CLI

```bash
# Check DAGs for import errors
docker exec airflow-webserver airflow dags list-import-errors

# Trigger manual data quality audit
docker exec airflow-webserver airflow dags trigger crypto_data_quality_audit

# Inspect execution state of recent runs
docker exec airflow-webserver airflow dags list-runs -d crypto_data_quality_audit
```

Airflow Web UI is accessible on port `8082` (`http://localhost:8082`).

---

## 5. PostgreSQL Analytical Warehouse

The analytical warehouse hosts the star schema with `dim_symbol`, `dim_date`, `fact_klines_min`, and analytical views.

### Querying Views via CLI

```bash
docker exec -i postgres-dw psql -U crypto_admin -d crypto_dw -c "
SELECT symbol, COUNT(*) as rows, MIN(open_time) as earliest, MAX(open_time) as latest
FROM fact_klines_min
GROUP BY symbol;
"
```
