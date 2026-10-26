# Real-Time Cryptocurrency Market Analytics

## Overview

This repository hosts a Big Data and Data Engineering system architecture designed for collecting, processing, storing, and visualizing both historical and real-time cryptocurrency market data.

The system integrates market data from the **Binance REST API** (for historical data) and the **Binance WebSocket API** (for live streaming data). Ingestion is mediated through **Apache Kafka** as a centralized event streaming and buffering layer, while **Apache Spark** serves as the unified processing engine for both batch and real-time streaming operations.

> **Status Notice:** The core data engineering infrastructure is **fully operational and verified**. Docker Compose services, Kafka KRaft streaming ingestion, Spark Structured Streaming, HDFS Parquet storage, PostgreSQL Data Warehouse, Airflow DAG orchestration, and Superset BI dashboards are running and integrated. Downstream ML and Visualization teams can connect to Kafka topics, HDFS partitions, and PostgreSQL analytical views directly.

## System Architecture

The baseline architecture handles dual ingestion flows (historical batch and live streaming) routed through Kafka to Spark for transformation, persistence, serving, and downstream visualization.

```
Binance REST API ─────┐
                      ├──> Python Producers
Binance WebSocket ────┘
                              ↓
                            Kafka
                              ↓
                    Apache Spark
                    /           \
             Batch Processing   Streaming
                    \           /
                     Processing
                          ↓
                  HDFS / Parquet
                          ↓
                     PostgreSQL
                          ↓
                      Superset
```

Additional core components include:
- **Spark SQL**: Analytical queries and multi-pair transformations.
- **Spark MLlib** *(Planned)*: Machine learning feature pipelines and price movement prediction.
- **Apache Airflow** *(Planned)*: Workflow orchestration, job scheduling, and pipeline monitoring.

## Technologies

- **Python**: Producer development and script execution environment.
- **Binance REST API**: Historical OHLCV Kline data retrieval.
- **Binance WebSocket**: Real-time market tick and candlestick streaming.
- **Apache Kafka**: Distributed event streaming, message buffering, and decoupling.
- **Apache Spark**: Scalable distributed data processing.
- **Spark Structured Streaming**: Real-Time streaming data processing.
- **Spark SQL**: Distributed relational queries over analytical datasets.
- **Spark MLlib**: Machine learning library for feature engineering and predictive modeling.
- **Hadoop HDFS**: Distributed persistent storage data lake.
- **Parquet**: Columnar file format optimized for analytical queries.
- **PostgreSQL**: Serving data warehouse for analytical query results and dashboard support.
- **Apache Superset**: Business intelligence and dashboard visualization interface.
- **Apache Airflow**: Data pipeline and DAG workflow orchestration engine.
- **Docker**: Containerization platform.
- **Docker Compose**: Container orchestration and local multi-service management.
- **Jupyter**: Interactive notebooks for exploratory data analysis and algorithm prototyping.

## Project Structure

```
crypto-market-analytics/
├── airflow/
│   └── dags/                # Airflow DAG workflow definitions (planned)
├── data/                    # Local temporary data directory (gitignored)
├── docs/                    # Technical documentation & data dictionaries
│   ├── architecture/        # Architectural diagrams and design specifications
│   └── data_dictionary/     # Data schemas and field definitions
├── kafka/                   # Kafka configuration and topic creation scripts
├── producer/                # Python ingestion producers
│   ├── historical/          # REST API batch ingestion script placeholders
│   ├── streaming/           # WebSocket real-time streaming script placeholders
│   ├── Dockerfile           # Producer container containerization draft
│   └── requirements.txt     # Python producer library dependencies
├── spark/                   # Apache Spark processing framework
│   ├── Dockerfile           # Base Spark image definition
│   ├── Dockerfile.master    # Spark Master container image
│   ├── Dockerfile.worker    # Spark Worker container image
│   └── apps/                # PySpark applications (batch, streaming, features, SQL)
├── hadoop/                  # Hadoop HDFS configuration variables
├── postgres-dw/             # PostgreSQL Data Warehouse initialization DDL scripts
├── superset/                # Apache Superset initialization & config files
├── jupyter/                 # Jupyter notebook environment for PySpark exploration
├── .gitignore               # Version control exclusion rules
├── .env.example             # Environment configuration placeholders template
└── README.md                # Project documentation
```

## Data Flow

### 1. Historical Data Ingestion
Historical data flow follows the path:
`Binance REST API → Python Producer → Kafka → Spark Batch Processing`
Historical data will be pulled incrementally in batches via REST API endpoints to avoid API rate limits and memory bottlenecks, pushing records directly into designated Kafka topics before batch ingestion into Spark.

### 2. Real-Time Data Streaming
Real-time streaming flow follows the path:
`Binance WebSocket → Python Streaming Producer → Kafka → Spark Structured Streaming`
Live price and candlestick data are continuously ingested over WebSocket connections and published immediately to Kafka. Spark Structured Streaming consumes these events with planned automatic reconnect and offset checkpointing.

### 3. Kafka Ingestion Layer
Kafka is planned as the central event broker responsible for:
- Event streaming and low-latency message queueing
- Buffering peak data volumes
- Decoupling producers (Binance collectors) from consumers (Spark)
- Guarantees on message delivery and configurable retention policies

*Note: Kafka will serve purely as an ingestion/buffering broker and does NOT perform data transformations.*

### 4. Spark Processing Layer
Apache Spark is planned as the primary computational engine for both batch and streaming pipelines. It will execute:
- Data cleaning and type validation
- Filtering invalid records and null checks
- Schema normalization and standardization
- Windowed time-series aggregations
- Feature engineering (technical indicators)
- Spark SQL query execution
- Machine learning data preparation

### 5. Storage Layer
- **HDFS**: Persistent distributed file storage designed as the primary Big Data storage lake.
- **Parquet**: Storage format using columnar compression to optimize I/O and query speed for Spark reads.
- **PostgreSQL**: Serving relational data warehouse planned for aggregated, dashboard-ready datasets and inference outputs.

### 6. Analytics and Machine Learning
- **Spark SQL**: Planned for running ad-hoc analytical queries across historical datasets.
- **Spark MLlib**: Planned for model training to forecast cryptocurrency price movement trends. *(Models are planned for future phases).*

### 7. Visualization
**Apache Superset** will provide analytical dashboards. Planned dashboard categories include:
- Market Overview
- Coin Analysis (featuring dynamic symbol filters)
- Price Analysis
- Volume Analysis
- Technical Indicators
- Cryptocurrency Comparison
- Prediction Results
- Real-Time Monitoring

### 8. Orchestration
**Apache Airflow** will orchestrate end-to-end data workflows:
- Triggering incremental historical ingestion jobs
- Scheduling regular batch data quality checks
- Managing database maintenance tasks
- Automating model retraining pipelines

## Historical Market Data

Historical datasets will originate from the Binance REST API Kline endpoint using the following parameters:
- **Base Granularity**: 1-minute candlesticks (Klines)
- **Time Horizon**: Approximately 1 year of historical data
- **Target Pairs**: Approximately 30 cryptocurrency pairs (e.g., `BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`, `ADAUSDT`, `DOGEUSDT`, `AVAXUSDT`, `DOTUSDT`, `LINKUSDT`, `LTCUSDT`, `TRXUSDT`, `SHIBUSDT`, `ATOMUSDT`, `UNIUSDT`, `ETCUSDT`, `FILUSDT`, `APTUSDT`, `NEARUSDT`).

### Logical Schema Mapping
Raw array payloads from Binance will be normalized to the following standard schema:
`[timestamp, symbol, open, high, low, close, volume, quote_volume, number_of_trades, taker_buy_volume, taker_buy_quote_volume]`

*(The raw Binance `ignore` field will be discarded during processing).*

## Real-Time Streaming

Real-time streaming will rely on Binance WebSocket feeds. Live WebSocket messages will be formatted, validated for basic structure, and pushed to Kafka for real-time windowed aggregation by Spark Structured Streaming.

## Kafka

Kafka topics will be provisioned to logically separate historical batch streams from live WebSocket feeds:
- `crypto_klines_historical` *(Proposed)*: Partitioned topic for incremental historical batch ingestion.
- `crypto_klines_streaming` *(Proposed)*: Partitioned topic for real-time streaming updates.

## Spark Processing

Spark will handle both batch and streaming computational workloads:
- **Batch Processing**: Periodically process raw historical blocks stored in Kafka, clean and transform OHLCV fields, calculate baseline metrics, and write compressed Parquet files to HDFS.
- **Structured Streaming**: Consume live Kafka streams using tumbling or sliding time windows to maintain live metrics updated in near real-time.

## Storage Layer

### HDFS
HDFS will act as the distributed storage system housing historical Parquet files across partitioned directories organized by symbol and date.

### Parquet
Data will be stored using Snappy-compressed Parquet files, optimized for high-throughput column scanning in Spark SQL.

### PostgreSQL
Will serve as the low-latency analytical serving database for Superset, holding pre-aggregated tables and predictive outputs.

## Apache Superset

Apache Superset will connect directly to PostgreSQL to render visual dashboards. The design will use dynamic filter controls (e.g., dynamic symbol selector) allowing a single unified Coin Analysis dashboard to serve all target trading pairs dynamically.

## Apache Airflow

Airflow will act as the control plane running DAGs for batch ingestion, HDFS maintenance, data validation checks, and scheduled batch transformations.

## Quickstart & Service Access

### 1. Environment Setup & Launch
```bash
# Clone and enter the repository
git clone https://github.com/ahmedsayed-png/crypto-market-analytics.git
cd crypto-market-analytics

# Copy configuration template (defaults are pre-configured for local/team run)
cp .env.example .env

# Launch all 14 infrastructure and processing services
docker compose up -d
```

### 2. Service Endpoints & Web Interfaces

| Service | Host Port / URL | Internal Network Address | Purpose |
|---|---|---|---|
| **Apache Superset** | [http://localhost:8088](http://localhost:8088) | `superset:8088` | BI Dashboards (`/superset/dashboard/crypto-market-analytics/`) |
| **Apache Airflow** | [http://localhost:8082](http://localhost:8082) | `airflow-webserver:8080` | DAG Orchestration & Data Quality Audits |
| **Kafka UI** | [http://localhost:8090](http://localhost:8090) | `kafka-ui:8080` | Topic Offsets, Message Browser & Consumer Lag |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | `spark-master:8080` (RPC `7077`) | Cluster Worker & Application Status |
| **Spark Streaming UI**| [http://localhost:4040](http://localhost:4040) | `spark-streaming:4040` | Structured Streaming Micro-batch Metrics |
| **JupyterLab** | [http://localhost:8888](http://localhost:8888) | `jupyter:8888` | Interactive PySpark Prototyping & EDA |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | `namenode:9000` (RPC) | Distributed Storage Lake Browser |
| **PostgreSQL DW** | `localhost:5432` | `postgres-dw:5432` | Data Warehouse (`crypto_dw`) |
| **Apache Kafka** | `localhost:9094` (External) | `kafka:9092` (Internal) | KRaft Event Broker |

### 3. Pipeline Health & Sanity Verification

```bash
# Check all 14 containers are running and healthy
docker compose ps

# Run producer unit tests (schema normalization, message validation)
docker exec -w /app crypto-producer python3 -m unittest discover -s /tests

# Trigger Airflow automated data quality DAG (validates DW, Lake, & Streaming)
docker exec airflow-scheduler airflow dags trigger crypto_data_quality_audit

# Inspect live Spark Structured Streaming micro-batch commits
docker logs --tail 20 spark-streaming

# Query live market summary from PostgreSQL Data Warehouse
docker exec postgres-dw psql -U crypto_admin -d crypto_dw -c "SELECT * FROM vw_market_overview;"
```

### 4. Downstream Team Data Access

- **Machine Learning Team**:
  - HDFS Parquet Data Lake: `hdfs://namenode:9000/data/crypto/streaming/klines/` (partitioned by `symbol=<SYM>/kline_date=<YYYY-MM-DD>`).
  - Feature View: `public.vw_coin_analysis` in PostgreSQL DW (`crypto_dw`).
  - Prediction Sink: `public.fact_predictions` (seeded DDL ready for ML inferences).
- **Visualization Team**:
  - Apache Superset: `http://localhost:8088` (Default dashboard: `/superset/dashboard/crypto-market-analytics/`).
  - Pre-connected Database: `Crypto Data Warehouse` (`postgres-dw:5432/crypto_dw` internally via Docker network).
  - Pre-registered Datasets: `vw_market_overview`, `vw_coin_analysis`, `fact_klines_min`, `fact_klines_daily`, `fact_predictions`.

### 5. Troubleshooting Common Issues

- **Producer in Dry-Run Mode**: Ensure `FORCE_RUN=true` is set in `.env` if operating without private Binance API keys.
- **Spark Worker Memory/Cores**: Standalone worker is allocated 2 cores and 3.0 GiB RAM. The continuous streaming job utilizes 2 cores; batch jobs queued via Airflow execute concurrently as slots permit.
- **Kafka Persistence**: Topic partition logs and cluster metadata are stored in the persistent Docker volume `crypto-market-analytics_kafka_data` via `KAFKA_LOG_DIRS=/var/lib/kafka/data`. Never run `docker compose down -v` to prevent volume deletion.
- **Detailed Reference**: See [crypto_infrastructure_audit_and_handoff.md](file:///home/ahmed/.gemini/antigravity/brain/f2c2230d-a37e-4a08-a44f-756dc1e31636/crypto_infrastructure_audit_and_handoff.md) for full schemas and disaster recovery procedures.

## Project Objective

The core Data Engineering objectives of this project include:
1. **Large-Scale Ingestion**: High-throughput historical REST ingestion & low-latency WebSocket streaming.
2. **Distributed Processing**: Scalable batch and stream processing with Apache Spark.
3. **Data Transformation & Cleansing**: Standardization of raw exchange data.
4. **Feature Engineering**: Time-series technical indicator calculation at scale.
5. **Analytical Storage**: Hybrid distributed file system (HDFS/Parquet) and database warehouse (PostgreSQL).
6. **Business Intelligence**: Rich interactive visual dashboards via Superset.
7. **ML Readiness**: Scalable feature vector generation for price trend predictions.

## Future Improvements

- Expanding target pairs beyond the initial 30 symbols.
- Adding advanced custom technical indicators (MACD, Bollinger Bands, Fibonacci levels).
- Implementing Great Expectations for data quality validation.
- Enhanced streaming failover and auto-reconnect handling.
- Machine learning model experimentation and drift monitoring.
- Real-time price threshold alerting systems.
- Production deployment on cloud infrastructure (AWS / GCP / K8s).
