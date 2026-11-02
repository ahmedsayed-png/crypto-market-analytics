# Real-Time Cryptocurrency Market Analytics

## Overview

This repository hosts a production-grade Big Data and Data Engineering system architecture designed for collecting, processing, storing, and visualizing both historical and real-time cryptocurrency market data.

The system integrates market data from the **Binance REST API** (for historical data) and the **Binance WebSocket API** (for live streaming data). Ingestion is mediated through **Apache Kafka** as a centralized event streaming and buffering layer, while **Apache Spark** serves as the unified processing engine for both batch and real-time streaming operations.

> **Status Notice:** The core data engineering infrastructure and dual ingestion pipelines are **fully operational, standardized, and verified**. All 14 Docker Compose services, Kafka KRaft streaming ingestion, Spark Structured Streaming, HDFS Parquet storage, PostgreSQL Data Warehouse, Airflow DAG orchestration, and Superset BI dashboards are running and integrated. Downstream ML and Visualization teams can connect to Kafka topics, HDFS partitions, and PostgreSQL analytical views directly.

---

## System Architecture

The dual-path ingestion architecture handles both historical batch ingestion and live real-time streaming routed through Apache Kafka to Apache Spark for validation, normalization, distributed storage, and analytical serving.

```
┌─────────────────────────────────────────┐      ┌─────────────────────────────────────────┐
│            Binance REST API             │      │          Binance WebSocket API          │
│       (/api/v3/klines Endpoint)         │      │     (/ws/<symbol>@kline_<interval>)     │
└────────────────────┬────────────────────┘      └────────────────────┬────────────────────┘
                     │                                                │
                     ▼                                                ▼
┌─────────────────────────────────────────┐      ┌─────────────────────────────────────────┐
│        Historical REST Producer         │      │      Real-Time Streaming Producer       │
│  • Configurable lookback / timestamps   │      │  • Dynamic multi-symbol multiplexing    │
│  • Pagination (1000 candles/chunk)      │      │  • Auto-reconnect with exp. backoff     │
│  • Exponential backoff on 429/418       │      │  • Watchdog heartbeat connection check  │
│  • Canonical schema normalization       │      │  • Provisional vs. closed candle tags   │
└────────────────────┬────────────────────┘      └────────────────────┬────────────────────┘
                     │                                                │
                     ▼                                                ▼
┌─────────────────────────────────────────┐      ┌─────────────────────────────────────────┐
│        crypto_klines_historical         │      │         crypto_klines_streaming         │
│   (Kafka Topic: 3 partitions, 7d ret)   │      │   (Kafka Topic: 3 partitions, 24h ret)  │
│   Key: <symbol>_<interval>_<open_time>  │      │   Key: <symbol>_<interval>_<open_time>  │
└────────────────────┬────────────────────┘      └────────────────────┬────────────────────┘
                     │                                                │
                     ▼                                                ▼
┌─────────────────────────────────────────┐      ┌─────────────────────────────────────────┐
│         Apache Spark Batch Job          │      │     Spark Structured Streaming Job      │
│     (spark/apps/historical_processing)  │      │    (spark/apps/streaming_processing)    │
│  • Consumes crypto_klines_historical    │      │  • Consumes crypto_klines_streaming     │
│  • Schema coalescing (20 fields)        │      │  • 10-second micro-batch trigger        │
│  • Deduplication by (symbol, open_time) │      │  • 15-minute event-time watermark       │
│  • Date partition derivation            │      │  • Stateful offset checkpointing        │
└────────────┬───────────────────┬────────┘      └────────────┬───────────────────┬────────┘
             │                   │                            │                   │
             │                   │ (All records)              │ (is_closed==True) │ (All records)
             ▼                   ▼                            ▼                   ▼
┌──────────────────────┐ ┌──────────────────────────────────────────────┐ ┌──────────────────────┐
│    PostgreSQL DW     │ │               Hadoop HDFS Lake               │ │    PostgreSQL DW     │
│   fact_klines_min    │ │       /data/crypto/historical/klines/        │ │   fact_klines_min    │
│ (Anti-join conflict) │ │        /data/crypto/streaming/klines/        │ │ (ON CONFLICT update) │
└────────────┬─────────┘ └──────────────────────────────────────────────┘ └──────────┬───────────┘
             │                                                                       │
             └───────────────────────────────────┬───────────────────────────────────┘
                                                 │
                                                 ▼
                                 ┌───────────────────────────────┐
                                 │    PostgreSQL Views & Marts   │
                                 │    • vw_market_overview       │
                                 │    • vw_coin_analysis         │
                                 │    • fact_predictions (ML)    │
                                 └───────────────┬───────────────┘
                                                 │
                                                 ▼
                                 ┌───────────────────────────────┐
                                 │   Apache Superset Dashboard   │
                                 │  • Interactive Visualizations │
                                 │  • Dynamic symbol filtering   │
                                 └───────────────────────────────┘
```

---

## Unified Event Schema

To ensure seamless interoperability between historical batch files and live WebSocket streaming, all producers publish records conforming to the **Canonical Kline Message Schema**.

### Canonical Field Specifications

| Field | Data Type | Spark SQL Type | Format / Unit | Description | Ingestion Source |
|---|---|---|---|---|---|
| `symbol` | `str` | `StringType` | Uppercase (e.g., `BTCUSDT`) | Standard trading pair ticker symbol | REST & WebSocket |
| `interval` | `str` | `StringType` | `1m`, `5m`, `1h`, `1d`, etc. | Candlestick time window interval | REST (`1m` default) / WS (`i` field) |
| `open_time` | `int` | `LongType` | Unix epoch milliseconds (UTC) | Start timestamp of the candle window | REST array index 0 / WS `t` |
| `close_time` | `int` | `LongType` | Unix epoch milliseconds (UTC) | End timestamp of the candle window | REST array index 6 / WS `T` |
| `open` | `float` | `DoubleType` | Decimal price string/float | Opening traded price in quote asset | REST array index 1 / WS `o` |
| `high` | `float` | `DoubleType` | Decimal price string/float | Highest traded price during the interval | REST array index 2 / WS `h` |
| `low` | `float` | `DoubleType` | Decimal price string/float | Lowest traded price during the interval | REST array index 3 / WS `l` |
| `close` | `float` | `DoubleType` | Decimal price string/float | Closing traded price during the interval | REST array index 4 / WS `c` |
| `volume` | `float` | `DoubleType` | Decimal base volume | Total base asset volume traded | REST array index 5 / WS `v` |
| `quote_volume` | `float` | `DoubleType` | Decimal quote volume | Total quote asset volume traded | REST array index 7 / WS `q` |
| `number_of_trades`| `int` | `LongType` | Integer count | Total trade transactions during interval | REST array index 8 / WS `n` |
| `taker_buy_volume`| `float` | `DoubleType` | Decimal base volume | Taker buy volume in base asset | REST array index 9 / WS `V` |
| `is_closed` | `bool` | `BooleanType` | `True` or `False` | Candle completion status indicator | REST (`True`) / WS (`x` boolean) |
| `event_time` | `int` | `LongType` | Unix epoch milliseconds (UTC) | Timestamp when the event was emitted | REST (`open_time`) / WS (`E` header) |
| `source` | `str` | `StringType` | `binance_rest` or `binance_ws` | Identifier of the originating feed | System tagging |

### Provisional vs. Closed Candles (`is_closed`)

- **Historical REST Data**: Every candle retrieved from `/api/v3/klines` represents a completed, immutable historical interval. Therefore, `is_closed` is **always `True`**.
- **Real-Time WebSocket Data**: Binance pushes continuous streaming updates every 250ms–2000ms as trades occur within the current minute. The payload contains `k.x`:
  - `is_closed: False` indicates an **in-progress provisional candle**. High, low, close, and volume will continue fluctuating.
  - `is_closed: True` indicates the **finalized closed candle** emitted when the minute concludes.
- **Downstream Integrity Rule**:
  - The **HDFS Data Lake** ingests **all** events (`is_closed IN (True, False)`) to preserve the raw tick-level micro-progression for advanced quantitative analysis.
  - The **PostgreSQL Data Warehouse (`fact_klines_min`)** enforces a unique constraint on `(symbol, open_time)`. If provisional candles were inserted into the warehouse, the first provisional tick would lock the row, causing subsequent updates or the final closed candle to be rejected or overwritten prematurely. Spark Structured Streaming explicitly filters `is_closed == True` before sinking to PostgreSQL, ensuring only authoritative closed candles populate relational analytical tables.

### Backward-Compatible Aliases

The validation and normalization layer in `producer/schema.py` automatically handles legacy schema naming:
- `quote_asset_volume` → `quote_volume`
- `taker_buy_base_volume` → `taker_buy_volume`
- `data_source` → `source`
- `timestamp` / `kline_open_time` → `open_time`

---

## Kafka Layer Architecture

Apache Kafka (operating in KRaft mode without ZooKeeper) acts as the high-throughput, low-latency event buffering backbone.

### Topic Specifications

| Topic Name | Purpose | Partitions | Replication | Retention Period | Cleanup Policy | Compression |
|---|---|---|---|---|---|---|
| `crypto_klines_historical` | Buffer for historical REST batches before Spark batch processing | 3 | 1 | 7 days (`604,800,000 ms`) | `delete` | `snappy` |
| `crypto_klines_streaming` | Low-latency stream for live WebSocket candlestick updates | 3 | 1 | 24 hours (`86,400,000 ms`) | `delete` | `snappy` |

### Partition Key Strategy

Messages are published using deterministic composite keys:
```text
<symbol>_<interval>_<open_time>
Example: BTCUSDT_1m_1700000000000
```
- **Ordering Guarantee**: Kafka guarantees total order per partition. By including `symbol` and `open_time` in the key, all provisional updates and the final closed update for a specific candle map to the exact same Kafka partition in strict temporal sequence.
- **Balanced Load Distribution**: Hashing the composite key across 3 partitions evenly distributes traffic across Spark executors without creating hot spots.

---

## Historical Ingestion Pipeline

The historical ingestion pipeline retrieves historical OHLCV Kline data via the Binance REST API `/api/v3/klines`.

### Key Features
1. **Multi-Symbol & Multi-Interval Support**: Ingests arbitrary trading pairs (defaults to top 10 market cap pairs) at configurable intervals (`1m`, `5m`, `15m`, `1h`, `1d`).
2. **Chunked Pagination**: Automatically chunks requested lookback windows into 1,000-candle batches (Binance API maximum per request) and steps forward in time.
3. **Resilient Rate-Limit Handling**:
   - Inspects Binance rate limit headers (`X-MBX-USED-WEIGHT-1M`).
   - Dynamically backs off when approaching quota thresholds.
   - Automatically catches HTTP `429` (Rate limit exceeded) and `418` (IP auto-ban IP warning), applying exponential backoff with jitter and honoring `Retry-After` headers.
4. **Independent Execution**: Can be executed standalone via Python CLI, via Docker Compose, or scheduled/triggered through Airflow.

---

## Real-Time Streaming Pipeline

The real-time streaming pipeline connects to the Binance WebSocket streams (`wss://stream.binance.com:9443/ws`).

### Key Features
1. **Dynamic Multiplexing**: Connects to multi-stream endpoints (`<symbol>@kline_<interval>`), multiplexing updates for multiple trading pairs over a single connection.
2. **Automatic Reconnection**: Reconnects automatically on network drops, TCP resets, or unexpected server closures using progressive backoff (`2s`, `4s`, `8s`, up to `60s`).
3. **Watchdog Heartbeat Monitoring**: A background watchdog thread monitors `last_message_time`. If no frames arrive within `WATCHDOG_TIMEOUT_SECONDS` (default 60s), it initiates a clean reconnect.
4. **Duplicate & Out-of-Order Handling**: Normalized events carry canonical `event_time` and `open_time`. Downstream Spark Structured Streaming maintains watermarks to drop stale out-of-order events.

---

## Spark Processing Layer

Apache Spark 3.5 executes both batch and real-time streaming workloads.

### 1. Batch Processing (`spark/apps/historical_processing.py`)
- Consumes from `crypto_klines_historical`.
- Enforces the 20-field unified Spark schema (`CANONICAL_KLINE_SPARK_SCHEMA`).
- Deduplicates raw records by `(symbol, open_time)`.
- Writes Snappy-compressed Parquet files to HDFS partitioned by:
  `hdfs://namenode:9000/data/crypto/historical/klines/symbol=<SYMBOL>/kline_date=<YYYY-MM-DD>/`
- Performs idempotent merge to PostgreSQL DW `fact_klines_min` via relational anti-join (`LEFT ANTI JOIN`), preventing duplicate key errors.

### 2. Structured Streaming (`spark/apps/streaming_processing.py`)
- Consumes from `crypto_klines_streaming` with a 10-second micro-batch trigger.
- Enforces a 15-minute event-time watermark on `kline_open_time`.
- Dual-sink execution:
  - **HDFS Data Lake**: Writes **all** received events (provisional + closed) to:
    `hdfs://namenode:9000/data/crypto/streaming/klines/symbol=<SYMBOL>/kline_date=<YYYY-MM-DD>/`
  - **PostgreSQL Warehouse**: Filters strictly `is_closed == True` and upserts into `fact_klines_min` using an idempotent merge.

---

## Operational Guide & Runbook

> [!NOTE]
> All producer scripts run in **dry-run mode by default** to protect API rate limits and prevent accidental production writes. Use `--force-run` or pass `FORCE_RUN=true` to execute live ingestion.

### 1. Historical Ingestion via CLI

Execute historical ingestion directly on the host or inside the `producer` container:

```bash
# Ingest 30 days of 1-minute historical data for BTC and ETH (Dry-Run preview)
docker exec crypto-producer python /app/producer/historical/binance_rest_producer.py \
    --symbols BTCUSDT ETHUSDT \
    --interval 1m \
    --lookback-days 30 \
    --dry-run

# Live Ingestion: 7 days of 1-minute Klines for 5 major pairs
docker exec crypto-producer python /app/producer/historical/binance_rest_producer.py \
    --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
    --interval 1m \
    --lookback-days 7 \
    --force-run

# Custom timestamp window ingestion (start and end in milliseconds)
docker exec crypto-producer python /app/producer/historical/binance_rest_producer.py \
    --symbols BTCUSDT \
    --interval 1h \
    --start-time 1704067200000 \
    --end-time 1706745600000 \
    --force-run
```

#### CLI Parameter Reference

| Flag | Type | Default | Description |
|---|---|---|---|
| `--symbols` | Space-separated list | Top 10 pairs | Trading pairs to ingest (e.g. `BTCUSDT ETHUSDT`) |
| `--interval` | string | `1m` | Candle interval (`1m`, `5m`, `15m`, `1h`, `1d`) |
| `--lookback-days` | integer | `30` | Number of days of historical data to pull |
| `--start-time` | integer (ms) | Calculated | Explicit UTC start timestamp in milliseconds |
| `--end-time` | integer (ms) | `now` (ms) | Explicit UTC end timestamp in milliseconds |
| `--max-batches` | integer | `None` (all) | Limit number of 1,000-candle chunks (useful for testing) |
| `--dry-run` | boolean flag | Active | Validate parameters, query API, test schema, do NOT publish to Kafka |
| `--force-run` | boolean flag | `False` | Override dry-run mode and publish live records to Kafka |

---

### 2. Historical Ingestion via Airflow DAG

Airflow hosts the parameterizable `crypto_historical_ingestion_pipeline` DAG for scheduled or manual on-demand batch ingestion.

#### Trigger via Airflow CLI
```bash
# Trigger with custom JSON configuration
docker exec airflow-scheduler airflow dags trigger crypto_historical_ingestion_pipeline \
    --conf '{"symbols": ["BTCUSDT", "ETHUSDT"], "interval": "1m", "lookback_days": 14, "force_run": true}'
```

#### Trigger via Airflow Web UI
1. Navigate to [http://localhost:8082](http://localhost:8082).
2. Locate `crypto_historical_ingestion_pipeline`.
3. Click **Trigger DAG w/ config**.
4. Supply your JSON parameters:
```json
{
  "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
  "interval": "1m",
  "lookback_days": 7,
  "force_run": true
}
```
5. Click **Trigger**. The DAG will run ingestion and then trigger `crypto_historical_batch_pipeline` to load data into HDFS and PostgreSQL DW.

---

### 3. Real-Time Streaming via CLI

The streaming producer can be launched on-demand or run continuously as a container service:

```bash
# Preview live WebSocket streaming (Dry-Run: receives & validates, does NOT write to Kafka)
docker exec crypto-producer python /app/producer/streaming/binance_ws_producer.py \
    --symbols BTCUSDT ETHUSDT \
    --interval 1m \
    --dry-run

# Start live real-time ingestion to crypto_klines_streaming topic
docker exec -d crypto-producer python /app/producer/streaming/binance_ws_producer.py \
    --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
    --interval 1m \
    --force-run

# View streaming producer live output
docker logs --tail 50 -f crypto-producer
```

---

### 4. Safe Producer Shutdown

Both REST and WebSocket producers register signal handlers for `SIGINT` (Ctrl+C) and `SIGTERM`. When a shutdown signal is intercepted:
1. Active WebSocket connections close gracefully with WebSocket close frame `1000`.
2. Any pending messages in memory are flushed to Kafka via `producer.flush(timeout=10)`.
3. Kafka broker connections close safely, avoiding lost offsets or partial writes.

To safely stop background producers:
```bash
# Gracefully terminate the producer process
docker compose stop producer
```

---

## Verification & Sanity Checking

Use the following commands to verify end-to-end data pipeline operations:

### 1. Verify Unit Tests & Schema Compatibility
```bash
# Run unit test suite (18 tests covering schemas, normalization, and Spark pipelines)
docker exec -w /app crypto-producer python -m unittest discover -s /app/tests -p "test_*.py" -v
```

### 2. Verify Kafka Topics & Offsets
```bash
# Inspect topic partitions, message counts, and consumer lag
docker exec -w /app crypto-producer python /app/kafka/tools.py inspect-topics

# Browse live topic messages via Kafka UI
# Open in browser: http://localhost:8090
```

### 3. Verify Spark Structured Streaming
```bash
# View live streaming micro-batch commits and throughput
docker logs --tail 25 spark-streaming

# Check Spark Master active executors
# Open in browser: http://localhost:8080
```

### 4. Verify HDFS Data Lake Storage
```bash
# List historical HDFS Parquet partitions
docker exec namenode hdfs dfs -ls -R /data/crypto/historical/klines/

# List streaming HDFS Parquet partitions
docker exec namenode hdfs dfs -ls -R /data/crypto/streaming/klines/
```

### 5. Verify PostgreSQL Data Warehouse & Analytical Views
```bash
# Check record counts in fact_klines_min
docker exec postgres-dw psql -U crypto_admin -d crypto_dw -c \
    "SELECT symbol, COUNT(*) AS candle_count, MIN(open_time) AS earliest, MAX(open_time) AS latest FROM fact_klines_min GROUP BY symbol;"

# Verify the unique constraint is intact
docker exec postgres-dw psql -U crypto_admin -d crypto_dw -c \
    "SELECT conname, contype FROM pg_constraint WHERE conname = 'uq_fact_klines_min_symbol_time';"

# Query Market Overview analytical view (powers Superset)
docker exec postgres-dw psql -U crypto_admin -d crypto_dw -c \
    "SELECT * FROM vw_market_overview LIMIT 5;"
```

### 6. Verify Airflow DAGs
```bash
# Check for any DAG import errors
docker exec airflow-scheduler airflow dags list-import-errors

# Trigger data quality audit DAG
docker exec airflow-scheduler airflow dags trigger crypto_data_quality_audit
```

---

## Service Endpoints & Web Interfaces

| Service | Host Port / URL | Internal Network Address | Purpose | Default Credentials |
|---|---|---|---|---|
| **Apache Superset** | [http://localhost:8088](http://localhost:8088) | `superset:8088` | BI Dashboards & Visualizations | `admin` / `admin` |
| **Apache Airflow** | [http://localhost:8082](http://localhost:8082) | `airflow-webserver:8080` | DAG Orchestration & Data Quality Audits | `admin` / `admin` |
| **Kafka UI** | [http://localhost:8090](http://localhost:8090) | `kafka-ui:8080` | Topic Offsets, Message Browser & Lag | None |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | `spark-master:8080` (RPC `7077`) | Cluster Worker & Application Status | None |
| **Spark Streaming UI**| [http://localhost:4040](http://localhost:4040) | `spark-streaming:4040` | Structured Streaming Micro-batch Metrics | None |
| **JupyterLab** | [http://localhost:8888](http://localhost:8888) | `jupyter:8888` | Interactive PySpark Prototyping & EDA | None |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | `namenode:9000` (RPC) | Distributed Storage Lake File Browser | None |
| **PostgreSQL DW** | `localhost:5432` | `postgres-dw:5432` | Analytical Data Warehouse (`crypto_dw`) | `crypto_admin` / `crypto_pass` |
| **Apache Kafka** | `localhost:9094` (Host) | `kafka:9092` (Internal) | KRaft Event Broker | None |

---

## Project Structure

```
crypto-market-analytics/
├── airflow/
│   └── dags/
│       ├── dag_crypto_historical_ingestion.py  # On-demand historical ingestion DAG
│       ├── dag_crypto_historical_pipeline.py   # Scheduled batch Spark ingestion DAG
│       └── dag_crypto_data_quality.py          # Data quality & consistency audit DAG
├── data/                                       # Local temporary scratch data (gitignored)
├── docs/                                       # Technical documentation & data dictionaries
├── kafka/
│   ├── init-kafka.sh                           # Idempotent topic provisioning script
│   └── tools.py                                # Topic inspection & test utility
├── postgres-dw/
│   └── init.sql                                # DDL schema, tables, constraints, views
├── producer/
│   ├── config.py                               # Environment settings & symbol catalog
│   ├── kafka_client.py                         # Resilient Kafka producer wrapper
│   ├── schema.py                               # Canonical 15-field Kline schema & validators
│   ├── historical/
│   │   └── binance_rest_producer.py            # Historical REST API ingestion producer
│   └── streaming/
│       └── binance_ws_producer.py              # Real-time WebSocket streaming producer
├── spark/
│   └── apps/
│       ├── historical_processing.py            # Spark batch processing (HDFS & PostgreSQL)
│       ├── streaming_processing.py             # Spark Structured Streaming (HDFS & PostgreSQL)
│       ├── technical_indicators.py             # Feature engineering (SMA, EMA, RSI)
│       └── adhoc_queries.py                    # Analytical Spark SQL query suite
├── superset/                                   # Apache Superset initialization & dashboards
├── tests/
│   ├── test_kafka_layer.py                     # Kafka producer & schema unit tests
│   └── test_spark_processing.py                # Spark schema, deduplication & DW sink tests
├── docker-compose.yml                          # Multi-service infrastructure orchestration
├── .env.example                                # Template for environment variables
└── README.md                                   # Comprehensive project runbook & documentation
```

---

## Downstream Team Data Access

- **Machine Learning Team**:
  - HDFS Parquet Data Lake: `hdfs://namenode:9000/data/crypto/streaming/klines/` (partitioned by `symbol=<SYM>/kline_date=<YYYY-MM-DD>`).
  - Feature View: `public.vw_coin_analysis` in PostgreSQL DW (`crypto_dw`).
  - Prediction Sink: `public.fact_predictions` (seeded DDL ready for ML inferences).
- **Visualization Team**:
  - Apache Superset: `http://localhost:8088` (Default dashboard: `/superset/dashboard/crypto-market-analytics/`).
  - Pre-connected Database: `Crypto Data Warehouse` (`postgres-dw:5432/crypto_dw` internally via Docker network).
  - Pre-registered Datasets: `vw_market_overview`, `vw_coin_analysis`, `fact_klines_min`, `fact_klines_daily`, `fact_predictions`.

---

## Future Improvements

- Expanding target pairs beyond the initial 30 symbols.
- Adding advanced custom technical indicators (MACD, Bollinger Bands, Fibonacci levels).
- Implementing Great Expectations for data quality validation.
- Real-time price threshold alerting systems.
- Production deployment on cloud infrastructure (AWS / GCP / K8s).
