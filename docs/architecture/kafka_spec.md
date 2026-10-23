# Apache Kafka Messaging Layer Architecture & Operations

## 1. Overview and Architecture

This document specifies the Apache Kafka infrastructure, messaging architecture, canonical schemas, delivery semantics, and operational procedures for the **Real-Time Cryptocurrency Market Analytics** platform.

Kafka acts as the high-throughput, fault-tolerant event buffering and decoupling tier between external Binance market feeds (REST API & WebSocket) and the distributed processing engine (Apache Spark).

```
                      ┌────────────────────────┐
                      │ Binance REST API (1m)  │
                      └───────────┬────────────┘
                                  │ (Historical chunks, rate-limited)
                                  ▼
                      ┌────────────────────────┐
                      │ Historical Producer    │
                      └───────────┬────────────┘
                                  │ (JSON UTF-8, Key: Symbol)
                                  ▼
               ╔══════════════════════════════════════════════════╗
               ║ Topic: crypto_klines_historical                  ║
               ║ (Partitions: 3, Replication: 1, Retention: 7d)   ║
               ╚══════════════════╤═══════════════════════════════╝
                                  │
                                  ▼
                      ┌────────────────────────┐
                      │ Spark Batch Processing │
                      └────────────────────────┘

                      ┌────────────────────────┐
                      │ Binance Combined WS    │
                      └───────────┬────────────┘
                                  │ (@kline_1m, @aggTrade, @depth20, @ticker)
                                  ▼
                      ┌────────────────────────┐
                      │ Streaming Producer     │
                      └───────────┬────────────┘
                                  │ (JSON UTF-8, Key: Symbol)
                                  ▼
               ╔══════════════════════════════════════════════════╗
               ║ Topic: crypto_klines_streaming                   ║
               ║ (Partitions: 3, Replication: 1, Retention: 24h)  ║
               ╚══════════════════╤═══════════════════════════════╝
                                  │
                                  ▼
                      ┌────────────────────────────┐
                      │ Spark Structured Streaming │
                      └────────────────────────────┘
```

---

## 2. Broker Configuration & Deployment Mode

- **Broker Engine**: Apache Kafka version `3.9.2`.
- **Mode**: **KRaft (Kafka Raft Metadata Mode)**.
  - No ZooKeeper dependency.
  - Quorum voter: `1@kafka:9093`.
  - Node ID: `1`.
  - Cluster ID: `crypto-analytics-kraft-cluster-01`.
- **Network Listeners & Advertised Endpoints**:
  - `PLAINTEXT://kafka:9092` — For internal Docker container communication (`crypto-net`).
  - `CONTROLLER://kafka:9093` — For internal KRaft controller consensus.
  - `EXTERNAL://localhost:9094` — For host-based development, monitoring, and external diagnostic tools.
- **Persistence**:
  - Kafka storage volume: `kafka_data` mounted at `/var/lib/kafka/data`.
- **Web UI**:
  - `provectuslabs/kafka-ui:latest` mapped to host port `8090` (internal 8080) pointing to `kafka:9092`.

---

## 3. Topic Specifications & Lifecycle

All topics are provisioned and managed idempotently via `kafka/create_topics.sh`.

| Topic Name | Partitions | Replication | Retention Time (`retention.ms`) | Cleanup Policy | Intended Purpose |
|---|---|---|---|---|---|
| `crypto_klines_historical` | 3 | 1 | 7 Days (`604800000`) | `delete` | Batched historical Kline candles from Binance REST API for Spark batch backfills. |
| `crypto_klines_streaming` | 3 | 1 | 24 Hours (`86400000`) | `delete` | Live real-time Kline candles from Binance WebSocket for Spark Structured Streaming. |
| `market_klines` | 3 | 1 | 3 Days (`259200000`) | `delete` | General / aggregate Kline topic (maintained for backwards compatibility). |
| `market_trades` | 3 | 1 | 24 Hours (`86400000`) | `delete` | High-frequency individual / aggregated trades stream (`@aggTrade`). |
| `order_book_events` | 3 | 1 | 12 Hours (`43200000`) | `delete` | Depth snapshots / top 20 bid-ask levels (`@depth20`). |
| `ticker_events` | 3 | 1 | 24 Hours (`86400000`) | `delete` | 24-hour rolling price and volume statistics (`@ticker`). |
| `alerts_events` | 3 | 1 | 7 Days (`604800000`) | `delete` | System anomaly alerts, threshold breaches, and pipeline notifications. |

### Replication Factor & Partition Rationale:
- **Replication Factor = 1**: Matches the single-broker development and staging architecture. If scaled to a 3-broker cluster, replication factor must be adjusted to 3 with `min.insync.replicas=2`.
- **Partitions = 3**: Matches parallel processing cores on consumer nodes (e.g. Spark worker executors). Keyed partitioning guarantees that all records for a specific symbol (e.g., `BTCUSDT`) route to the same partition, guaranteeing chronological order per asset.

---

## 4. Canonical Message Contract (Kline Schema)

Every Kline record published to `crypto_klines_historical` and `crypto_klines_streaming` MUST adhere to the 12-field canonical schema defined in `producer/schema.py`. The raw Binance 12th element (`ignore`) is discarded upon ingestion.

### Fields Specification

| Field Name | Data Type | Units / Format | Nullable | Description |
|---|---|---|---|---|
| `open_time` | `int` (64-bit) | Epoch Milliseconds | No | Timestamp representing candlestick start time. |
| `symbol` | `str` | Uppercase (e.g. `BTCUSDT`) | No | Cryptocurrency trading pair symbol. Partition key. |
| `open` | `float` (64-bit) | Quote asset currency (e.g. USDT) | No | Opening price of the interval. |
| `high` | `float` (64-bit) | Quote asset currency (e.g. USDT) | No | Highest traded price during the interval. |
| `low` | `float` (64-bit) | Quote asset currency (e.g. USDT) | No | Lowest traded price during the interval. |
| `close` | `float` (64-bit) | Quote asset currency (e.g. USDT) | No | Closing price of the interval. |
| `volume` | `float` (64-bit) | Base asset quantity (e.g. BTC) | No | Total traded volume of the base asset. |
| `close_time` | `int` (64-bit) | Epoch Milliseconds | No | Timestamp representing candlestick close time. |
| `quote_asset_volume` | `float` (64-bit) | Quote asset currency (e.g. USDT) | No | Total trading volume in quote asset. |
| `number_of_trades` | `int` (64-bit) | Integer count | No | Count of individual trade executions during candle. |
| `taker_buy_base_volume` | `float` (64-bit) | Base asset quantity | No | Base asset volume bought by takers (buy volume). |
| `taker_buy_quote_volume` | `float` (64-bit) | Quote asset currency | No | Quote asset volume bought by takers. |
| `data_source` | `str` | Enum: `"historical"` \| `"realtime"` | No | Ingestion pipeline lineage identifier. |
| `ingested_at` | `str` | ISO-8601 UTC timestamp | No | UTC timestamp when record was parsed and enqueued. |

### Validation Invariants:
1. `close_time >= open_time`
2. `high >= low`, `high >= open`, `high >= close`
3. `low <= open`, `low <= close`
4. `volume >= 0`, `quote_asset_volume >= 0`, `number_of_trades >= 0`

Records violating any invariant are rejected before hitting the Kafka producer queue.

---

## 5. Producer Configuration & Delivery Semantics

Producers use the centralized client in `producer/kafka_client.py` configured with the following settings:

- **Acknowledgments (`acks`)**: `"all"` (wait for full in-sync replica acknowledgment).
- **Retries**: `retries=5` with exponential backoff (`retry_backoff_ms=500`).
- **Batching & Latency**: `linger_ms=100`, `batch_size=16384` bytes.
- **Serialization**:
  - Key: `symbol.encode("utf-8")`
  - Value: `json.dumps(dict).encode("utf-8")`
- **Delivery Guarantees**:
  - **At-Least-Once Delivery**: Messages are guaranteed not to be lost. Transient network retries may occasionally produce duplicate records. Downstream Spark consumers must deduplicate using the composite primary key `(symbol, open_time)`.
- **Graceful Shutdown**:
  - On `SIGTERM` / `SIGINT`, producers invoke `producer.flush()` and `producer.close()` to ensure all buffered messages in the accumulator are published.

---

## 6. Operational & Diagnostic Commands

### 1. Provision / Refresh Topics
```bash
# Via Docker Compose
docker compose run --rm kafka-topics-init

# Or directly on host if connected
bash kafka/create_topics.sh
```

### 2. Inspect Topics, Offsets, and Configurations
```bash
docker run --rm --network crypto-net -v $(pwd):/app -w /app crypto-producer:local \
    python kafka/tools.py --bootstrap kafka:9092 inspect-topics --topics crypto_klines_historical crypto_klines_streaming
```

### 3. Non-Destructive Consumer Smoke Test
```bash
# Read from crypto_klines_historical without resetting production offsets
docker run --rm --network crypto-net -v $(pwd):/app -w /app crypto-producer:local \
    python kafka/tools.py --bootstrap kafka:9092 consume-test --topic crypto_klines_historical --from-beginning --max 5
```

### 4. Publish a Validated Test Message
```bash
docker run --rm --network crypto-net -v $(pwd):/app -w /app crypto-producer:local \
    python kafka/tools.py --bootstrap kafka:9092 publish-test --topic crypto_klines_streaming --symbol SOLUSDT --source realtime
```

### 5. Run Automated Unit Tests
```bash
docker run --rm -v $(pwd):/app -w /app crypto-producer:local \
    python -m unittest discover -s tests -p "test_*.py"
```

---

## 7. Known Limitations & Downstream Spark Interface

1. **Replication Factor**: Running on a single-node broker (`replication-factor=1`). When deploying to multi-broker production, increase cluster replication and set `min.insync.replicas=2`.
2. **Spark Structured Streaming Offsets**: Spark jobs must maintain external checkpointing (`checkpointLocation`) in HDFS or persistent volumes. Never reset Kafka offsets manually while Spark Structured Streaming queries are active.
3. **Deduplication Contract**: Spark batch and streaming jobs MUST enforce deduplication on `(symbol, open_time)` before writing into HDFS Parquet files or PostgreSQL `fact_klines_min`.
