# System Architecture Overview

## Data Pipeline Architecture

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

### Architectural Principles
1. **Decoupled Ingestion**: Binance REST and WebSocket producers stream raw standardized Kline messages into Kafka.
2. **Centralized Streaming**: Kafka handles event buffering, topic partitioning, and message retention.
3. **Unified Engine**: Apache Spark handles both batch (historical) and structured streaming (real-time) data processing, transformation, validation, and feature engineering.
4. **Persistent Big Data Storage**: HDFS storing data in Parquet format serves as the long-term data lake.
5. **Serving & Analytics Layer**: PostgreSQL acts as the structured serving warehouse for downstream Superset dashboards and analytics.
