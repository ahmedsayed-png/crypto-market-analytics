"""
==============================================================================
producer/config.py
Centralised runtime configuration for the Binance Kafka Producer.

All values are read from environment variables so the same Docker image can
be used in every environment (local, staging, production) without rebuilding.
==============================================================================
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


# ---------------------------------------------------------------------------
# Kafka Configuration
# ---------------------------------------------------------------------------
@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    # Topic names — primary dedicated Kline topics + proposal supplementary topics
    topic_historical_klines: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_HISTORICAL_KLINES", "crypto_klines_historical")
    )
    topic_streaming_klines: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_STREAMING_KLINES", "crypto_klines_streaming")
    )
    topic_trades: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_TRADES", "market_trades")
    )
    topic_klines: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_KLINES", "market_klines")
    )
    topic_order_book: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_ORDER_BOOK", "order_book_events")
    )
    topic_ticker: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_TICKER", "ticker_events")
    )
    topic_alerts: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC_ALERTS", "alerts_events")
    )

    # Producer delivery settings
    acks: str = "all"               # Wait for full ISR acknowledgement
    retries: int = 5
    retry_backoff_ms: int = 500
    linger_ms: int = 100            # Micro-batching window (ms)
    batch_size: int = 16384         # bytes


# ---------------------------------------------------------------------------
# Binance API Configuration
# ---------------------------------------------------------------------------
@dataclass
class BinanceConfig:
    api_key: str = field(
        default_factory=lambda: os.environ.get("BINANCE_API_KEY", "")
    )
    api_secret: str = field(
        default_factory=lambda: os.environ.get("BINANCE_API_SECRET", "")
    )
    base_url: str = field(
        default_factory=lambda: os.environ.get("BINANCE_BASE_URL", "https://api.binance.com")
    )
    ws_base_url: str = field(
        default_factory=lambda: os.environ.get("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
    )

    # REST API endpoints
    klines_endpoint: str = "/api/v3/klines"
    trades_endpoint: str = "/api/v3/trades"
    depth_endpoint: str = "/api/v3/depth"
    ticker_endpoint: str = "/api/v3/ticker/24hr"

    # Rate limiting — Binance allows 1200 weight/minute on REST
    request_weight_limit: int = 1200
    rate_limit_pause_seconds: float = 0.1     # pause between REST calls


# ---------------------------------------------------------------------------
# Historical Ingestion Configuration
# ---------------------------------------------------------------------------
@dataclass
class HistoricalConfig:
    # Target trading pairs (subset; extend list freely or override via HISTORICAL_SYMBOLS env var)
    symbols: List[str] = field(default_factory=lambda: (
        [s.strip().upper() for s in os.environ.get("HISTORICAL_SYMBOLS").split(",") if s.strip()]
        if os.environ.get("HISTORICAL_SYMBOLS")
        else [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
            "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
            "LTCUSDT", "TRXUSDT", "SHIBUSDT", "ATOMUSDT", "UNIUSDT",
            "ETCUSDT", "FILUSDT", "APTUSDT", "NEARUSDT", "MATICUSDT",
        ]
    ))
    # Kline interval — 1m gives the finest granularity
    kline_interval: str = field(
        default_factory=lambda: os.environ.get("HISTORICAL_KLINE_INTERVAL", "1m")
    )
    # How many klines per REST request (Binance max = 1000)
    klines_per_request: int = field(
        default_factory=lambda: int(os.environ.get("HISTORICAL_KLINES_PER_REQUEST", "1000"))
    )
    # Lookback window expressed as days of history (override via HISTORICAL_LOOKBACK_DAYS)
    lookback_days: int = field(
        default_factory=lambda: int(os.environ.get("HISTORICAL_LOOKBACK_DAYS", "365"))
    )
    # Optional max batches limit per symbol (useful for safe small test runs)
    max_batches: Optional[int] = field(
        default_factory=lambda: int(os.environ["HISTORICAL_MAX_BATCHES"]) if os.environ.get("HISTORICAL_MAX_BATCHES") else None
    )
    # data_source tag attached to every produced message
    data_source: str = "historical"


# ---------------------------------------------------------------------------
# Streaming (Real-Time) Configuration
# ---------------------------------------------------------------------------
@dataclass
class StreamingConfig:
    # Same target pairs as historical
    symbols: List[str] = field(default_factory=lambda: [
        "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt",
        "adausdt", "dogeusdt", "avaxusdt", "dotusdt", "linkusdt",
        "ltcusdt", "trxusdt", "shibusdt", "atomusdt", "uniusdt",
        "etcusdt", "filusdt", "aptusdt", "nearusdt", "maticusdt",
    ])
    # Stream types to subscribe to
    kline_interval: str = "1m"         # for @kline streams
    depth_levels: int = 20             # for @depth20 partial book
    # WebSocket reconnect settings
    max_reconnect_attempts: int = 10
    reconnect_delay_seconds: float = 5.0
    ping_interval: int = 30            # seconds
    # data_source tag attached to every produced message
    data_source: str = "realtime"


# ---------------------------------------------------------------------------
# Aggregated ProducerConfig — single import point for all modules
# ---------------------------------------------------------------------------
@dataclass
class ProducerConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    historical: HistoricalConfig = field(default_factory=HistoricalConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)


# Convenience singleton — import this in producer scripts
CONFIG = ProducerConfig()
