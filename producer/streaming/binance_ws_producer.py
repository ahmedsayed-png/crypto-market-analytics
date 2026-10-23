"""
==============================================================================
producer/streaming/binance_ws_producer.py
Real-Time Streaming — Binance WebSocket → Kafka

Flow
----
  Binance Combined Stream WebSocket:
    wss://stream.binance.com:9443/stream?streams=
        <symbol>@aggTrade / <symbol>@kline_1m / <symbol>@depth20 / <symbol>@ticker
      ↓
  Normalise & enrich with metadata (data_source="realtime")
      ↓
  Kafka topics:
      market_trades        — aggregated trade stream (@aggTrade)
      market_klines        — real-time kline updates  (@kline_1m)
      order_book_events    — partial order book (20 levels) (@depth20)
      ticker_events        — 24-hour rolling ticker  (@ticker)

Usage
-----
  python producer/streaming/binance_ws_producer.py

The script will be activated when the team is ready to stream real-time data.
No WebSocket connections are opened simply by importing this module.
==============================================================================
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import websocket
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import CONFIG
from kafka_client import get_kafka_producer
from schema import normalise_ws_kline, validate_kline_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("streaming_producer")


# ---------------------------------------------------------------------------
# Graceful shutdown flag
# ---------------------------------------------------------------------------
_shutdown_event = threading.Event()


def _handle_signal(sig, frame) -> None:
    logger.info("Shutdown signal received (%s). Closing WebSocket...", sig)
    _shutdown_event.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Normalisation helpers  (data_source = "realtime")
# ---------------------------------------------------------------------------

def _base_metadata(symbol: str) -> Dict:
    return {
        "symbol": symbol.upper(),
        "data_source": CONFIG.streaming.data_source,   # "realtime"
        "received_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def normalise_agg_trade(data: Dict) -> Optional[Dict]:
    """@aggTrade stream → market_trades topic."""
    symbol = data.get("s", "")
    return {
        **_base_metadata(symbol),
        "aggregate_trade_id": data.get("a"),
        "price": float(data.get("p", 0)),
        "quantity": float(data.get("q", 0)),
        "first_trade_id": data.get("f"),
        "last_trade_id": data.get("l"),
        "trade_time": data.get("T"),
        "is_buyer_maker": data.get("m"),
    }


def normalise_kline(data: Dict) -> Optional[Dict]:
    """@kline_<interval> stream → canonical crypto_klines_streaming format."""
    return normalise_ws_kline(data)


def normalise_depth(data: Dict, symbol: str) -> Optional[Dict]:
    """@depth<levels> stream → order_book_events topic."""
    return {
        **_base_metadata(symbol),
        "last_update_id": data.get("lastUpdateId"),
        "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
        "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
        "levels": len(data.get("bids", [])),
    }


def normalise_ticker(data: Dict) -> Optional[Dict]:
    """@ticker stream → ticker_events topic."""
    symbol = data.get("s", "")
    return {
        **_base_metadata(symbol),
        "price_change": float(data.get("p", 0)),
        "price_change_pct": float(data.get("P", 0)),
        "weighted_avg_price": float(data.get("w", 0)),
        "open_price": float(data.get("o", 0)),
        "high_price": float(data.get("h", 0)),
        "low_price": float(data.get("l", 0)),
        "last_price": float(data.get("c", 0)),
        "volume": float(data.get("v", 0)),
        "quote_volume": float(data.get("q", 0)),
        "open_time": data.get("O"),
        "close_time": data.get("C"),
        "count": int(data.get("n", 0)),
    }


# ---------------------------------------------------------------------------
# Stream router — dispatches each incoming frame to the correct normaliser
# and Kafka topic.
# ---------------------------------------------------------------------------

class StreamRouter:
    """
    Routes incoming WebSocket frames to the correct Kafka topic
    based on the stream name embedded in the combined-stream payload.
    """

    _STREAM_HANDLERS: Dict[str, Callable] = {}

    def __init__(self, producer) -> None:
        self._producer = producer

    def route(self, raw_message: str) -> None:
        """Parse *raw_message* and dispatch to the appropriate handler."""
        try:
            envelope = json.loads(raw_message)
            stream_name: str = envelope.get("stream", "")
            data: Dict = envelope.get("data", {})
            event_type: str = data.get("e", "")

            if "aggTrade" in stream_name:
                self._handle_agg_trade(data)
            elif "kline" in stream_name:
                self._handle_kline(data)
            elif "depth" in stream_name:
                # Extract symbol from stream name: "btcusdt@depth20" → "BTCUSDT"
                symbol = stream_name.split("@")[0].upper()
                self._handle_depth(data, symbol)
            elif "ticker" in stream_name:
                self._handle_ticker(data)
            else:
                logger.debug("Unknown stream: %s", stream_name)

        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Failed to parse stream message: %s", exc)

    # ------------------------------------------------------------------

    def _handle_agg_trade(self, data: Dict) -> None:
        msg = normalise_agg_trade(data)
        if msg:
            self._producer.send_message(
                topic=CONFIG.kafka.topic_trades,
                payload=msg,
                key=msg.get("symbol"),
            )

    def _handle_kline(self, data: Dict) -> None:
        msg = normalise_kline(data)
        if msg:
            self._producer.send_message(
                topic=CONFIG.kafka.topic_streaming_klines,
                payload=msg,
                key=msg.get("symbol"),
                validate=True,
            )

    def _handle_depth(self, data: Dict, symbol: str) -> None:
        msg = normalise_depth(data, symbol)
        if msg:
            self._producer.send_message(
                topic=CONFIG.kafka.topic_order_book,
                payload=msg,
                key=symbol,
            )

    def _handle_ticker(self, data: Dict) -> None:
        msg = normalise_ticker(data)
        if msg:
            self._producer.send_message(
                topic=CONFIG.kafka.topic_ticker,
                payload=msg,
                key=msg.get("symbol"),
            )


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------

def _build_stream_url(symbols: list) -> str:
    """
    Build a Binance combined stream URL subscribing to:
      @aggTrade, @kline_1m, @depth20, @ticker
    for every symbol in *symbols*.
    """
    streams = []
    interval = CONFIG.streaming.kline_interval
    depth = CONFIG.streaming.depth_levels

    for sym in symbols:
        streams.append(f"{sym}@aggTrade")
        streams.append(f"{sym}@kline_{interval}")
        streams.append(f"{sym}@depth{depth}")
        streams.append(f"{sym}@ticker")

    combined = "/".join(streams)
    return f"{CONFIG.binance.ws_base_url.rstrip('/ws')}/stream?streams={combined}"


class BinanceWebSocketProducer:
    """
    Manages a Binance combined-stream WebSocket connection.
    Auto-reconnects on disconnect with exponential back-off via tenacity.
    """

    def __init__(self) -> None:
        self._producer = get_kafka_producer()
        self._router = StreamRouter(self._producer)
        self._url = _build_stream_url(CONFIG.streaming.symbols)
        self._ws: Optional[websocket.WebSocketApp] = None

    # ------------------------------------------------------------------

    def _on_message(self, ws: Any, raw: str) -> None:
        self._router.route(raw)

    def _on_error(self, ws: Any, error: Exception) -> None:
        logger.error("WebSocket error: %s", error)

    def _on_close(self, ws: Any, close_status_code: Any, close_msg: Any) -> None:
        logger.warning(
            "WebSocket closed — status=%s msg=%s", close_status_code, close_msg
        )

    def _on_open(self, ws: Any) -> None:
        logger.info(
            "WebSocket connected — streaming %d symbols × 4 stream types",
            len(CONFIG.streaming.symbols),
        )

    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(CONFIG.streaming.max_reconnect_attempts),
        wait=wait_fixed(CONFIG.streaming.reconnect_delay_seconds),
        reraise=True,
    )
    def _connect_and_run(self) -> None:
        if _shutdown_event.is_set():
            return

        self._ws = websocket.WebSocketApp(
            self._url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(
            ping_interval=CONFIG.streaming.ping_interval,
            ping_timeout=10,
        )
        if not _shutdown_event.is_set():
            raise ConnectionError("WebSocket disconnected — triggering reconnect")

    def run(self) -> None:
        """Start the WebSocket consumer loop (blocking until shutdown)."""
        logger.info(
            "Starting real-time streaming producer — %d symbols",
            len(CONFIG.streaming.symbols),
        )
        try:
            self._connect_and_run()
        except Exception as exc:
            logger.critical("WebSocket permanently failed: %s", exc)
        finally:
            self._producer.flush()
            self._producer.close()
            logger.info("Streaming producer shut down cleanly.")

    def stop(self) -> None:
        """Signal graceful shutdown."""
        _shutdown_event.set()
        if self._ws:
            self._ws.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    force_run = os.environ.get("FORCE_RUN", "false").lower() in ("true", "1", "yes")
    has_custom_key = CONFIG.binance.api_key and CONFIG.binance.api_key != "your_binance_api_key_here"

    if not has_custom_key and not force_run:
        logger.warning(
            "DRY RUN MODE — BINANCE_API_KEY is not set. "
            "To test public market WebSocket streams without an API key, set FORCE_RUN=true. "
            "Set BINANCE_API_KEY in .env for authenticated requests."
        )
    else:
        ws_producer = BinanceWebSocketProducer()
        ws_producer.run()
