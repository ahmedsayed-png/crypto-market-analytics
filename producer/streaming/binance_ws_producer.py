"""
==============================================================================
producer/streaming/binance_ws_producer.py
Real-Time Streaming — Binance WebSocket -> Kafka (crypto_klines_streaming)

Flow
----
  Binance Combined Stream WebSocket:
    wss://stream.binance.com:9443/stream?streams=
        <symbol>@aggTrade / <symbol>@kline_<interval> / <symbol>@depth20 / <symbol>@ticker
      ↓
  Normalise & enrich with Unified Event Schema (source="realtime", is_closed=x)
      ↓
  Kafka topics:
      crypto_klines_streaming — real-time kline updates (both provisional and closed)
      market_trades           — aggregated trade stream (@aggTrade)
      order_book_events       — partial order book (20 levels) (@depth20)
      ticker_events           — 24-hour rolling ticker (@ticker)

Usage
-----
  python producer/streaming/binance_ws_producer.py

The script handles:
  - Connection interruptions & exponential backoff reconnection.
  - Heartbeat / ping-pong monitoring.
  - Distinction between provisional (in-progress) and finalized (closed) candles.
  - Composite partition keys: <symbol>_<interval>_<open_time>.
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

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import websocket
except ImportError:
    websocket = None

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:
    def retry(*args, **kwargs):
        def decorator(f):
            return f
        return decorator
    def stop_after_attempt(*args, **kwargs): return None
    def wait_exponential(*args, **kwargs): return None

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import CONFIG
from kafka_client import get_kafka_producer
from schema import generate_kline_key, normalise_ws_kline, validate_kline_dict

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
    logger.info("Shutdown signal received (%s). Closing WebSocket cleanly...", sig)
    _shutdown_event.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _base_metadata(symbol: str) -> Dict:
    return {
        "symbol": symbol.strip().upper(),
        "source": CONFIG.streaming.data_source,  # "realtime"
        "data_source": CONFIG.streaming.data_source,
        "received_at": datetime.now(tz=timezone.utc).isoformat(),
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def normalise_agg_trade(data: Dict) -> Optional[Dict]:
    """@aggTrade stream -> market_trades topic."""
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


def normalise_kline(data: Dict, default_interval: str = "1m") -> Optional[Dict]:
    """@kline_<interval> stream -> Unified Event Schema format."""
    return normalise_ws_kline(data, default_interval=default_interval)


def normalise_depth(data: Dict, symbol: str) -> Optional[Dict]:
    """@depth<levels> stream -> order_book_events topic."""
    return {
        **_base_metadata(symbol),
        "last_update_id": data.get("lastUpdateId"),
        "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
        "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
        "levels": len(data.get("bids", [])),
    }


def normalise_ticker(data: Dict) -> Optional[Dict]:
    """@ticker stream -> ticker_events topic."""
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

    def __init__(self, producer) -> None:
        self._producer = producer
        self.kline_events_count = 0
        self.closed_klines_count = 0

    def route(self, raw_message: str) -> None:
        """Parse raw_message and dispatch to the appropriate handler."""
        try:
            envelope = json.loads(raw_message)
            stream_name: str = envelope.get("stream", "")
            data: Dict = envelope.get("data", {})

            if "kline" in stream_name:
                self._handle_kline(data)
            elif "aggTrade" in stream_name:
                self._handle_agg_trade(data)
            elif "depth" in stream_name:
                symbol = stream_name.split("@")[0].upper()
                self._handle_depth(data, symbol)
            elif "ticker" in stream_name:
                self._handle_ticker(data)
            else:
                logger.debug("Unknown stream type: %s", stream_name)

        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Failed to parse stream message: %s", exc)

    # ------------------------------------------------------------------

    def _handle_kline(self, data: Dict) -> None:
        msg = normalise_kline(data, default_interval=CONFIG.streaming.kline_interval)
        if msg:
            self.kline_events_count += 1
            if msg.get("is_closed"):
                self.closed_klines_count += 1
                logger.info(
                    "[%s] Closed candle: open_time=%d close=%.2f volume=%.4f (closed total: %d)",
                    msg["symbol"], msg["open_time"], msg["close"], msg["volume"], self.closed_klines_count,
                )
            else:
                logger.debug(
                    "[%s] Provisional update: open_time=%d close=%.2f",
                    msg["symbol"], msg["open_time"], msg["close"],
                )

            key = generate_kline_key(msg["symbol"], msg.get("interval", "1m"), msg["open_time"])
            self._producer.send_message(
                topic=CONFIG.kafka.topic_streaming_klines,
                payload=msg,
                key=key,
                validate=True,
            )

    def _handle_agg_trade(self, data: Dict) -> None:
        msg = normalise_agg_trade(data)
        if msg:
            self._producer.send_message(
                topic=CONFIG.kafka.topic_trades,
                payload=msg,
                key=msg.get("symbol"),
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

def build_stream_url(symbols: list, interval: str = "1m", depth: int = 20) -> str:
    """
    Build a Binance combined stream URL subscribing to:
      @kline_<interval>, @aggTrade, @depth<depth>, @ticker
    for every symbol in *symbols*.
    """
    streams = []
    for sym in symbols:
        sym_lower = sym.strip().lower()
        streams.append(f"{sym_lower}@kline_{interval}")
        streams.append(f"{sym_lower}@aggTrade")
        streams.append(f"{sym_lower}@depth{depth}")
        streams.append(f"{sym_lower}@ticker")

    combined = "/".join(streams)
    base_ws = CONFIG.binance.ws_base_url.rstrip("/ws")
    return f"{base_ws}/stream?streams={combined}"


class BinanceWebSocketProducer:
    """
    Manages a Binance combined-stream WebSocket connection.
    Auto-reconnects on disconnect with exponential back-off via tenacity.
    """

    def __init__(self, symbols: Optional[list] = None, interval: Optional[str] = None) -> None:
        self._producer = get_kafka_producer()
        self._router = StreamRouter(self._producer)
        self._symbols = symbols or CONFIG.streaming.symbols
        self._interval = interval or CONFIG.streaming.kline_interval
        self._url = build_stream_url(self._symbols, self._interval, CONFIG.streaming.depth_levels)
        self._ws: Optional[websocket.WebSocketApp] = None
        self.last_message_time: float = time.time()
        self.connected_at: Optional[float] = None

    # ------------------------------------------------------------------

    def _on_message(self, ws: Any, raw: str) -> None:
        self.last_message_time = time.time()
        self._router.route(raw)

    def _on_error(self, ws: Any, error: Exception) -> None:
        logger.error("WebSocket connection error: %s", error)

    def _on_close(self, ws: Any, close_status_code: Any, close_msg: Any) -> None:
        logger.warning(
            "WebSocket closed — status=%s msg=%s (active duration: %.1fs)",
            close_status_code, close_msg,
            (time.time() - self.connected_at) if self.connected_at else 0.0,
        )

    def _on_open(self, ws: Any) -> None:
        self.connected_at = time.time()
        self.last_message_time = time.time()
        logger.info(
            "WebSocket successfully connected — streaming %d symbols at %s interval",
            len(self._symbols), self._interval,
        )

    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(CONFIG.streaming.max_reconnect_attempts),
        wait=wait_exponential(multiplier=1, min=2, max=30),
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
            raise ConnectionError("WebSocket disconnected — triggering reconnect with exponential backoff")

    def run(self) -> None:
        """Start the WebSocket consumer loop (blocking until shutdown)."""
        logger.info(
            "Starting real-time streaming producer — %d symbols (target topic: %s)",
            len(self._symbols),
            CONFIG.kafka.topic_streaming_klines,
        )
        try:
            self._connect_and_run()
        except Exception as exc:
            logger.critical("WebSocket permanently failed or interrupted: %s", exc)
        finally:
            self._producer.flush()
            self._producer.close()
            logger.info(
                "Streaming producer shut down cleanly (processed %d klines, %d closed).",
                self._router.kline_events_count,
                self._router.closed_klines_count,
            )

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
            "========================================================\n"
            "  DRY RUN MODE — BINANCE_API_KEY is not set.\n"
            "  To test public market WebSocket streams without an API key, set FORCE_RUN=true.\n"
            "  Set BINANCE_API_KEY in .env for authenticated requests.\n"
            "========================================================"
        )
    else:
        ws_producer = BinanceWebSocketProducer()
        ws_producer.run()
