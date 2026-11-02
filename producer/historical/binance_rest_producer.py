"""
==============================================================================
producer/historical/binance_rest_producer.py
Historical Ingestion — Binance REST API -> Kafka (crypto_klines_historical)

Flow
----
  Binance REST API (/api/v3/klines)
      ↓  paginated requests per symbol per time window
  Normalise & validate with Unified Event Schema (source="historical", is_closed=True)
      ↓
  Kafka topics:
      crypto_klines_historical — primary target for historical Kline candles
      market_trades            — individual raw trade records (auxiliary)
      order_book_events        — depth snapshot (top N levels) (auxiliary)
      ticker_events            — 24-hour rolling ticker stats (auxiliary)

Usage
-----
  # Standalone invocation:
  python producer/historical/binance_rest_producer.py --symbols BTCUSDT,ETHUSDT --interval 1m --lookback-days 30

The script is safe:
- When DRY_RUN=true (or when BINANCE_API_KEY is unset and FORCE_RUN=false), no external
  requests are sent unless explicitly instructed.
==============================================================================
"""

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import sys
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import requests
except ImportError:
    requests = None

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:
    # Fallback dummy retry decorator if tenacity is not installed in local environment
    def retry(*args, **kwargs):
        def decorator(f):
            return f
        return decorator
    def stop_after_attempt(*args, **kwargs): return None
    def wait_exponential(*args, **kwargs): return None

# Add project root and producer root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import CONFIG
from kafka_client import get_kafka_producer
from schema import generate_kline_key, normalise_rest_kline, validate_kline_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("historical_producer")

# Kline interval string to milliseconds mapping
INTERVAL_MS_MAP: Dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}


def parse_timestamp_ms(val: Optional[Any]) -> Optional[int]:
    """Parse integer ms or ISO-8601 string to UTC Epoch ms."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        if val.isdigit():
            return int(val)
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            logger.warning("Could not parse timestamp string '%s', ignoring.", val)
            return None
    return None


# ---------------------------------------------------------------------------
# REST Client helper
# ---------------------------------------------------------------------------

class BinanceRestClient:
    """
    Wrapper around the Binance public REST API.
    Rate-limit-aware: handles 429/418 with exponential backoff and Retry-After.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        headers = {"Content-Type": "application/json"}
        if CONFIG.binance.api_key and CONFIG.binance.api_key != "your_binance_api_key_here":
            headers["X-MBX-APIKEY"] = CONFIG.binance.api_key
        self._session.headers.update(headers)
        self._base_url = CONFIG.binance.base_url
        self._pause = CONFIG.binance.rate_limit_pause_seconds

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _get(self, endpoint: str, params: Dict[str, Any]) -> Any:
        url = f"{self._base_url}{endpoint}"
        response = self._session.get(url, params=params, timeout=15)
        
        # Check rate-limit response codes
        if response.status_code in (418, 429):
            retry_after = int(response.headers.get("Retry-After", "10"))
            logger.warning(
                "Binance rate limit triggered (HTTP %d). Sleeping for %d seconds...",
                response.status_code,
                retry_after,
            )
            time.sleep(retry_after)
            response.raise_for_status()

        response.raise_for_status()
        time.sleep(self._pause)
        return response.json()

    # ------------------------------------------------------------------
    # Public REST endpoints
    # ------------------------------------------------------------------

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> List[List]:
        """Fetch kline (candlestick) records for a single time window."""
        return self._get(
            CONFIG.binance.klines_endpoint,
            {
                "symbol": symbol.strip().upper(),
                "interval": interval.strip(),
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": limit,
            },
        )

    def get_recent_trades(self, symbol: str, limit: int = 1000) -> List[Dict]:
        """Fetch recent public trades."""
        return self._get(
            CONFIG.binance.trades_endpoint,
            {"symbol": symbol.strip().upper(), "limit": limit},
        )

    def get_order_book(self, symbol: str, depth: int = 20) -> Dict:
        """Fetch order book snapshot (top N levels)."""
        return self._get(
            CONFIG.binance.depth_endpoint,
            {"symbol": symbol.strip().upper(), "limit": depth},
        )

    def get_ticker_24h(self, symbol: str) -> Dict:
        """Fetch 24-hour rolling ticker statistics."""
        return self._get(
            CONFIG.binance.ticker_endpoint,
            {"symbol": symbol.strip().upper()},
        )


# ---------------------------------------------------------------------------
# Pagination generator
# ---------------------------------------------------------------------------

def kline_time_windows(
    lookback_days: int = 365,
    klines_per_request: int = 1000,
    interval: str = "1m",
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
) -> Generator[Tuple[int, int], None, None]:
    """
    Yield (start_ms, end_ms) pairs covering the ingestion window
    broken into chunks of `klines_per_request` candles.
    """
    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)
    chunk_duration_ms = klines_per_request * interval_ms

    if end_time_ms is not None:
        end_ms_total = end_time_ms
    else:
        end_ms_total = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    if start_time_ms is not None:
        start_ms = start_time_ms
    else:
        start_ms = end_ms_total - (lookback_days * 86_400_000)

    if start_ms >= end_ms_total:
        logger.warning("Start time (%d) >= End time (%d). No windows to yield.", start_ms, end_ms_total)
        return

    while start_ms < end_ms_total:
        window_end_ms = min(start_ms + chunk_duration_ms - 1, end_ms_total)
        yield start_ms, window_end_ms
        start_ms = window_end_ms + 1


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _base_metadata(symbol: str, data_source: str = "historical") -> Dict:
    return {
        "symbol": symbol.strip().upper(),
        "source": data_source,
        "data_source": data_source,
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def normalise_kline(raw: List, symbol: str, interval: str = "1m") -> Dict:
    return normalise_rest_kline(raw, symbol, interval=interval)


def normalise_trade(raw: Dict, symbol: str) -> Dict:
    return {
        **_base_metadata(symbol),
        "trade_id": raw["id"],
        "price": float(raw["price"]),
        "quantity": float(raw["qty"]),
        "quote_quantity": float(raw["quoteQty"]),
        "trade_time": raw["time"],
        "is_buyer_maker": raw["isBuyerMaker"],
    }


def normalise_order_book(raw: Dict, symbol: str) -> Dict:
    return {
        **_base_metadata(symbol),
        "last_update_id": raw["lastUpdateId"],
        "bids": [[float(p), float(q)] for p, q in raw["bids"]],
        "asks": [[float(p), float(q)] for p, q in raw["asks"]],
        "levels": len(raw["bids"]),
    }


def normalise_ticker(raw: Dict, symbol: str) -> Dict:
    return {
        **_base_metadata(symbol),
        "price_change": float(raw["priceChange"]),
        "price_change_pct": float(raw["priceChangePercent"]),
        "weighted_avg_price": float(raw["weightedAvgPrice"]),
        "open_price": float(raw["openPrice"]),
        "high_price": float(raw["highPrice"]),
        "low_price": float(raw["lowPrice"]),
        "last_price": float(raw["lastPrice"]),
        "volume": float(raw["volume"]),
        "quote_volume": float(raw["quoteVolume"]),
        "open_time": raw["openTime"],
        "close_time": raw["closeTime"],
        "count": int(raw["count"]),
    }


# ---------------------------------------------------------------------------
# Main Ingestion Functions
# ---------------------------------------------------------------------------

def ingest_klines(
    client: BinanceRestClient,
    producer,
    symbol: str,
    interval: Optional[str] = None,
    lookback_days: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    max_batches: Optional[int] = None,
) -> int:
    """
    Paginate through historical Kline window and produce validated Unified Event Schema
    records to `crypto_klines_historical`.
    """
    cfg = CONFIG.historical
    interval = interval or cfg.kline_interval
    lookback_days = lookback_days if lookback_days is not None else cfg.lookback_days
    start_ms = start_time if start_time is not None else cfg.start_time
    end_ms = end_time if end_time is not None else cfg.end_time
    max_batches = max_batches if max_batches is not None else cfg.max_batches

    topic = CONFIG.kafka.topic_historical_klines
    total_sent = 0
    batches_processed = 0

    logger.info(
        "[%s] Starting kline ingestion to topic '%s' (interval=%s, lookback=%dd, max_batches=%s)",
        symbol, topic, interval, lookback_days, max_batches,
    )

    for win_start, win_end in kline_time_windows(
        lookback_days=lookback_days,
        klines_per_request=cfg.klines_per_request,
        interval=interval,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
    ):
        raw_klines = client.get_klines(
            symbol=symbol,
            interval=interval,
            start_ms=win_start,
            end_ms=win_end,
            limit=cfg.klines_per_request,
        )
        if not raw_klines:
            break

        for raw in raw_klines:
            msg = normalise_kline(raw, symbol, interval=interval)
            msg_key = generate_kline_key(symbol, interval, msg["open_time"])
            sent = producer.send_message(
                topic=topic,
                payload=msg,
                key=msg_key,
                validate=True,
            )
            if sent:
                total_sent += 1

        batches_processed += 1
        logger.debug("[%s] Sent %d klines (batch %d, window end: %d)", symbol, total_sent, batches_processed, win_end)

        if max_batches and batches_processed >= max_batches:
            logger.info("[%s] Reached maximum requested batches (%d), stopping symbol ingestion.", symbol, max_batches)
            break

    logger.info("[%s] Kline ingestion complete — %d records across %d batch(es)", symbol, total_sent, batches_processed)
    return total_sent


def ingest_trades(client: BinanceRestClient, producer, symbol: str) -> None:
    raw_trades = client.get_recent_trades(symbol=symbol)
    for raw in raw_trades:
        msg = normalise_trade(raw, symbol)
        producer.send_message(topic=CONFIG.kafka.topic_trades, payload=msg, key=symbol)
    logger.info("[%s] Sent %d trade records", symbol, len(raw_trades))


def ingest_order_book(client: BinanceRestClient, producer, symbol: str) -> None:
    raw = client.get_order_book(symbol=symbol, depth=CONFIG.streaming.depth_levels)
    msg = normalise_order_book(raw, symbol)
    producer.send_message(topic=CONFIG.kafka.topic_order_book, payload=msg, key=symbol)
    logger.info("[%s] Sent order book snapshot (%d levels)", symbol, msg["levels"])


def ingest_ticker(client: BinanceRestClient, producer, symbol: str) -> None:
    raw = client.get_ticker_24h(symbol=symbol)
    msg = normalise_ticker(raw, symbol)
    producer.send_message(topic=CONFIG.kafka.topic_ticker, payload=msg, key=symbol)
    logger.info("[%s] Sent ticker record", symbol)


def run_historical_ingestion(
    symbols: Optional[List[str]] = None,
    interval: Optional[str] = None,
    lookback_days: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    max_batches: Optional[int] = None,
) -> None:
    """Execute complete historical ingestion pipeline across configured symbols."""
    client = BinanceRestClient()
    producer = get_kafka_producer()
    target_symbols = symbols or CONFIG.historical.symbols
    interval = interval or CONFIG.historical.kline_interval

    logger.info(
        "Historical ingestion pipeline started — %d symbol(s), interval=%s, lookback=%s days",
        len(target_symbols),
        interval,
        lookback_days if lookback_days is not None else CONFIG.historical.lookback_days,
    )

    for symbol in target_symbols:
        try:
            ingest_klines(
                client=client,
                producer=producer,
                symbol=symbol,
                interval=interval,
                lookback_days=lookback_days,
                start_time=start_time,
                end_time=end_time,
                max_batches=max_batches,
            )
            ingest_trades(client, producer, symbol)
            ingest_order_book(client, producer, symbol)
            ingest_ticker(client, producer, symbol)
        except Exception as exc:
            logger.error("[%s] Historical ingestion failed: %s", symbol, exc, exc_info=True)

    producer.flush()
    producer.close()
    logger.info("Historical ingestion pipeline finished successfully.")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Binance REST Historical Ingestion Producer")
    parser.add_argument("--symbols", help="Comma-separated list of symbols (e.g. BTCUSDT,ETHUSDT)")
    parser.add_argument("--interval", default="1m", help="Kline interval (default: 1m)")
    parser.add_argument("--lookback-days", type=int, help="Number of lookback days")
    parser.add_argument("--start-time", help="Start timestamp in ms or ISO-8601 string")
    parser.add_argument("--end-time", help="End timestamp in ms or ISO-8601 string")
    parser.add_argument("--max-batches", type=int, help="Maximum batches per symbol (smoke test)")
    parser.add_argument("--dry-run", action="store_true", help="Print configuration without making API requests")
    parser.add_argument("--force-run", action="store_true", help="Execute without requiring authenticated API key")
    args = parser.parse_args()

    symbols_list = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    start_ms = parse_timestamp_ms(args.start_time)
    end_ms = parse_timestamp_ms(args.end_time)

    force_run = args.force_run or os.environ.get("FORCE_RUN", "false").lower() in ("true", "1", "yes")
    has_custom_key = CONFIG.binance.api_key and CONFIG.binance.api_key != "your_binance_api_key_here"

    if args.dry_run or (not has_custom_key and not force_run):
        logger.warning(
            "========================================================\n"
            "  DRY RUN MODE — BINANCE_API_KEY is not configured.\n"
            "  No external REST requests will be made.\n"
            "  Target topic: %s\n"
            "  Symbols: %s\n"
            "  Interval: %s\n"
            "  To run: provide --force-run or set FORCE_RUN=true / BINANCE_API_KEY in .env.\n"
            "========================================================",
            CONFIG.kafka.topic_historical_klines,
            symbols_list or CONFIG.historical.symbols,
            args.interval,
        )
        return

    run_historical_ingestion(
        symbols=symbols_list,
        interval=args.interval,
        lookback_days=args.lookback_days,
        start_time=start_ms,
        end_time=end_ms,
        max_batches=args.max_batches,
    )


if __name__ == "__main__":
    main()
