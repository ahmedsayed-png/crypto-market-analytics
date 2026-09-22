"""
==============================================================================
producer/historical/binance_rest_producer.py
Historical Ingestion — Binance REST API → Kafka

Flow
----
  Binance REST API (/api/v3/klines, /api/v3/trades)
      ↓  paginated requests per symbol per time window
  Normalise & enrich with metadata (data_source="historical")
      ↓
  Kafka topics:
      market_klines        — OHLCV candlestick data
      market_trades        — individual raw trade records
      order_book_events    — depth snapshot (top N levels)
      ticker_events        — 24-hour rolling ticker stats

Usage
-----
  python producer/historical/binance_rest_producer.py

The script will be activated when the team is ready to begin ingestion.
No Binance API calls are made simply by importing or running this file
while DRY_RUN=true (default when BINANCE_API_KEY is absent).
==============================================================================
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()

# Add project root to sys.path so sibling modules resolve
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import CONFIG
from kafka_client import get_kafka_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("historical_producer")


# ---------------------------------------------------------------------------
# REST Client helper
# ---------------------------------------------------------------------------

class BinanceRestClient:
    """
    Thin wrapper around the Binance public REST API.
    Rate-limit-aware: inserts a configurable pause between calls.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "X-MBX-APIKEY": CONFIG.binance.api_key,
            "Content-Type": "application/json",
        })
        self._base_url = CONFIG.binance.base_url
        self._pause = CONFIG.binance.rate_limit_pause_seconds

    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _get(self, endpoint: str, params: Dict[str, Any]) -> Any:
        url = f"{self._base_url}{endpoint}"
        response = self._session.get(url, params=params, timeout=10)
        response.raise_for_status()
        time.sleep(self._pause)
        return response.json()

    # ------------------------------------------------------------------
    # Public data-fetching methods (each returns raw Binance JSON)
    # ------------------------------------------------------------------

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> List[List]:
        """
        Fetch kline (candlestick) records for a single time window.
        Returns a list of raw Binance kline arrays.
        """
        return self._get(
            CONFIG.binance.klines_endpoint,
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": limit,
            },
        )

    def get_recent_trades(self, symbol: str, limit: int = 1000) -> List[Dict]:
        """Fetch the most recent trades for a symbol."""
        return self._get(
            CONFIG.binance.trades_endpoint,
            {"symbol": symbol, "limit": limit},
        )

    def get_order_book(self, symbol: str, depth: int = 20) -> Dict:
        """Fetch order book snapshot (top N bids/asks)."""
        return self._get(
            CONFIG.binance.depth_endpoint,
            {"symbol": symbol, "limit": depth},
        )

    def get_ticker_24h(self, symbol: str) -> Dict:
        """Fetch 24-hour rolling ticker statistics."""
        return self._get(
            CONFIG.binance.ticker_endpoint,
            {"symbol": symbol},
        )


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _base_metadata(symbol: str, data_source: str = "historical") -> Dict:
    """Common fields attached to every produced message."""
    return {
        "symbol": symbol,
        "data_source": data_source,
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def normalise_kline(raw: List, symbol: str) -> Dict:
    """
    Convert a raw Binance kline array to a structured dict.

    Binance kline array index mapping:
      0  open_time           (ms)
      1  open
      2  high
      3  low
      4  close
      5  volume
      6  close_time          (ms)
      7  quote_asset_volume
      8  number_of_trades
      9  taker_buy_base_vol
     10  taker_buy_quote_vol
     11  ignore
    """
    return {
        **_base_metadata(symbol),
        "open_time": raw[0],
        "open": float(raw[1]),
        "high": float(raw[2]),
        "low": float(raw[3]),
        "close": float(raw[4]),
        "volume": float(raw[5]),
        "close_time": raw[6],
        "quote_asset_volume": float(raw[7]),
        "number_of_trades": int(raw[8]),
        "taker_buy_base_volume": float(raw[9]),
        "taker_buy_quote_volume": float(raw[10]),
    }


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
# Pagination generator
# ---------------------------------------------------------------------------

def kline_time_windows(
    lookback_days: int,
    klines_per_request: int,
    interval_ms: int = 60_000,  # 1 minute in ms
) -> Generator[tuple, None, None]:
    """
    Yield (start_ms, end_ms) pairs covering the full lookback window
    broken into chunks of ``klines_per_request`` candles.
    """
    end_dt = datetime.now(tz=timezone.utc)
    start_dt = end_dt - timedelta(days=lookback_days)

    chunk_duration_ms = klines_per_request * interval_ms
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms_total = int(end_dt.timestamp() * 1000)

    while start_ms < end_ms_total:
        window_end_ms = min(start_ms + chunk_duration_ms - 1, end_ms_total)
        yield start_ms, window_end_ms
        start_ms = window_end_ms + 1


# ---------------------------------------------------------------------------
# Main ingestion entry points
# ---------------------------------------------------------------------------

def ingest_klines(
    client: BinanceRestClient,
    producer,
    symbol: str,
) -> None:
    """
    Paginate through the full history window for *symbol* and push
    normalised kline records to the ``market_klines`` Kafka topic.
    """
    cfg = CONFIG.historical
    total_sent = 0

    logger.info("[%s] Starting kline ingestion (lookback=%dd)", symbol, cfg.lookback_days)

    for start_ms, end_ms in kline_time_windows(
        lookback_days=cfg.lookback_days,
        klines_per_request=cfg.klines_per_request,
    ):
        raw_klines = client.get_klines(
            symbol=symbol,
            interval=cfg.kline_interval,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=cfg.klines_per_request,
        )
        if not raw_klines:
            break

        for raw in raw_klines:
            msg = normalise_kline(raw, symbol)
            producer.send_message(
                topic=CONFIG.kafka.topic_klines,
                payload=msg,
                key=symbol,
            )
            total_sent += 1

        logger.debug("[%s] Sent %d klines (window end: %d)", symbol, total_sent, end_ms)

    logger.info("[%s] Kline ingestion complete — %d records", symbol, total_sent)


def ingest_trades(
    client: BinanceRestClient,
    producer,
    symbol: str,
) -> None:
    """Fetch recent trades for *symbol* and push to ``market_trades``."""
    raw_trades = client.get_recent_trades(symbol=symbol)
    for raw in raw_trades:
        msg = normalise_trade(raw, symbol)
        producer.send_message(
            topic=CONFIG.kafka.topic_trades,
            payload=msg,
            key=symbol,
        )
    logger.info("[%s] Sent %d trade records", symbol, len(raw_trades))


def ingest_order_book(
    client: BinanceRestClient,
    producer,
    symbol: str,
) -> None:
    """Fetch order book snapshot for *symbol* and push to ``order_book_events``."""
    raw = client.get_order_book(symbol=symbol, depth=CONFIG.streaming.depth_levels)
    msg = normalise_order_book(raw, symbol)
    producer.send_message(
        topic=CONFIG.kafka.topic_order_book,
        payload=msg,
        key=symbol,
    )
    logger.info("[%s] Sent order book snapshot (%d levels)", symbol, msg["levels"])


def ingest_ticker(
    client: BinanceRestClient,
    producer,
    symbol: str,
) -> None:
    """Fetch 24h ticker for *symbol* and push to ``ticker_events``."""
    raw = client.get_ticker_24h(symbol=symbol)
    msg = normalise_ticker(raw, symbol)
    producer.send_message(
        topic=CONFIG.kafka.topic_ticker,
        payload=msg,
        key=symbol,
    )
    logger.info("[%s] Sent ticker record", symbol)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_historical_ingestion() -> None:
    """
    Full historical ingestion pipeline.
    Iterates over every configured symbol and ingests:
      1. Klines  → market_klines
      2. Trades  → market_trades
      3. Depth   → order_book_events
      4. Ticker  → ticker_events
    """
    client = BinanceRestClient()
    producer = get_kafka_producer()
    symbols = CONFIG.historical.symbols

    logger.info(
        "Historical ingestion started — %d symbols, interval=%s, lookback=%dd",
        len(symbols),
        CONFIG.historical.kline_interval,
        CONFIG.historical.lookback_days,
    )

    for symbol in symbols:
        try:
            ingest_klines(client, producer, symbol)
            ingest_trades(client, producer, symbol)
            ingest_order_book(client, producer, symbol)
            ingest_ticker(client, producer, symbol)
        except Exception as exc:
            logger.error("[%s] Ingestion failed: %s", symbol, exc, exc_info=True)

    producer.flush()
    producer.close()
    logger.info("Historical ingestion complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dry_run = not CONFIG.binance.api_key or CONFIG.binance.api_key == "your_binance_api_key_here"
    if dry_run:
        logger.warning(
            "DRY RUN MODE — BINANCE_API_KEY is not set. "
            "No API calls will be made. "
            "Set BINANCE_API_KEY in .env to start real ingestion."
        )
    else:
        run_historical_ingestion()
