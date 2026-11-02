"""
==============================================================================
producer/schema.py
Unified Event Schema, Normalization, and Validation for Crypto Market Analytics.

Defines the canonical Kline (candlestick) data model across both ingestion paths:
1. Historical REST Ingestion (Binance REST API -> crypto_klines_historical)
2. Real-Time Streaming Ingestion (Binance WebSocket -> crypto_klines_streaming)

Unified Event Schema Fields:
  1.  symbol                  (str, uppercase trading pair e.g. 'BTCUSDT')
  2.  interval                (str, candlestick timeframe e.g. '1m', '5m', '1h')
  3.  open_time               (int, UTC Epoch milliseconds of candle start)
  4.  close_time              (int, UTC Epoch milliseconds of candle end)
  5.  open                    (float, candle opening price)
  6.  high                    (float, candle highest price)
  7.  low                     (float, candle lowest price)
  8.  close                   (float, candle closing price)
  9.  volume                  (float, base asset trading volume)
  10. quote_volume            (float, quote asset trading volume)
  11. number_of_trades        (int, total trades executed in candle window)
  12. taker_buy_volume        (float, taker buy base asset volume)
  13. is_closed               (bool, True if candle is finalized/closed, False if provisional)
  14. event_time              (int, UTC Epoch milliseconds of exchange event generation)
  15. source                  (str, 'historical' or 'realtime')

Backward-Compatibility Lineage Aliases:
  - quote_asset_volume        (float, identical to quote_volume)
  - taker_buy_base_volume     (float, identical to taker_buy_volume)
  - taker_buy_quote_volume    (float, quote taker buy volume)
  - data_source               (str, identical to source)
  - ingested_at               (str, ISO-8601 UTC timestamp of pipeline ingestion)
==============================================================================
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Required fields in the Unified Event Schema
CANONICAL_KLINE_FIELDS = (
    "symbol",
    "interval",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "number_of_trades",
    "taker_buy_volume",
    "is_closed",
    "event_time",
    "source",
)

# Backward-compatibility alias fields
COMPATIBILITY_FIELDS = (
    "quote_asset_volume",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "data_source",
    "ingested_at",
)


@dataclass(frozen=True)
class CanonicalKlineMessage:
    """Immutable data container representing a standardized Kline event."""
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    number_of_trades: int
    taker_buy_volume: float
    is_closed: bool
    event_time: int
    source: str
    # Lineage & compatibility fields
    quote_asset_volume: float = 0.0
    taker_buy_base_volume: float = 0.0
    taker_buy_quote_volume: float = 0.0
    data_source: str = ""
    ingested_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Ensure compatibility fields are populated if empty
        if not data.get("quote_asset_volume"):
            data["quote_asset_volume"] = self.quote_volume
        if not data.get("taker_buy_base_volume"):
            data["taker_buy_base_volume"] = self.taker_buy_volume
        if not data.get("data_source"):
            data["data_source"] = self.source
        if not data.get("ingested_at"):
            data["ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


def generate_kline_key(symbol: str, interval: str, open_time: int) -> str:
    """
    Generate a deterministic, partition-aligned Kafka message key.
    Format: <SYMBOL>_<INTERVAL>_<OPEN_TIME> (e.g. BTCUSDT_1m_1672531140000)
    Guarantees strict per-candle partition routing and deduplication support.
    """
    return f"{symbol.strip().upper()}_{interval.strip()}_{int(open_time)}"


def validate_kline_dict(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate that a dictionary strictly conforms to the Unified Event Schema.
    Returns (is_valid, error_reason).
    """
    if not isinstance(record, dict):
        return False, "Record must be a dictionary"

    # 1. Required fields presence check
    for field_name in CANONICAL_KLINE_FIELDS:
        if field_name not in record:
            return False, f"Missing required canonical field: {field_name}"

    # 2. Symbol check
    symbol = record.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return False, f"Invalid symbol: {symbol}"

    # 3. Interval check
    interval = record.get("interval")
    if not isinstance(interval, str) or not interval.strip():
        return False, f"Invalid interval: {interval}"

    # 4. Timestamp checks (UTC Epoch ms)
    open_time = record.get("open_time")
    close_time = record.get("close_time")
    event_time = record.get("event_time")
    if not isinstance(open_time, int) or open_time <= 0:
        return False, f"Invalid open_time (must be positive int ms): {open_time}"
    if not isinstance(close_time, int) or close_time <= 0:
        return False, f"Invalid close_time (must be positive int ms): {close_time}"
    if close_time < open_time:
        return False, f"close_time ({close_time}) cannot be earlier than open_time ({open_time})"
    if not isinstance(event_time, int) or event_time <= 0:
        return False, f"Invalid event_time (must be positive int ms): {event_time}"

    # 5. Numeric price checks (must be positive)
    price_fields = ("open", "high", "low", "close")
    for name in price_fields:
        val = record.get(name)
        if not isinstance(val, (int, float)):
            return False, f"Price field '{name}' must be numeric, got {type(val).__name__}"
        if val <= 0:
            return False, f"Price field '{name}' must be positive: {val}"

    # 6. High / Low logical boundary constraints
    o, h, l, c = record["open"], record["high"], record["low"], record["close"]
    if h < l:
        return False, f"High price ({h}) cannot be less than low price ({l})"
    if h < o or h < c:
        return False, f"High price ({h}) must be >= open ({o}) and close ({c})"
    if l > o or l > c:
        return False, f"Low price ({l}) must be <= open ({o}) and close ({c})"

    # 7. Volume checks (must be non-negative)
    volume_fields = ("volume", "quote_volume", "taker_buy_volume")
    for name in volume_fields:
        val = record.get(name)
        if not isinstance(val, (int, float)):
            return False, f"Volume field '{name}' must be numeric, got {type(val).__name__}"
        if val < 0:
            return False, f"Volume field '{name}' cannot be negative: {val}"

    # 8. Trades count check
    trades = record.get("number_of_trades")
    if not isinstance(trades, int) or trades < 0:
        return False, f"number_of_trades must be non-negative integer: {trades}"

    # 9. is_closed check (must be boolean)
    is_closed = record.get("is_closed")
    if not isinstance(is_closed, bool):
        return False, f"is_closed must be boolean, got {type(is_closed).__name__}"

    # 10. Source check
    source = record.get("source")
    if source not in ("historical", "realtime"):
        return False, f"source must be 'historical' or 'realtime', got '{source}'"

    return True, None


def normalise_rest_kline(
    raw: List[Any],
    symbol: str,
    interval: str = "1m",
) -> Dict[str, Any]:
    """
    Convert a raw Binance REST API Kline array (11 or 12 elements) into Unified Event Schema format.
    
    Binance REST Kline array layout:
      0: Open time (ms)
      1: Open price
      2: High price
      3: Low price
      4: Close price
      5: Volume
      6: Close time (ms)
      7: Quote asset volume
      8: Number of trades
      9: Taker buy base asset volume
     10: Taker buy quote asset volume
     11: Ignore (DROPPED)
    """
    if len(raw) < 11:
        raise ValueError(f"Expected at least 11 Kline elements from Binance REST, got {len(raw)}")

    open_time = int(raw[0])
    close_time = int(raw[6])
    quote_volume = float(raw[7])
    taker_buy_volume = float(raw[9])
    taker_buy_quote = float(raw[10])

    now_iso = datetime.now(tz=timezone.utc).isoformat()

    return {
        "symbol": symbol.strip().upper(),
        "interval": interval.strip(),
        "open_time": open_time,
        "close_time": close_time,
        "open": float(raw[1]),
        "high": float(raw[2]),
        "low": float(raw[3]),
        "close": float(raw[4]),
        "volume": float(raw[5]),
        "quote_volume": quote_volume,
        "number_of_trades": int(raw[8]),
        "taker_buy_volume": taker_buy_volume,
        "is_closed": True,  # Historical klines fetched via REST pagination are completed candles
        "event_time": close_time,
        "source": "historical",
        # Backward-compatibility fields
        "quote_asset_volume": quote_volume,
        "taker_buy_base_volume": taker_buy_volume,
        "taker_buy_quote_volume": taker_buy_quote,
        "data_source": "historical",
        "ingested_at": now_iso,
    }


def normalise_ws_kline(
    data: Dict[str, Any],
    symbol_override: Optional[str] = None,
    default_interval: str = "1m",
) -> Dict[str, Any]:
    """
    Convert a Binance WebSocket Kline frame (@kline_<interval>) into Unified Event Schema format.
    
    Extracts both provisional and closed candles, preserving exchange timestamps (E),
    interval (i), and completion state (x).
    """
    k = data.get("k", {})
    raw_symbol = data.get("s") or k.get("s") or symbol_override or ""
    symbol = raw_symbol.strip().upper()
    interval = str(k.get("i", default_interval)).strip()

    open_time = int(k.get("t", 0))
    close_time = int(k.get("T", 0))
    event_time = int(data.get("E", close_time if close_time > 0 else open_time))
    is_closed = bool(k.get("x", False))

    quote_volume = float(k.get("q", 0.0))
    taker_buy_volume = float(k.get("V", 0.0))
    taker_buy_quote = float(k.get("Q", 0.0))

    now_iso = datetime.now(tz=timezone.utc).isoformat()

    return {
        "symbol": symbol,
        "interval": interval,
        "open_time": open_time,
        "close_time": close_time,
        "open": float(k.get("o", 0.0)),
        "high": float(k.get("h", 0.0)),
        "low": float(k.get("l", 0.0)),
        "close": float(k.get("c", 0.0)),
        "volume": float(k.get("v", 0.0)),
        "quote_volume": quote_volume,
        "number_of_trades": int(k.get("n", 0)),
        "taker_buy_volume": taker_buy_volume,
        "is_closed": is_closed,
        "event_time": event_time,
        "source": "realtime",
        # Backward-compatibility fields
        "quote_asset_volume": quote_volume,
        "taker_buy_base_volume": taker_buy_volume,
        "taker_buy_quote_volume": taker_buy_quote,
        "data_source": "realtime",
        "ingested_at": now_iso,
    }
