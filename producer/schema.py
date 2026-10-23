"""
==============================================================================
producer/schema.py
Canonical Message Schema and Validation for Crypto Market Analytics.

Based on Binance REST and WebSocket Kline specifications and project requirements.
Fields:
  1.  open_time               (int, Epoch milliseconds)
  2.  symbol                  (str, uppercase trading pair e.g. BTCUSDT)
  3.  open                    (float)
  4.  high                    (float)
  5.  low                     (float)
  6.  close                   (float)
  7.  volume                  (float, base asset volume)
  8.  close_time              (int, Epoch milliseconds)
  9.  quote_asset_volume      (float)
  10. number_of_trades        (int)
  11. taker_buy_base_volume   (float)
  12. taker_buy_quote_volume  (float)

Metadata fields:
  - data_source               (str, 'historical' or 'realtime')
  - ingested_at               (str, ISO-8601 UTC timestamp)
==============================================================================
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

CANONICAL_KLINE_FIELDS = (
    "open_time",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "data_source",
    "ingested_at",
)


@dataclass(frozen=True)
class CanonicalKlineMessage:
    open_time: int
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float
    data_source: str
    ingested_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


def validate_kline_dict(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate that a record matches the Canonical Kline specification.
    Returns (is_valid, error_reason).
    """
    if not isinstance(record, dict):
        return False, "Record must be a dictionary"

    for field_name in CANONICAL_KLINE_FIELDS:
        if field_name not in record:
            return False, f"Missing required canonical field: {field_name}"

    # Symbol check
    symbol = record.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return False, f"Invalid symbol: {symbol}"

    # Timestamp checks
    open_time = record.get("open_time")
    close_time = record.get("close_time")
    if not isinstance(open_time, int) or open_time <= 0:
        return False, f"Invalid open_time (must be positive int ms): {open_time}"
    if not isinstance(close_time, int) or close_time <= 0:
        return False, f"Invalid close_time (must be positive int ms): {close_time}"
    if close_time < open_time:
        return False, f"close_time ({close_time}) cannot be earlier than open_time ({open_time})"

    # Numeric price / volume checks
    numeric_fields = (
        ("open", record.get("open")),
        ("high", record.get("high")),
        ("low", record.get("low")),
        ("close", record.get("close")),
        ("volume", record.get("volume")),
        ("quote_asset_volume", record.get("quote_asset_volume")),
        ("taker_buy_base_volume", record.get("taker_buy_base_volume")),
        ("taker_buy_quote_volume", record.get("taker_buy_quote_volume")),
    )
    for name, val in numeric_fields:
        if not isinstance(val, (int, float)):
            return False, f"Field '{name}' must be numeric, got {type(val).__name__}"
        if val < 0 and name in ("volume", "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"):
            return False, f"Field '{name}' cannot be negative: {val}"

    # High / Low logical constraints
    o, h, l, c = record["open"], record["high"], record["low"], record["close"]
    if h < l:
        return False, f"High price ({h}) cannot be less than low price ({l})"
    if h < o or h < c:
        return False, f"High price ({h}) must be >= open ({o}) and close ({c})"
    if l > o or l > c:
        return False, f"Low price ({l}) must be <= open ({o}) and close ({c})"

    # Trades count
    trades = record.get("number_of_trades")
    if not isinstance(trades, int) or trades < 0:
        return False, f"number_of_trades must be non-negative integer: {trades}"

    # Data source
    data_source = record.get("data_source")
    if data_source not in ("historical", "realtime"):
        return False, f"data_source must be 'historical' or 'realtime', got '{data_source}'"

    return True, None


def normalise_rest_kline(raw: List[Any], symbol: str) -> Dict[str, Any]:
    """
    Convert a raw Binance REST API Kline array (12 elements) into canonical format.
    Raw layout:
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
        raise ValueError(f"Expected at least 11 Kline elements, got {len(raw)}")

    return {
        "open_time": int(raw[0]),
        "symbol": symbol.strip().upper(),
        "open": float(raw[1]),
        "high": float(raw[2]),
        "low": float(raw[3]),
        "close": float(raw[4]),
        "volume": float(raw[5]),
        "close_time": int(raw[6]),
        "quote_asset_volume": float(raw[7]),
        "number_of_trades": int(raw[8]),
        "taker_buy_base_volume": float(raw[9]),
        "taker_buy_quote_volume": float(raw[10]),
        "data_source": "historical",
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def normalise_ws_kline(data: Dict[str, Any], symbol_override: Optional[str] = None) -> Dict[str, Any]:
    """
    Convert a Binance WebSocket Kline frame (@kline_<interval>) into canonical format.
    """
    k = data.get("k", {})
    raw_symbol = data.get("s") or symbol_override or ""
    symbol = raw_symbol.strip().upper()

    return {
        "open_time": int(k.get("t", 0)),
        "symbol": symbol,
        "open": float(k.get("o", 0.0)),
        "high": float(k.get("h", 0.0)),
        "low": float(k.get("l", 0.0)),
        "close": float(k.get("c", 0.0)),
        "volume": float(k.get("v", 0.0)),
        "close_time": int(k.get("T", 0)),
        "quote_asset_volume": float(k.get("q", 0.0)),
        "number_of_trades": int(k.get("n", 0)),
        "taker_buy_base_volume": float(k.get("V", 0.0)),
        "taker_buy_quote_volume": float(k.get("Q", 0.0)),
        "data_source": "realtime",
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
    }
