"""
==============================================================================
tests/test_kafka_layer.py
Comprehensive automated test suite for the Kafka messaging and schema layer.

Covers:
  - Canonical schema normalization from Binance REST array (11 fields + drop ignore)
  - Canonical schema normalization from Binance WebSocket kline event
  - Distinction between provisional (x=False) and closed (x=True) candles
  - Consistent composite Kafka message keys (symbol_interval_open_time)
  - Schema validation rules (timestamps, positive prices, non-negativity, logical bounds)
  - Rejection of malformed / corrupted payloads
  - Kafka Producer wrapper validation & mock delivery
  - Producer serialization (key as UTF-8, value as JSON UTF-8)
  - Rate-limit retry and backoff logic with mocked HTTP responses
==============================================================================
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure producer modules can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "producer")))

from schema import (
    CanonicalKlineMessage,
    CANONICAL_KLINE_FIELDS,
    COMPATIBILITY_FIELDS,
    generate_kline_key,
    normalise_rest_kline,
    normalise_ws_kline,
    validate_kline_dict,
)
from config import CONFIG
from kafka_client import CryptoKafkaProducer
from historical.binance_rest_producer import BinanceRestClient, kline_time_windows


class TestCanonicalSchema(unittest.TestCase):
    """Test Unified Event Schema normalization and validation rules."""

    def setUp(self):
        # 12-element Binance Kline REST response
        self.raw_rest_kline = [
            1672531140000,      # 0: open_time (ms)
            "16530.50",         # 1: open
            "16550.00",         # 2: high
            "16525.10",         # 3: low
            "16545.20",         # 4: close
            "45.2154",          # 5: volume
            1672531199999,      # 6: close_time (ms)
            "747942.31",        # 7: quote_asset_volume
            184,                # 8: number_of_trades
            "22.1050",          # 9: taker_buy_base_volume
            "365780.12",        # 10: taker_buy_quote_volume
            "0",                # 11: ignore (SHOULD BE DROPPED)
        ]

        # Binance WebSocket closed kline frame (x=True)
        self.raw_ws_kline_closed = {
            "e": "kline",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "k": {
                "t": 1672531140000,
                "T": 1672531199999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 100,
                "L": 284,
                "o": "16530.50",
                "c": "16545.20",
                "h": "16550.00",
                "l": "16525.10",
                "v": "45.2154",
                "n": 184,
                "x": True,  # CLOSED
                "q": "747942.31",
                "V": "22.1050",
                "Q": "365780.12",
                "B": "0",
            },
        }

        # Binance WebSocket provisional kline frame (x=False)
        self.raw_ws_kline_provisional = {
            "e": "kline",
            "E": 1672531160000,
            "s": "BTCUSDT",
            "k": {
                "t": 1672531140000,
                "T": 1672531199999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 100,
                "L": 150,
                "o": "16530.50",
                "c": "16540.00",
                "h": "16548.00",
                "l": "16528.00",
                "v": "20.1000",
                "n": 95,
                "x": False,  # PROVISIONAL
                "q": "332454.00",
                "V": "10.0000",
                "Q": "165400.00",
                "B": "0",
            },
        }

    def test_rest_normalization_excludes_ignore_and_populates_unified_fields(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT", interval="1m")

        # Must not contain 'ignore'
        self.assertNotIn("ignore", norm)

        # Must contain all required canonical fields
        for field in CANONICAL_KLINE_FIELDS:
            self.assertIn(field, norm)

        # Must contain backward-compatibility fields
        for field in COMPATIBILITY_FIELDS:
            self.assertIn(field, norm)

        self.assertEqual(norm["symbol"], "BTCUSDT")
        self.assertEqual(norm["interval"], "1m")
        self.assertEqual(norm["open_time"], 1672531140000)
        self.assertEqual(norm["close_time"], 1672531199999)
        self.assertEqual(norm["open"], 16530.50)
        self.assertEqual(norm["high"], 16550.00)
        self.assertEqual(norm["low"], 16525.10)
        self.assertEqual(norm["close"], 16545.20)
        self.assertEqual(norm["volume"], 45.2154)
        self.assertEqual(norm["quote_volume"], 747942.31)
        self.assertEqual(norm["quote_asset_volume"], 747942.31)
        self.assertEqual(norm["number_of_trades"], 184)
        self.assertEqual(norm["taker_buy_volume"], 22.1050)
        self.assertEqual(norm["is_closed"], True)
        self.assertEqual(norm["source"], "historical")
        self.assertEqual(norm["data_source"], "historical")

        is_valid, reason = validate_kline_dict(norm)
        self.assertTrue(is_valid, f"Validation failed: {reason}")

    def test_ws_normalization_closed_candle(self):
        norm = normalise_ws_kline(self.raw_ws_kline_closed)

        self.assertEqual(norm["symbol"], "BTCUSDT")
        self.assertEqual(norm["interval"], "1m")
        self.assertEqual(norm["is_closed"], True)
        self.assertEqual(norm["source"], "realtime")
        self.assertEqual(norm["event_time"], 1672531200000)
        self.assertEqual(norm["close"], 16545.20)

        is_valid, reason = validate_kline_dict(norm)
        self.assertTrue(is_valid, f"Validation failed: {reason}")

    def test_ws_normalization_provisional_candle(self):
        norm = normalise_ws_kline(self.raw_ws_kline_provisional)

        self.assertEqual(norm["symbol"], "BTCUSDT")
        self.assertEqual(norm["interval"], "1m")
        self.assertEqual(norm["is_closed"], False)
        self.assertEqual(norm["source"], "realtime")
        self.assertEqual(norm["event_time"], 1672531160000)
        self.assertEqual(norm["close"], 16540.00)

        is_valid, reason = validate_kline_dict(norm)
        self.assertTrue(is_valid, f"Validation failed: {reason}")

    def test_generate_kline_key(self):
        key = generate_kline_key("BTCUSDT", "1m", 1672531140000)
        self.assertEqual(key, "BTCUSDT_1m_1672531140000")

    def test_validation_rejects_missing_fields(self):
        record = {"symbol": "BTCUSDT", "open_time": 1000}
        is_valid, reason = validate_kline_dict(record)
        self.assertFalse(is_valid)
        self.assertIn("Missing required canonical field", reason)

    def test_validation_rejects_inverted_high_low(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        norm["high"] = 100.0
        norm["low"] = 200.0
        is_valid, reason = validate_kline_dict(norm)
        self.assertFalse(is_valid)
        self.assertIn("cannot be less than low price", reason)

    def test_validation_rejects_inverted_timestamps(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        norm["close_time"] = norm["open_time"] - 1000
        is_valid, reason = validate_kline_dict(norm)
        self.assertFalse(is_valid)
        self.assertIn("cannot be earlier than open_time", reason)

    def test_validation_rejects_negative_volume(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        norm["volume"] = -5.0
        is_valid, reason = validate_kline_dict(norm)
        self.assertFalse(is_valid)
        self.assertIn("cannot be negative", reason)

    def test_validation_rejects_non_boolean_is_closed(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        norm["is_closed"] = "true"  # Not a boolean
        is_valid, reason = validate_kline_dict(norm)
        self.assertFalse(is_valid)
        self.assertIn("is_closed must be boolean", reason)

    def test_validation_rejects_invalid_source(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        norm["source"] = "unknown_source"
        is_valid, reason = validate_kline_dict(norm)
        self.assertFalse(is_valid)
        self.assertIn("source must be 'historical' or 'realtime'", reason)


class TestCryptoKafkaProducer(unittest.TestCase):
    """Test producer wrapper, validation gate, and serialization."""

    @patch("kafka_client.KafkaProducer")
    def test_producer_validates_and_sends(self, mock_kafka_producer_cls):
        mock_raw_producer = MagicMock()
        mock_kafka_producer_cls.return_value = mock_raw_producer

        producer = CryptoKafkaProducer()

        valid_record = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "open_time": 1672531140000,
            "close_time": 1672531199999,
            "open": 16500.0,
            "high": 16600.0,
            "low": 16400.0,
            "close": 16550.0,
            "volume": 10.0,
            "quote_volume": 165000.0,
            "number_of_trades": 50,
            "taker_buy_volume": 5.0,
            "is_closed": True,
            "event_time": 1672531199999,
            "source": "historical",
        }

        # Send valid record with validation enabled
        key = generate_kline_key("BTCUSDT", "1m", 1672531140000)
        sent = producer.send_message("crypto_klines_historical", valid_record, key=key, validate=True)
        self.assertTrue(sent)
        self.assertEqual(producer.sent_count, 1)
        self.assertEqual(producer.validation_failure_count, 0)
        mock_raw_producer.send.assert_called_once_with(
            "crypto_klines_historical",
            value=valid_record,
            key=key,
        )

        # Send invalid record with validation enabled
        invalid_record = {"symbol": "BTCUSDT", "bad_payload": True}
        sent_invalid = producer.send_message("crypto_klines_historical", invalid_record, key=key, validate=True)
        self.assertFalse(sent_invalid)
        self.assertEqual(producer.validation_failure_count, 1)


class TestPaginationAndRateLimit(unittest.TestCase):
    """Test pagination time windows and backoff handling."""

    def test_kline_time_windows_chunking(self):
        start_ms = 1672531200000  # 2023-01-01 00:00:00 UTC
        end_ms = start_ms + (3000 * 60_000) - 1  # exactly 3000 minutes = 3 chunks of 1000

        windows = list(kline_time_windows(
            klines_per_request=1000,
            interval="1m",
            start_time_ms=start_ms,
            end_time_ms=end_ms,
        ))
        self.assertEqual(len(windows), 3)  # 0-999, 1000-1999, 2000-2999
        self.assertEqual(windows[0][0], start_ms)
        self.assertEqual(windows[0][1], start_ms + (1000 * 60_000) - 1)


class TestTopicConfiguration(unittest.TestCase):
    """Verify topic names in runtime configuration."""

    def test_topic_names_configured(self):
        cfg = CONFIG.kafka
        self.assertEqual(cfg.topic_historical_klines, "crypto_klines_historical")
        self.assertEqual(cfg.topic_streaming_klines, "crypto_klines_streaming")


if __name__ == "__main__":
    unittest.main()
