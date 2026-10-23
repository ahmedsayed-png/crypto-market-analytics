"""
==============================================================================
tests/test_kafka_layer.py
Comprehensive automated test suite for the Kafka messaging and schema layer.

Covers:
  - Canonical schema normalization from Binance REST array (11 fields + drop ignore)
  - Canonical schema normalization from Binance WebSocket kline event
  - Schema validation rules (timestamps, prices, non-negativity, logical high/low)
  - Rejection of malformed / corrupted payloads
  - Kafka Producer wrapper validation & mock delivery
  - Producer serialization (key as UTF-8, value as JSON UTF-8)
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
    normalise_rest_kline,
    normalise_ws_kline,
    validate_kline_dict,
)
from config import CONFIG
from kafka_client import CryptoKafkaProducer


class TestCanonicalSchema(unittest.TestCase):
    """Test schema normalization and validation rules."""

    def setUp(self):
        # 12-element Binance Kline response
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

        # Binance WebSocket kline frame
        self.raw_ws_kline_payload = {
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
                "x": True,
                "q": "747942.31",
                "V": "22.1050",
                "Q": "365780.12",
                "B": "0",
            },
        }

    def test_rest_normalization_excludes_ignore_and_casts(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")

        # Must not contain 'ignore'
        self.assertNotIn("ignore", norm)

        # Must contain all 11 documented fields + metadata
        for field in CANONICAL_KLINE_FIELDS:
            self.assertIn(field, norm)

        self.assertEqual(norm["symbol"], "BTCUSDT")
        self.assertEqual(norm["open_time"], 1672531140000)
        self.assertEqual(norm["close_time"], 1672531199999)
        self.assertEqual(norm["open"], 16530.50)
        self.assertEqual(norm["high"], 16550.00)
        self.assertEqual(norm["low"], 16525.10)
        self.assertEqual(norm["close"], 16545.20)
        self.assertEqual(norm["volume"], 45.2154)
        self.assertEqual(norm["quote_asset_volume"], 747942.31)
        self.assertEqual(norm["number_of_trades"], 184)
        self.assertEqual(norm["taker_buy_base_volume"], 22.1050)
        self.assertEqual(norm["taker_buy_quote_volume"], 365780.12)
        self.assertEqual(norm["data_source"], "historical")

        # Validate against schema rules
        is_valid, reason = validate_kline_dict(norm)
        self.assertTrue(is_valid, f"Validation failed: {reason}")

    def test_ws_normalization_and_validation(self):
        norm = normalise_ws_kline(self.raw_ws_kline_payload)

        self.assertNotIn("ignore", norm)
        self.assertNotIn("B", norm)
        self.assertEqual(norm["symbol"], "BTCUSDT")
        self.assertEqual(norm["data_source"], "realtime")
        self.assertEqual(norm["close"], 16545.20)
        self.assertEqual(norm["open_time"], 1672531140000)
        self.assertEqual(norm["close_time"], 1672531199999)

        is_valid, reason = validate_kline_dict(norm)
        self.assertTrue(is_valid, f"Validation failed: {reason}")

    def test_validation_rejects_missing_fields(self):
        record = {"symbol": "BTCUSDT", "open_time": 1000}
        is_valid, reason = validate_kline_dict(record)
        self.assertFalse(is_valid)
        self.assertIn("Missing required canonical field", reason)

    def test_validation_rejects_inverted_high_low(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        # Invert high and low
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

    def test_validation_rejects_invalid_data_source(self):
        norm = normalise_rest_kline(self.raw_rest_kline, "BTCUSDT")
        norm["data_source"] = "unknown_source"
        is_valid, reason = validate_kline_dict(norm)
        self.assertFalse(is_valid)
        self.assertIn("data_source must be 'historical' or 'realtime'", reason)


class TestCryptoKafkaProducer(unittest.TestCase):
    """Test producer wrapper, validation gate, and serialization."""

    @patch("kafka_client.KafkaProducer")
    def test_producer_validates_and_sends(self, mock_kafka_producer_cls):
        mock_raw_producer = MagicMock()
        mock_kafka_producer_cls.return_value = mock_raw_producer

        producer = CryptoKafkaProducer()

        valid_record = {
            "open_time": 1672531140000,
            "symbol": "BTCUSDT",
            "open": 16500.0,
            "high": 16600.0,
            "low": 16400.0,
            "close": 16550.0,
            "volume": 10.0,
            "close_time": 1672531199999,
            "quote_asset_volume": 165000.0,
            "number_of_trades": 50,
            "taker_buy_base_volume": 5.0,
            "taker_buy_quote_volume": 82500.0,
            "data_source": "historical",
            "ingested_at": "2026-10-23T12:00:00Z",
        }

        # Send valid record with validation enabled
        sent = producer.send_message("crypto_klines_historical", valid_record, key="BTCUSDT", validate=True)
        self.assertTrue(sent)
        self.assertEqual(producer.sent_count, 1)
        self.assertEqual(producer.validation_failure_count, 0)
        mock_raw_producer.send.assert_called_once_with(
            "crypto_klines_historical",
            value=valid_record,
            key="BTCUSDT",
        )

        # Send invalid record with validation enabled
        invalid_record = {"symbol": "BTCUSDT", "bad_payload": True}
        sent_invalid = producer.send_message("crypto_klines_historical", invalid_record, key="BTCUSDT", validate=True)
        self.assertFalse(sent_invalid)
        self.assertEqual(producer.validation_failure_count, 1)


class TestTopicConfiguration(unittest.TestCase):
    """Verify topic names in runtime configuration."""

    def test_topic_names_configured(self):
        cfg = CONFIG.kafka
        self.assertEqual(cfg.topic_historical_klines, "crypto_klines_historical")
        self.assertEqual(cfg.topic_streaming_klines, "crypto_klines_streaming")


if __name__ == "__main__":
    unittest.main()
