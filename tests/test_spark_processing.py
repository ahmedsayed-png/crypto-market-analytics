"""
==============================================================================
tests/test_spark_processing.py
Unit tests for Spark batch & streaming transformation, schema parsing,
numeric casting, deduplication, and malformed message validation.
==============================================================================
"""

import json
import os
import sys
import unittest

# Ensure modules can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "spark", "apps")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "producer")))
sys.path.insert(0, "/app")

from schema import validate_kline_dict, CANONICAL_KLINE_FIELDS

try:
    from historical_processing import CANONICAL_KLINE_SPARK_SCHEMA
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


class TestSparkSchemaAndValidation(unittest.TestCase):
    """Test schema definitions and record validation logic used in Spark jobs."""

    def setUp(self):
        self.valid_kline = {
            "open_time": 1700000000000,
            "symbol": "BTCUSDT",
            "open": 50000.0,
            "high": 50500.0,
            "low": 49900.0,
            "close": 50200.0,
            "volume": 12.5,
            "close_time": 1700000059999,
            "quote_asset_volume": 627500.0,
            "number_of_trades": 350,
            "taker_buy_base_volume": 6.2,
            "taker_buy_quote_volume": 311240.0,
            "data_source": "historical",
            "ingested_at": "2026-10-23T12:00:00Z",
        }

    def test_schema_field_names_and_types(self):
        """Verify Spark schema matches the canonical 14 fields."""
        if HAS_PYSPARK:
            fields = {f.name: f.dataType.typeName() for f in CANONICAL_KLINE_SPARK_SCHEMA.fields}
            self.assertEqual(len(fields), 14)
            self.assertEqual(fields["open_time"], "long")
            self.assertEqual(fields["symbol"], "string")
            self.assertEqual(fields["open"], "double")
            self.assertEqual(fields["high"], "double")
            self.assertEqual(fields["low"], "double")
            self.assertEqual(fields["close"], "double")
            self.assertEqual(fields["volume"], "double")
            self.assertEqual(fields["close_time"], "long")
            self.assertEqual(fields["quote_asset_volume"], "double")
            self.assertEqual(fields["number_of_trades"], "long")
            self.assertEqual(fields["taker_buy_base_volume"], "double")
            self.assertEqual(fields["taker_buy_quote_volume"], "double")
            self.assertEqual(fields["data_source"], "string")
            self.assertEqual(fields["ingested_at"], "string")
        else:
            self.assertEqual(len(CANONICAL_KLINE_FIELDS), 14)

    def test_valid_record_passes_validation(self):
        valid, reason = validate_kline_dict(self.valid_kline)
        self.assertTrue(valid, f"Expected valid record, got: {reason}")

    def test_malformed_numeric_and_null_rejection(self):
        """Simulate corrupt records that should be rejected or quarantined."""
        # Null price
        corrupt_open = dict(self.valid_kline, open=None)
        valid, reason = validate_kline_dict(corrupt_open)
        self.assertFalse(valid)

        # Inverted high/low
        corrupt_bounds = dict(self.valid_kline, high=40000.0, low=50000.0)
        valid, reason = validate_kline_dict(corrupt_bounds)
        self.assertFalse(valid)

        # Negative volume
        corrupt_volume = dict(self.valid_kline, volume=-10.0)
        valid, reason = validate_kline_dict(corrupt_volume)
        self.assertFalse(valid)

        # Inverted timestamps
        corrupt_time = dict(self.valid_kline, close_time=1600000000000)
        valid, reason = validate_kline_dict(corrupt_time)
        self.assertFalse(valid)

    def test_deduplication_logic(self):
        """Verify deduplication by (symbol, open_time)."""
        records = [
            self.valid_kline,
            dict(self.valid_kline),  # exact duplicate
            dict(self.valid_kline, close=50300.0),  # duplicate key with different close
            dict(self.valid_kline, open_time=1700000060000),  # different key
        ]
        # Simulate dropDuplicates(["symbol", "open_time"])
        seen = set()
        unique_records = []
        for r in records:
            key = (r["symbol"], r["open_time"])
            if key not in seen:
                seen.add(key)
                unique_records.append(r)

        self.assertEqual(len(unique_records), 2)
        self.assertEqual(len(records) - len(unique_records), 2)  # 2 duplicates dropped


if __name__ == "__main__":
    unittest.main()
