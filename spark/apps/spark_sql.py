"""
==============================================================================
spark/apps/spark_sql.py
Spark SQL Analytical Query Execution over HDFS Parquet Data Lake.

Responsibilities:
1. Load historical and streaming Parquet tables from HDFS.
2. Register temporary views (e.g. 'klines_historical', 'klines_all').
3. Execute analytical queries (OHLCV aggregates, volume leaders, volatility).
4. Save analytical summary tables or display interactive results.
==============================================================================
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_sql_analytics")


def run_analytics(args: argparse.Namespace) -> None:
    builder = SparkSession.builder.appName("CryptoSparkSQLAnalytics")
    if args.spark_master:
        builder = builder.master(args.spark_master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        hdfs_path = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_input_path.lstrip('/')}"
        logger.info("Reading Parquet data lake from HDFS: %s", hdfs_path)

        try:
            df = spark.read.parquet(hdfs_path)
        except Exception as e:
            logger.warning("No data found at HDFS path '%s' (%s). Exiting analytics.", hdfs_path, e)
            return

        df.createOrReplaceTempView("klines_historical")
        logger.info("Registered temporary view 'klines_historical' (%d records)", df.count())

        # Analytical Query 1: Market summary per symbol
        logger.info("--- 1. Market Summary by Symbol ---")
        summary_df = spark.sql("""
            SELECT
                symbol,
                COUNT(*) AS total_candles,
                MIN(open_time) AS earliest_candle,
                MAX(close_time) AS latest_candle,
                ROUND(AVG(close), 4) AS avg_close_price,
                ROUND(MAX(high), 4) AS highest_price,
                ROUND(MIN(low), 4) AS lowest_price,
                ROUND(SUM(volume), 4) AS total_base_volume,
                ROUND(SUM(quote_asset_volume), 2) AS total_quote_volume,
                SUM(number_of_trades) AS total_trades
            FROM klines_historical
            GROUP BY symbol
            ORDER BY total_quote_volume DESC
        """)
        summary_df.show(truncate=False)

        # Analytical Query 2: Daily Aggregated Metrics
        logger.info("--- 2. Daily Aggregates ---")
        daily_df = spark.sql("""
            SELECT
                symbol,
                kline_date,
                FIRST(open) AS open_price,
                MAX(high) AS high_price,
                MIN(low) AS low_price,
                LAST(close) AS close_price,
                ROUND(SUM(volume), 4) AS total_volume,
                ROUND(SUM(quote_asset_volume), 2) AS total_quote_volume,
                SUM(number_of_trades) AS total_trades
            FROM klines_historical
            GROUP BY symbol, kline_date
            ORDER BY kline_date DESC, symbol ASC
        """)
        daily_df.show(20, truncate=False)

    finally:
        spark.stop()
        logger.info("Spark SQL session closed.")


def parse_args():
    parser = argparse.ArgumentParser(description="Spark SQL Crypto Analytics")
    parser.add_argument("--spark-master", default=None)
    parser.add_argument("--hdfs-namenode", default=os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000"))
    parser.add_argument("--hdfs-input-path", default="/data/crypto/historical/klines")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_analytics(args)
