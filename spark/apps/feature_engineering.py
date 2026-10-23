"""
==============================================================================
spark/apps/feature_engineering.py
Spark Feature Engineering Pipeline for Cryptocurrency Price Analysis & ML.

Responsibilities:
1. Load clean historical OHLCV Kline data from HDFS Parquet data lake.
2. Compute time-series technical market indicators using Spark Window functions:
   - Simple Moving Averages (SMA 7, 20, 50)
   - Exponential Moving Average proxies & rolling volatility
   - Bollinger Bands (upper, lower, bandwidth)
   - Log returns and momentum indicators
3. Assemble feature vectors for predictive machine learning workflows.
4. Output feature tables back to HDFS in Parquet format.
==============================================================================
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_feature_engineering")


def run_feature_pipeline(args: argparse.Namespace) -> None:
    builder = SparkSession.builder.appName("CryptoFeatureEngineering")
    if args.spark_master:
        builder = builder.master(args.spark_master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        input_path = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_input_path.lstrip('/')}"
        output_path = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_output_path.lstrip('/')}"
        logger.info("Reading input Parquet lake from: %s", input_path)

        try:
            df = spark.read.parquet(input_path)
        except Exception as e:
            logger.warning("No data found at input path '%s' (%s). Exiting pipeline.", input_path, e)
            return

        logger.info("Computing technical features for %d records...", df.count())

        # Define time window partitioned by symbol, ordered chronologically
        w_symbol = Window.partitionBy("symbol").orderBy("open_time")
        w_7 = w_symbol.rowsBetween(-6, 0)
        w_20 = w_symbol.rowsBetween(-19, 0)
        w_50 = w_symbol.rowsBetween(-49, 0)

        df_features = (
            df
            # Moving averages
            .withColumn("sma_7", F.avg("close").over(w_7))
            .withColumn("sma_20", F.avg("close").over(w_20))
            .withColumn("sma_50", F.avg("close").over(w_50))
            # Rolling standard deviation (for volatility & Bollinger Bands)
            .withColumn("rolling_std_20", F.stddev("close").over(w_20))
            # Bollinger Bands
            .withColumn("bb_upper", F.col("sma_20") + (F.lit(2.0) * F.coalesce(F.col("rolling_std_20"), F.lit(0.0))))
            .withColumn("bb_lower", F.col("sma_20") - (F.lit(2.0) * F.coalesce(F.col("rolling_std_20"), F.lit(0.0))))
            .withColumn("bb_bandwidth", (F.col("bb_upper") - F.col("bb_lower")) / F.col("sma_20"))
            # Price returns and momentum
            .withColumn("prev_close", F.lag("close", 1).over(w_symbol))
            .withColumn("price_return_1m", (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
            # Volume & Trade Intensity
            .withColumn("avg_volume_20", F.avg("volume").over(w_20))
            .withColumn("volume_ratio_20", F.col("volume") / F.coalesce(F.col("avg_volume_20"), F.lit(1.0)))
            .withColumn("feature_timestamp", F.current_timestamp())
        )

        logger.info("Saving engineered feature dataset to HDFS: %s", output_path)
        (
            df_features.write
            .mode("overwrite")
            .format("parquet")
            .partitionBy("symbol")
            .save(output_path)
        )
        logger.info("Feature engineering completed successfully.")

    finally:
        spark.stop()
        logger.info("Spark feature engineering session closed.")


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Technical Feature Engineering")
    parser.add_argument("--spark-master", default=None)
    parser.add_argument("--hdfs-namenode", default=os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000"))
    parser.add_argument("--hdfs-input-path", default="/data/crypto/historical/klines")
    parser.add_argument("--hdfs-output-path", default="/data/crypto/features/klines")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_feature_pipeline(args)
