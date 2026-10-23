"""
==============================================================================
spark/apps/streaming_processing.py
Apache Spark Structured Streaming Pipeline for Crypto Market Analytics.

Responsibilities:
1. Consume live streaming Kline events from Kafka topic 'crypto_klines_streaming'.
2. Watermarking and event-time parsing using canonical Kline schema.
3. Validate and filter corrupt / non-compliant records.
4. Structured Parquet sink to separate HDFS streaming path with Snappy compression.
5. Persistent checkpointing in HDFS for fault tolerance and recovery.
6. Optional micro-batch sink to PostgreSQL Data Warehouse ('fact_klines_min').
7. Bounded smoke-test mode (Trigger.AvailableNow / timeout) for safe testing.
==============================================================================
"""

import argparse
import logging
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_streaming_processing")

CANONICAL_KLINE_SPARK_SCHEMA = StructType([
    StructField("open_time", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("close_time", LongType(), True),
    StructField("quote_asset_volume", DoubleType(), True),
    StructField("number_of_trades", LongType(), True),
    StructField("taker_buy_base_volume", DoubleType(), True),
    StructField("taker_buy_quote_volume", DoubleType(), True),
    StructField("data_source", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


def build_spark_session(app_name: str, master: Optional[str] = None) -> SparkSession:
    """Build SparkSession configured for streaming operations."""
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    builder = (
        builder
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.checkpointLocation.checkpointDir", "/tmp/checkpoints")
    )
    return builder.getOrCreate()


def run_streaming_pipeline(args: argparse.Namespace) -> None:
    """Initialize and run Spark Structured Streaming job."""
    logger.info("Initializing Spark Streaming Session: %s", args.app_name)
    spark = build_spark_session(args.app_name, args.spark_master)
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info(
            "Subscribing to streaming topic '%s' on broker '%s'",
            args.kafka_topic,
            args.kafka_bootstrap,
        )

        # 1. Structured Streaming Read from Kafka
        df_stream_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("subscribe", args.kafka_topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        # 2. Parse Value with Schema
        df_parsed = df_stream_raw.select(
            F.col("key").cast("string").alias("symbol_key"),
            F.from_json(F.col("value").cast("string"), CANONICAL_KLINE_SPARK_SCHEMA).alias("kline"),
        )

        # 3. Filter Valid Messages
        is_valid = (
            F.col("kline.symbol").isNotNull()
            & F.col("kline.open_time").isNotNull()
            & (F.col("kline.open_time") > 0)
            & F.col("kline.close").isNotNull()
            & (F.col("kline.close") >= 0)
            & F.col("kline.volume").isNotNull()
            & (F.col("kline.volume") >= 0)
        )
        df_valid_stream = df_parsed.filter(is_valid)

        # 4. Standardize Columns and Event Time
        df_transformed = df_valid_stream.select(
            F.upper(F.trim(F.col("kline.symbol"))).alias("symbol"),
            F.col("kline.open_time"),
            (F.col("kline.open_time") / 1000.0).cast(TimestampType()).alias("open_time_dt"),
            F.col("kline.open"),
            F.col("kline.high"),
            F.col("kline.low"),
            F.col("kline.close"),
            F.col("kline.volume"),
            F.col("kline.close_time"),
            (F.col("kline.close_time") / 1000.0).cast(TimestampType()).alias("close_time_dt"),
            F.col("kline.quote_asset_volume"),
            F.col("kline.number_of_trades"),
            F.col("kline.taker_buy_base_volume"),
            F.col("kline.taker_buy_quote_volume"),
            F.lit("realtime").alias("data_source"),
            F.current_timestamp().alias("processed_at"),
        ).withColumn("kline_date", F.to_date(F.col("open_time_dt")))

        # Define 10-minute watermark on open_time_dt for late arrival handling
        df_watermarked = df_transformed.withWatermark("open_time_dt", "10 minutes")

        hdfs_sink_path = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_output_path.lstrip('/')}"
        checkpoint_path = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_checkpoint_path.lstrip('/')}"

        logger.info("Configuring streaming sink to HDFS: %s (checkpoint: %s)", hdfs_sink_path, checkpoint_path)

        def write_micro_batch(batch_df, batch_id):
            """Micro-batch handler writing to HDFS Parquet and optionally PostgreSQL DW."""
            record_count = batch_df.count()
            logger.info("Processing streaming micro-batch #%d: %d records", batch_id, record_count)

            if record_count == 0:
                return

            # Deduplicate within batch
            batch_unique = batch_df.dropDuplicates(["symbol", "open_time"])

            # 1. Write Parquet to HDFS
            (
                batch_unique.write
                .mode("append")
                .format("parquet")
                .partitionBy("symbol", "kline_date")
                .save(hdfs_sink_path)
            )

            # 2. Write to PostgreSQL DW if enabled
            if args.enable_postgres:
                pg_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
                pg_table = "fact_klines_min"
                pg_batch = batch_unique.select(
                    F.col("open_time_dt").alias("open_time"),
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    F.col("quote_asset_volume").alias("quote_volume"),
                    "number_of_trades",
                    F.col("taker_buy_base_volume").alias("taker_buy_volume"),
                    "taker_buy_quote_volume",
                    "processed_at",
                )

                try:
                    (
                        pg_batch.write
                        .format("jdbc")
                        .option("url", pg_url)
                        .option("dbtable", pg_table)
                        .option("user", args.postgres_user)
                        .option("password", args.postgres_password)
                        .option("driver", "org.postgresql.Driver")
                        .mode("append")
                        .save()
                    )
                    logger.info("Micro-batch #%d successfully sinked to PostgreSQL table '%s'", batch_id, pg_table)
                except Exception as exc:
                    logger.error("PostgreSQL sink error in micro-batch #%d: %s", batch_id, exc)

        # 5. Start Streaming Query
        writer = (
            df_watermarked.writeStream
            .foreachBatch(write_micro_batch)
            .option("checkpointLocation", checkpoint_path)
        )

        if args.smoke_test:
            # Bounded smoke test mode: processes all available data and stops
            logger.info("Executing streaming pipeline in bounded SMOKE TEST mode (Trigger.AvailableNow)...")
            query = writer.trigger(availableNow=True).start()
            query.awaitTermination()
            logger.info("Bounded smoke test completed successfully.")
        else:
            # Continuous streaming execution with specified trigger interval
            logger.info("Starting continuous streaming query (trigger: %s)...", args.trigger_interval)
            query = writer.trigger(processingTime=args.trigger_interval).start()
            if args.timeout_seconds:
                logger.info("Streaming query will terminate automatically after %d seconds.", args.timeout_seconds)
                query.awaitTermination(timeout=args.timeout_seconds)
                query.stop()
            else:
                query.awaitTermination()

    finally:
        spark.stop()
        logger.info("Streaming Spark session cleanly closed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Real-Time Kline Job")

    parser.add_argument("--app-name", default="CryptoStreamingJob", help="Application name")
    parser.add_argument("--spark-master", default=None, help="Spark Master URL")

    # Kafka
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap server",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.environ.get("KAFKA_TOPIC_STREAMING_KLINES", "crypto_klines_streaming"),
        help="Kafka real-time streaming topic",
    )
    parser.add_argument("--starting-offsets", default="latest", help="Kafka starting offsets (latest or earliest)")

    # HDFS
    parser.add_argument(
        "--hdfs-namenode",
        default=os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000"),
        help="HDFS NameNode URL",
    )
    parser.add_argument(
        "--hdfs-output-path",
        default="/data/crypto/streaming/klines",
        help="HDFS directory for real-time Parquet stream",
    )
    parser.add_argument(
        "--hdfs-checkpoint-path",
        default="/data/crypto/checkpoints/streaming_klines",
        help="HDFS checkpoint directory",
    )

    # Trigger & Execution Mode
    parser.add_argument("--trigger-interval", default="10 seconds", help="Micro-batch processing interval")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Run in bounded mode (AvailableNow) to test without keeping query open indefinitely",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="Optional timeout in seconds to gracefully terminate streaming query",
    )

    # PostgreSQL DW
    parser.add_argument("--postgres-host", default=os.environ.get("POSTGRES_DW_HOST", "postgres-dw"))
    parser.add_argument("--postgres-port", default=os.environ.get("POSTGRES_DW_PORT", "5432"))
    parser.add_argument("--postgres-db", default=os.environ.get("POSTGRES_DW_DB", "crypto_dw"))
    parser.add_argument("--postgres-user", default=os.environ.get("POSTGRES_DW_USER", "crypto_admin"))
    parser.add_argument("--postgres-password", default=os.environ.get("POSTGRES_DW_PASSWORD", "crypto_secure_password_123"))
    parser.add_argument(
        "--enable-postgres",
        action="store_true",
        default=os.environ.get("ENABLE_POSTGRES_STREAMING_SINK", "false").lower() in ("true", "1", "yes"),
        help="Whether to write streaming micro-batches to PostgreSQL DW",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_streaming_pipeline(args)
