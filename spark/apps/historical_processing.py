"""
==============================================================================
spark/apps/historical_processing.py
Apache Spark Batch Ingestion & Transformation Job for Crypto Market Analytics.

Responsibilities:
1. Connect to Kafka topic 'crypto_klines_historical'.
2. Bounded batch consumption with explicit, reproducible partition offset boundaries
   and persistent offset state checkpoints for reliable multi-run resumption.
3. Parse canonical 12-field Kline JSON schema.
4. Quarantine malformed records to HDFS rejected path.
5. Deduplicate valid records by (symbol, open_time).
6. Persist structured Parquet data lake partitioned by (symbol, kline_date) to HDFS idempotently.
7. Idempotently sink non-duplicate records to PostgreSQL Data Warehouse ('fact_klines_min')
   with fail-closed error handling (no unsafe fallbacks).
8. Mask sensitive credentials in logs and report comprehensive processing metrics.
==============================================================================
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_historical_processing")

# Canonical 12-field Kline schema + lineage metadata
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
    """Instantiate and configure the PySpark SparkSession."""
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    # Optimization and HDFS settings
    builder = (
        builder
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )
    return builder.getOrCreate()


def sanitize_args_for_logging(args: argparse.Namespace) -> Dict[str, Any]:
    """Return a dictionary of arguments with sensitive credentials masked."""
    sanitized = {}
    for k, v in vars(args).items():
        if any(secret_term in k.lower() for secret_term in ("password", "secret", "token", "key")):
            sanitized[k] = "******" if v else None
        else:
            sanitized[k] = v
    return sanitized


def query_kafka_partition_offsets(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Query Kafka broker for current beginning (earliest) and end (latest) offsets
    per partition using the Kafka Consumer loaded on the Spark driver classpath.
    Returns: (beginning_offsets_dict, end_offsets_dict)
    """
    gw = spark._jvm
    props = gw.java.util.Properties()
    props.put("bootstrap.servers", bootstrap_servers)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("enable.auto.commit", "false")
    props.put("group.id", f"spark-offset-probe-{os.getpid()}")

    consumer = gw.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    try:
        partition_infos = consumer.partitionsFor(topic)
        if partition_infos is None or partition_infos.isEmpty():
            raise RuntimeError(f"Topic '{topic}' does not exist or has no active partitions on broker {bootstrap_servers}")

        topic_partitions = gw.java.util.ArrayList()
        for info in partition_infos:
            topic_partitions.add(gw.org.apache.kafka.common.TopicPartition(topic, info.partition()))

        beg_map = consumer.beginningOffsets(topic_partitions)
        end_map = consumer.endOffsets(topic_partitions)

        earliest_offsets = {}
        latest_offsets = {}
        for tp in topic_partitions:
            p_str = str(tp.partition())
            earliest_offsets[p_str] = int(beg_map.get(tp))
            latest_offsets[p_str] = int(end_map.get(tp))

        return earliest_offsets, latest_offsets
    finally:
        consumer.close()


def load_offset_state(
    spark: SparkSession,
    hdfs_namenode: str,
    state_path: str,
    topic: str,
) -> Optional[Dict[str, int]]:
    """Load previously persisted offset state from HDFS if present."""
    try:
        hdfs_uri = spark._jvm.java.net.URI(hdfs_namenode)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdfs_uri, spark._jsc.hadoopConfiguration())
        target_path = spark._jvm.org.apache.hadoop.fs.Path(state_path)
        if not fs.exists(target_path):
            return None

        in_stream = fs.open(target_path)
        reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(in_stream))
        content = []
        line = reader.readLine()
        while line is not None:
            content.append(str(line))
            line = reader.readLine()
        in_stream.close()
        full_str = "\n".join(content)
        parsed = json.loads(full_str)
        offsets = parsed.get(topic, parsed)
        return {str(k): int(v) for k, v in offsets.items()}
    except Exception as exc:
        logger.warning("Could not read offset state checkpoint from HDFS (%s): %s", state_path, exc)
        return None


def save_offset_state(
    spark: SparkSession,
    hdfs_namenode: str,
    state_path: str,
    end_offsets_json: str,
) -> None:
    """Save the last committed batch ending offsets to HDFS for auto-resumption."""
    try:
        hdfs_uri = spark._jvm.java.net.URI(hdfs_namenode)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdfs_uri, spark._jsc.hadoopConfiguration())
        target_path = spark._jvm.org.apache.hadoop.fs.Path(state_path)
        parent_path = target_path.getParent()
        if not fs.exists(parent_path):
            fs.mkdirs(parent_path)

        out_stream = fs.create(target_path, True)  # True = overwrite
        out_stream.write(bytearray(end_offsets_json.encode("utf-8")))
        out_stream.close()
        logger.info("Successfully persisted offset state checkpoint to HDFS: %s", state_path)
    except Exception as exc:
        logger.warning("Could not persist offset state checkpoint to HDFS (%s): %s", state_path, exc)


def determine_batch_offsets(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets_arg: str,
    ending_offsets_arg: Optional[str],
    max_records_per_partition: Optional[int] = None,
    allow_unbounded: bool = False,
    hdfs_namenode: Optional[str] = None,
    offset_state_path: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Determine explicit, reproducible starting and ending offset JSON strings
    for Spark Kafka batch consumption, enforcing bounded reads.
    Returns: (starting_offsets_json, ending_offsets_json)
    """
    # 1. Query current broker offsets for deterministic snapshots
    earliest_map, latest_map = query_kafka_partition_offsets(spark, bootstrap_servers, topic)
    logger.info("Kafka cluster offset snapshot for '%s': earliest=%s, latest=%s", topic, earliest_map, latest_map)

    # 2. Resolve starting offsets
    resolved_start = {}
    if starting_offsets_arg == "auto":
        saved_offsets = load_offset_state(spark, hdfs_namenode, offset_state_path, topic) if (hdfs_namenode and offset_state_path) else None
        if saved_offsets:
            logger.info("Resuming from persisted Kafka offset state on HDFS: %s", saved_offsets)
            resolved_start = saved_offsets
        else:
            logger.info("No prior offset state found on HDFS. Defaulting starting offsets to earliest.")
            resolved_start = earliest_map
    elif starting_offsets_arg == "earliest":
        resolved_start = earliest_map
    elif starting_offsets_arg == "latest":
        resolved_start = latest_map
    else:
        try:
            parsed = json.loads(starting_offsets_arg)
            resolved_start = parsed.get(topic, parsed)
            resolved_start = {str(k): int(v) for k, v in resolved_start.items()}
        except Exception as e:
            raise ValueError(f"Invalid starting_offsets specification '{starting_offsets_arg}': {e}")

    # 3. Resolve ending offsets
    resolved_end = {}
    if ending_offsets_arg is not None and ending_offsets_arg != "latest":
        try:
            parsed = json.loads(ending_offsets_arg)
            resolved_end = parsed.get(topic, parsed)
            resolved_end = {str(k): int(v) for k, v in resolved_end.items()}
        except Exception as e:
            raise ValueError(f"Invalid ending_offsets specification '{ending_offsets_arg}': {e}")
    else:
        # Resolve ending offsets deterministically from latest snapshot
        for p, end_offset in latest_map.items():
            start_offset = resolved_start.get(p, earliest_map.get(p, 0))
            if max_records_per_partition is not None and max_records_per_partition > 0:
                capped_end = min(end_offset, start_offset + max_records_per_partition)
                resolved_end[p] = capped_end
            elif allow_unbounded:
                resolved_end[p] = end_offset
            else:
                # Default: snapshot of current broker latest offset
                resolved_end[p] = end_offset

    # Validate boundaries
    total_batch_capacity = 0
    for p in latest_map.keys():
        s = resolved_start.get(p, 0)
        e = resolved_end.get(p, s)
        if e < s:
            logger.warning("Partition %s ending offset (%d) < starting offset (%d). Adjusting ending to starting.", p, e, s)
            resolved_end[p] = s
            e = s
        records_in_p = e - s
        total_batch_capacity += records_in_p
        logger.info("Partition %s batch boundary: [%d -> %d] (%d records)", p, s, e, records_in_p)

    logger.info("Computed batch offset boundary: total %d records expected across all partitions", total_batch_capacity)

    start_json = json.dumps({topic: resolved_start})
    end_json = json.dumps({topic: resolved_end})
    return start_json, end_json


def run_batch_job(args: argparse.Namespace) -> Dict[str, Any]:
    """Execute the end-to-end historical batch processing job."""
    logger.info("Initializing Spark session: app_name=%s, master=%s", args.app_name, args.spark_master)
    spark = build_spark_session(args.app_name, args.spark_master)
    spark.sparkContext.setLogLevel("WARN")

    metrics = {
        "kafka_records_read": 0,
        "valid_records": 0,
        "rejected_records": 0,
        "unique_records": 0,
        "duplicates_dropped": 0,
        "hdfs_records_written": 0,
        "postgres_records_written": 0,
    }

    try:
        # Determine explicit, reproducible batch offset boundaries
        start_offsets_json, end_offsets_json = determine_batch_offsets(
            spark=spark,
            bootstrap_servers=args.kafka_bootstrap,
            topic=args.kafka_topic,
            starting_offsets_arg=args.starting_offsets,
            ending_offsets_arg=args.ending_offsets,
            max_records_per_partition=args.max_records_per_partition,
            allow_unbounded=args.allow_unbounded,
            hdfs_namenode=args.hdfs_namenode,
            offset_state_path=args.offset_state_path,
        )

        logger.info(
            "Reading Kafka topic '%s' with explicit bounded offsets: starting=%s, ending=%s",
            args.kafka_topic,
            start_offsets_json,
            end_offsets_json,
        )

        # 1. Read raw stream from Kafka with explicit JSON offset boundaries
        df_kafka = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("subscribe", args.kafka_topic)
            .option("startingOffsets", start_offsets_json)
            .option("endingOffsets", end_offsets_json)
            .load()
        )

        total_read = df_kafka.count()
        metrics["kafka_records_read"] = total_read
        logger.info("Total raw messages read from Kafka: %d", total_read)

        if total_read == 0:
            logger.warning("No messages found in topic '%s' for specified offsets. Exiting.", args.kafka_topic)
            return metrics

        # 2. Extract string value and parse JSON
        df_parsed = df_kafka.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("raw_value"),
            F.from_json(F.col("value").cast("string"), CANONICAL_KLINE_SPARK_SCHEMA).alias("kline"),
        )

        # 3. Validation Rules
        is_valid = (
            F.col("kline.symbol").isNotNull()
            & (F.length(F.trim(F.col("kline.symbol"))) > 0)
            & F.col("kline.open_time").isNotNull()
            & (F.col("kline.open_time") > 0)
            & F.col("kline.close_time").isNotNull()
            & (F.col("kline.close_time") >= F.col("kline.open_time"))
            & F.col("kline.open").isNotNull()
            & (F.col("kline.open") >= 0)
            & F.col("kline.high").isNotNull()
            & (F.col("kline.high") >= F.col("kline.low"))
            & F.col("kline.low").isNotNull()
            & (F.col("kline.low") >= 0)
            & F.col("kline.close").isNotNull()
            & (F.col("kline.close") >= 0)
            & F.col("kline.volume").isNotNull()
            & (F.col("kline.volume") >= 0)
        )

        df_valid_raw = df_parsed.filter(is_valid)
        df_rejected = df_parsed.filter(~is_valid)

        valid_count = df_valid_raw.count()
        rejected_count = df_rejected.count()
        metrics["valid_records"] = valid_count
        metrics["rejected_records"] = rejected_count

        logger.info("Validation breakdown: %d valid, %d rejected", valid_count, rejected_count)

        # 4. Quarantine Rejected / Malformed Records to HDFS
        if rejected_count > 0:
            rejected_output = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_rejected_path.lstrip('/')}"
            logger.warning("Quarantining %d malformed records to HDFS: %s", rejected_count, rejected_output)
            df_rejected_out = df_rejected.select(
                "kafka_key", "topic", "partition", "offset", "kafka_timestamp", "raw_value"
            ).withColumn("quarantined_at", F.current_timestamp())
            df_rejected_out.write.mode("append").format("json").save(rejected_output)

        if valid_count == 0:
            logger.warning("No valid records remaining after validation. Stopping pipeline.")
            return metrics

        # 5. Extract Valid Fields and Normalize Types
        df_valid = df_valid_raw.select(
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
            F.coalesce(F.col("kline.data_source"), F.lit("historical")).alias("data_source"),
            F.current_timestamp().alias("processed_at"),
        ).withColumn("kline_date", F.to_date(F.col("open_time_dt")))

        # 6. Deduplicate by primary key (symbol, open_time) within the current batch
        df_unique = df_valid.dropDuplicates(["symbol", "open_time"])
        unique_count = df_unique.count()
        duplicates_dropped = valid_count - unique_count

        metrics["unique_records"] = unique_count
        metrics["duplicates_dropped"] = duplicates_dropped
        logger.info("Batch deduplication complete: %d unique records, %d duplicates dropped", unique_count, duplicates_dropped)

        # 7. Write Structured Parquet to HDFS Data Lake (Idempotent Partition Handling)
        hdfs_full_path = f"{args.hdfs_namenode.rstrip('/')}/{args.hdfs_output_path.lstrip('/')}"
        logger.info("Evaluating HDFS Parquet data lake at: %s", hdfs_full_path)

        # Check whether target HDFS path already contains existing Parquet data to prevent file duplication
        df_hdfs_to_write = df_unique
        try:
            hdfs_uri = spark._jvm.java.net.URI(args.hdfs_namenode)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdfs_uri, spark._jsc.hadoopConfiguration())
            output_hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(args.hdfs_output_path)

            if fs.exists(output_hdfs_path):
                try:
                    df_existing_hdfs = (
                        spark.read.format("parquet")
                        .load(hdfs_full_path)
                        .select("symbol", "open_time")
                        .dropDuplicates(["symbol", "open_time"])
                    )
                    # Filter out records already stored in HDFS Parquet data lake
                    df_hdfs_to_write = df_unique.join(
                        df_existing_hdfs,
                        on=["symbol", "open_time"],
                        how="left_anti",
                    )
                except Exception as read_err:
                    logger.info(
                        "HDFS output directory exists but contains no readable Parquet dataset (%s). Treating as initial write.",
                        read_err,
                    )
                    df_hdfs_to_write = df_unique
        except Exception as fs_err:
            logger.error("HDFS filesystem inspection error for '%s': %s", hdfs_full_path, fs_err)
            raise RuntimeError(f"Aborting job: unable to inspect HDFS path '{hdfs_full_path}': {fs_err}") from fs_err

        hdfs_records_to_write = df_hdfs_to_write.count()
        logger.info(
            "Identified %d net-new records to write to HDFS Parquet (%d already exist in lake)",
            hdfs_records_to_write,
            unique_count - hdfs_records_to_write,
        )

        if hdfs_records_to_write > 0:
            (
                df_hdfs_to_write.write
                .mode("append")
                .format("parquet")
                .partitionBy("symbol", "kline_date")
                .save(hdfs_full_path)
            )
            metrics["hdfs_records_written"] = hdfs_records_to_write
            logger.info("Successfully appended %d net-new records to HDFS Parquet lake.", hdfs_records_to_write)
        else:
            metrics["hdfs_records_written"] = 0
            logger.info("All records already exist in HDFS data lake. 0 new files written (idempotent run).")

        # 8. PostgreSQL Data Warehouse Sink (Fail-Closed Idempotent Anti-Join)
        if args.enable_postgres:
            pg_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
            pg_table = "fact_klines_min"
            logger.info("Preparing PostgreSQL DW sink to %s (table: %s)", pg_url, pg_table)

            # Map DataFrame fields to match fact_klines_min DDL exactly:
            # open_time (timestamp), symbol, open, high, low, close, volume, quote_volume,
            # number_of_trades, taker_buy_volume, taker_buy_quote_volume, processed_at
            df_pg_source = df_unique.select(
                F.col("open_time_dt").alias("open_time"),
                F.col("symbol"),
                F.col("open"),
                F.col("high"),
                F.col("low"),
                F.col("close"),
                F.col("volume"),
                F.col("quote_asset_volume").alias("quote_volume"),
                F.col("number_of_trades"),
                F.col("taker_buy_base_volume").alias("taker_buy_volume"),
                F.col("taker_buy_quote_volume").alias("taker_buy_quote_volume"),
                F.col("processed_at"),
            )

            # Idempotency check: Read existing keys from fact_klines_min to avoid duplicate insertion on rerun.
            # Fail-closed error handling: abort the job if the existing keys cannot be queried.
            try:
                existing_keys_df = (
                    spark.read.format("jdbc")
                    .option("url", pg_url)
                    .option("dbtable", f"(SELECT open_time, symbol FROM {pg_table}) AS existing_keys")
                    .option("user", args.postgres_user)
                    .option("password", args.postgres_password)
                    .option("driver", "org.postgresql.Driver")
                    .load()
                )

                # Filter out records already present in DW
                df_to_insert = df_pg_source.join(
                    existing_keys_df,
                    on=["open_time", "symbol"],
                    how="left_anti",
                )
            except Exception as e:
                logger.error(
                    "Database query failure while reading existing keys from PostgreSQL '%s' for anti-join: %s",
                    pg_table,
                    e,
                )
                raise RuntimeError(
                    f"Aborting job to prevent duplicate PostgreSQL insertion: unable to verify existing keys from '{pg_table}': {e}"
                ) from e

            new_insert_count = df_to_insert.count()
            logger.info("Identified %d new records to insert into PostgreSQL '%s'", new_insert_count, pg_table)

            if new_insert_count > 0:
                (
                    df_to_insert.write
                    .format("jdbc")
                    .option("url", pg_url)
                    .option("dbtable", pg_table)
                    .option("user", args.postgres_user)
                    .option("password", args.postgres_password)
                    .option("driver", "org.postgresql.Driver")
                    .mode("append")
                    .save()
                )
                metrics["postgres_records_written"] = new_insert_count
                logger.info("Successfully appended %d records to PostgreSQL DW.", new_insert_count)
            else:
                logger.info("All records already exist in PostgreSQL DW. 0 inserted (idempotent run).")

        # 9. Persist Offset State Checkpoint to HDFS for Reliable Resumption
        if args.offset_state_path and end_offsets_json:
            save_offset_state(
                spark=spark,
                hdfs_namenode=args.hdfs_namenode,
                state_path=args.offset_state_path,
                end_offsets_json=end_offsets_json,
            )

    finally:
        spark.stop()
        logger.info("Spark session cleanly terminated.")

    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Historical Kline Batch Processing")

    parser.add_argument("--app-name", default="CryptoHistoricalBatchJob", help="Spark Application Name")
    parser.add_argument("--spark-master", default=None, help="Spark Master URL (default: spark default / local[*])")

    # Kafka Options
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap server",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.environ.get("KAFKA_TOPIC_HISTORICAL_KLINES", "crypto_klines_historical"),
        help="Kafka source topic",
    )
    parser.add_argument(
        "--starting-offsets",
        default="auto",
        help="Kafka starting offsets ('auto' to resume from HDFS checkpoint, 'earliest', 'latest', or JSON)",
    )
    parser.add_argument("--ending-offsets", default=None, help="Kafka ending offsets (latest, JSON, or None for latest snapshot)")
    parser.add_argument(
        "--max-records-per-partition",
        type=int,
        default=None,
        help="Maximum records to consume per Kafka partition (enforces bounded batch boundary)",
    )
    parser.add_argument(
        "--allow-unbounded",
        action="store_true",
        default=False,
        help="Explicitly permit consuming all records up to current latest offset without partition caps",
    )

    # HDFS Options
    parser.add_argument(
        "--hdfs-namenode",
        default=os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000"),
        help="HDFS NameNode URI",
    )
    parser.add_argument(
        "--hdfs-output-path",
        default=os.environ.get("HDFS_HISTORICAL_OUTPUT", "/data/crypto/historical/klines"),
        help="HDFS directory for valid Parquet output",
    )
    parser.add_argument(
        "--hdfs-rejected-path",
        default="/data/crypto/historical/rejected",
        help="HDFS directory for quarantined malformed records",
    )
    parser.add_argument(
        "--offset-state-path",
        default=os.environ.get("HDFS_OFFSET_STATE_PATH", "/data/crypto/checkpoints/historical_batch/offsets.json"),
        help="HDFS path to persist and load Kafka partition offset state for auto-resumption",
    )

    # PostgreSQL DW Options
    parser.add_argument("--postgres-host", default=os.environ.get("POSTGRES_DW_HOST", "postgres-dw"))
    parser.add_argument("--postgres-port", default=os.environ.get("POSTGRES_DW_PORT", "5432"))
    parser.add_argument("--postgres-db", default=os.environ.get("POSTGRES_DW_DB", "crypto_dw"))
    parser.add_argument("--postgres-user", default=os.environ.get("POSTGRES_DW_USER", "crypto_admin"))
    parser.add_argument("--postgres-password", default=os.environ.get("POSTGRES_DW_PASSWORD", "crypto_secure_password_123"))
    parser.add_argument(
        "--enable-postgres",
        action="store_true",
        default=os.environ.get("ENABLE_POSTGRES_SINK", "true").lower() in ("true", "1", "yes"),
        help="Whether to write validated Kline records to PostgreSQL DW",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logger.info("Starting Spark Historical Batch Processing with arguments: %s", sanitize_args_for_logging(args))
    results = run_batch_job(args)
    logger.info("Job execution summary: %s", results)
