#!/usr/bin/env bash
# ==============================================================================
# spark/run_streaming.sh
# Entrypoint script for continuous Spark Structured Streaming service.
# ==============================================================================
set -euo pipefail

echo "=============================================================================="
echo " Starting Spark Structured Streaming Service"
echo "=============================================================================="

# 1. Dependency Readiness Checks using Python socket
echo "Verifying cluster dependencies..."

python3 - <<'PYEOF'
import os
import socket
import sys
import time

def wait_for(host, port, name, max_attempts=60):
    print(f"Waiting for {name} ({host}:{port})...")
    for attempt in range(1, max_attempts + 1):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((host, port))
            s.close()
            print(f"✓ {name} ({host}:{port}) is reachable.")
            return
        except Exception:
            time.sleep(2)
    print(f"Error: {name} ({host}:{port}) did not become reachable in {max_attempts*2}s.")
    sys.exit(1)

wait_for(os.environ.get("KAFKA_HOST", "kafka"), int(os.environ.get("KAFKA_PORT", 9092)), "Apache Kafka")
wait_for(os.environ.get("HDFS_HOST", "namenode"), int(os.environ.get("HDFS_RPC_PORT", 9000)), "HDFS NameNode")
wait_for(os.environ.get("SPARK_MASTER_HOST", "spark-master"), int(os.environ.get("SPARK_MASTER_PORT", 7077)), "Spark Master")
PYEOF

# 2. Configuration resolution
SPARK_MASTER_URL="${SPARK_MASTER:-spark://spark-master:7077}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC_STREAMING:-crypto_klines_streaming}"
HDFS_NN="${HDFS_NAMENODE:-hdfs://namenode:9000}"
HDFS_OUTPUT="${HDFS_STREAMING_OUTPUT:-/data/crypto/streaming/klines}"
HDFS_CHECKPOINT="${HDFS_STREAMING_CHECKPOINT:-/data/crypto/checkpoints/streaming_klines_continuous}"
TRIGGER_INTERVAL="${STREAMING_TRIGGER_INTERVAL:-10 seconds}"
STARTING_OFFSETS="${STREAMING_STARTING_OFFSETS:-latest}"

EXTRA_ARGS=()
if [ "${ENABLE_POSTGRES_STREAMING_SINK:-false}" = "true" ] || [ "${ENABLE_POSTGRES_STREAMING_SINK:-false}" = "1" ]; then
    EXTRA_ARGS+=(
        "--enable-postgres"
        "--postgres-host" "${POSTGRES_DW_HOST:-postgres-dw}"
        "--postgres-port" "${POSTGRES_DW_PORT:-5432}"
        "--postgres-db" "${POSTGRES_DW_DB:-crypto_dw}"
        "--postgres-user" "${POSTGRES_DW_USER:-crypto_admin}"
        "--postgres-password" "${POSTGRES_DW_PASSWORD:-crypto_secure_password_123}"
    )
fi

echo "Configuration:"
echo "  Spark Master:        ${SPARK_MASTER_URL}"
echo "  Kafka Topic:         ${KAFKA_TOPIC} (${KAFKA_BOOTSTRAP_SERVERS})"
echo "  Starting Offsets:    ${STARTING_OFFSETS}"
echo "  Trigger Interval:    ${TRIGGER_INTERVAL}"
echo "  HDFS Output Path:    ${HDFS_OUTPUT}"
echo "  HDFS Checkpoint:     ${HDFS_CHECKPOINT}"
echo "  PostgreSQL Sink:     ${ENABLE_POSTGRES_STREAMING_SINK:-false}"
echo "=============================================================================="

# 3. Launch PySpark Structured Streaming Job
exec spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.driver.host="${SPARK_DRIVER_HOST:-spark-streaming}" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.port="${SPARK_DRIVER_PORT:-7078}" \
    --conf spark.blockManager.port="${SPARK_BLOCK_MANAGER_PORT:-7079}" \
    --conf spark.ui.port=4040 \
    /opt/spark-apps/streaming_processing.py \
        --app-name "CryptoStreamingJob" \
        --kafka-bootstrap "${KAFKA_BOOTSTRAP_SERVERS}" \
        --kafka-topic "${KAFKA_TOPIC}" \
        --starting-offsets "${STARTING_OFFSETS}" \
        --trigger-interval "${TRIGGER_INTERVAL}" \
        --hdfs-namenode "${HDFS_NN}" \
        --hdfs-output-path "${HDFS_OUTPUT}" \
        --hdfs-checkpoint-path "${HDFS_CHECKPOINT}" \
        "${EXTRA_ARGS[@]}"
