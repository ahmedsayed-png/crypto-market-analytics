#!/usr/bin/env bash
# ==============================================================================
# Kafka Topic Creation & Management Script for Crypto Market Analytics
# Idempotent topic provisioning with retention, partition, and replication control.
# ==============================================================================
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

echo "Waiting for Kafka broker at ${BOOTSTRAP}..."
MAX_ATTEMPTS=30
ATTEMPT=0
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ "$ATTEMPT" -ge "$MAX_ATTEMPTS" ]; then
        echo "Error: Kafka broker at ${BOOTSTRAP} did not become available after ${MAX_ATTEMPTS} attempts."
        exit 1
    fi
    echo "  (attempt ${ATTEMPT}/${MAX_ATTEMPTS} - broker not ready, retrying in 2s...)"
    sleep 2
done

echo "Kafka broker is responsive. Provisioning topics..."

# Helper function to create or ensure topic configuration
# Args: topic_name partitions replication_factor retention_ms cleanup_policy
provision_topic() {
    local topic="$1"
    local partitions="$2"
    local replication="$3"
    local retention_ms="$4"
    local cleanup_policy="${5:-delete}"

    echo "--- Provisioning topic: '${topic}' ---"

    # Check if topic already exists
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list | grep -qx "${topic}"; then
        echo "  Topic '${topic}' already exists."
        # Ensure configuration is updated idempotently
        /opt/kafka/bin/kafka-configs.sh --bootstrap-server "${BOOTSTRAP}" \
            --entity-type topics \
            --entity-name "${topic}" \
            --alter \
            --add-config retention.ms="${retention_ms}",cleanup.policy="${cleanup_policy}" >/dev/null 2>&1 || true
        echo "  Updated/verified config for existing topic '${topic}'."
    else
        echo "  Creating topic '${topic}' (partitions: ${partitions}, replication: ${replication}, retention.ms: ${retention_ms})..."
        /opt/kafka/bin/kafka-topics.sh --create \
            --topic "${topic}" \
            --bootstrap-server "${BOOTSTRAP}" \
            --partitions "${partitions}" \
            --replication-factor "${replication}" \
            --config retention.ms="${retention_ms}" \
            --config cleanup.policy="${cleanup_policy}"
        echo "  Topic '${topic}' created successfully."
    fi
}

# 1. Primary Dedicated Historical Klines Topic
# High retention (7 days: 604800000 ms) for replay and batch re-processing
provision_topic "crypto_klines_historical" 3 1 604800000 "delete"

# 2. Primary Dedicated Real-Time Streaming Klines Topic
# Low-to-moderate retention (24 hours: 86400000 ms) for real-time buffer & tumbling windows
provision_topic "crypto_klines_streaming" 3 1 86400000 "delete"

# 3. Auxiliary Proposal Topics (Maintained for full backward compatibility)
provision_topic "market_klines" 3 1 259200000 "delete"         # 3 days
provision_topic "market_trades" 3 1 86400000 "delete"          # 1 day
provision_topic "order_book_events" 3 1 43200000 "delete"      # 12 hours
provision_topic "ticker_events" 3 1 86400000 "delete"          # 1 day
provision_topic "alerts_events" 3 1 604800000 "delete"         # 7 days

echo "=============================================================================="
echo "Topic provisioning complete. Current active topics on ${BOOTSTRAP}:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
echo "=============================================================================="
