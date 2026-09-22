#!/usr/bin/env bash
# ==============================================================================
# Kafka Topic Creation Script for Crypto Market Analytics
# Proposal Mapping: 5 Core Streaming & Event Topics
# ==============================================================================
set -e

BOOTSTRAP="kafka:9092"
TOPICS=(
    "market_trades"
    "market_klines"
    "order_book_events"
    "ticker_events"
    "alerts_events"
)

echo "Waiting for Kafka broker at ${BOOTSTRAP}..."
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
    sleep 3
done

for topic in "${TOPICS[@]}"; do
    echo "Creating Kafka topic: ${topic}"
    /opt/kafka/bin/kafka-topics.sh --create \
        --topic "${topic}" \
        --bootstrap-server "${BOOTSTRAP}" \
        --partitions 3 \
        --replication-factor 1 \
        || echo "  (topic '${topic}' already exists, skipping)"
done

echo "All proposal Kafka topics are ready."
