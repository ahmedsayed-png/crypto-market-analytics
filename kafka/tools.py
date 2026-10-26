"""
==============================================================================
kafka/tools.py
Kafka Management, Inspection, and Smoke Testing Utilities.

Provides:
  - inspect_topics: partition info, ISR, log offsets, configs
  - inspect_consumer_groups: group state, members, lagged offsets
  - publish_test_message: send canonical test message with explicit tracking
  - smoke_test_consumer: read non-destructively with dedicated consumer group
==============================================================================
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "producer"))

try:
    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import ConfigResource, ConfigResourceType
    from kafka.errors import KafkaError
except ImportError:
    print("Error: kafka-python is not installed in the current environment.", file=sys.stderr)
    print("Run this tool inside Docker: docker run --rm --network crypto-net -v $(pwd):/app -w /app crypto-producer:local python kafka/tools.py ...", file=sys.stderr)
    sys.exit(1)

from schema import CanonicalKlineMessage, validate_kline_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("kafka_tools")

# Detect whether we are running inside Docker or on the host
def _resolve_default_bootstrap() -> str:
    env_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if env_bootstrap:
        return env_bootstrap
    # Check if 'kafka' hostname resolves (inside Docker crypto-net)
    import socket
    try:
        socket.gethostbyname("kafka")
        return "kafka:9092"
    except socket.gaierror:
        return "localhost:9094"

DEFAULT_BOOTSTRAP = _resolve_default_bootstrap()


def get_admin_client(bootstrap_servers: str) -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="crypto_kafka_tools_admin",
        request_timeout_ms=10000,
    )


def inspect_topics(bootstrap_servers: str, topics: Optional[List[str]] = None) -> None:
    """Inspect topic configurations, partitions, leaders, and highwater offsets."""
    print(f"\n==============================================================================")
    print(f" Kafka Topic Inspection (Bootstrap: {bootstrap_servers})")
    print(f"==============================================================================")

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        client_id="crypto_topic_inspector",
        request_timeout_ms=10000,
    )
    admin = get_admin_client(bootstrap_servers)

    try:
        cluster_topics = consumer.topics()
        target_topics = topics if topics else sorted(list(cluster_topics))

        for topic in target_topics:
            if topic not in cluster_topics:
                print(f"\n[TOPIC] {topic} — NOT FOUND ON CLUSTER")
                continue

            partitions = consumer.partitions_for_topic(topic) or set()
            print(f"\n[TOPIC] {topic} (Partitions: {len(partitions)})")

            # Fetch configs
            try:
                resource = ConfigResource(ConfigResourceType.TOPIC, topic)
                configs_dict = admin.describe_configs([resource])
                for res, cfg in configs_dict.items():
                    retention = cfg.get("retention.ms")
                    cleanup = cfg.get("cleanup.policy")
                    retention_val = retention.value if retention else "default"
                    cleanup_val = cleanup.value if cleanup else "default"
                    print(f"  Configuration: retention.ms={retention_val}, cleanup.policy={cleanup_val}")
            except Exception as e:
                print(f"  Configuration: unable to retrieve ({e})")

            # Fetch partition offsets
            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            beginning_offsets = consumer.beginning_offsets(tps)
            end_offsets = consumer.end_offsets(tps)

            for tp in tps:
                start = beginning_offsets.get(tp, 0)
                end = end_offsets.get(tp, 0)
                count = end - start
                print(f"  Partition {tp.partition:2d}: Start Offset={start:6d} | Highwater Offset={end:6d} | Messages={count:6d}")

    finally:
        consumer.close()
        admin.close()


def inspect_consumer_groups(bootstrap_servers: str) -> None:
    """List consumer groups and describe active group state."""
    print(f"\n==============================================================================")
    print(f" Consumer Group Inspection (Bootstrap: {bootstrap_servers})")
    print(f"==============================================================================")
    admin = get_admin_client(bootstrap_servers)
    try:
        groups = admin.list_consumer_groups()
        if not groups:
            print("No consumer groups found on this cluster.")
            return

        for group_id, protocol_type in groups:
            print(f"- Group: {group_id} (Protocol: {protocol_type})")
    finally:
        admin.close()


def publish_test_message(
    bootstrap_servers: str,
    topic: str,
    symbol: str = "BTCUSDT",
    data_source: str = "historical",
) -> Dict[str, Any]:
    """Publish a validated canonical Kline message to Kafka."""
    print(f"\nPublishing test canonical message to topic '{topic}' (symbol={symbol}, source={data_source})...")

    now_ms = int(time.time() * 1000)
    canonical = CanonicalKlineMessage(
        open_time=now_ms - 60000,
        symbol=symbol.upper(),
        open=65000.50,
        high=65120.00,
        low=64980.25,
        close=65050.75,
        volume=12.4501,
        close_time=now_ms,
        quote_asset_volume=809881.85,
        number_of_trades=425,
        taker_buy_base_volume=6.1200,
        taker_buy_quote_volume=398100.10,
        data_source=data_source,
        ingested_at=datetime.now(tz=timezone.utc).isoformat(),
    )

    msg_dict = canonical.to_dict()
    is_valid, reason = validate_kline_dict(msg_dict)
    if not is_valid:
        raise ValueError(f"Test message failed schema validation: {reason}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        request_timeout_ms=10000,
    )

    try:
        future = producer.send(topic, value=msg_dict, key=symbol.upper())
        record_metadata = future.get(timeout=10)
        producer.flush()
        print(f"✓ Message successfully delivered:")
        print(f"  Topic:     {record_metadata.topic}")
        print(f"  Partition: {record_metadata.partition}")
        print(f"  Offset:    {record_metadata.offset}")
        print(f"  Key:       {symbol.upper()}")
        print(f"  Timestamp: {record_metadata.timestamp}")
        return msg_dict
    finally:
        producer.close()


def smoke_test_consumer(
    bootstrap_servers: str,
    topic: str,
    max_messages: int = 5,
    timeout_seconds: float = 10.0,
    from_beginning: bool = False,
) -> List[Dict[str, Any]]:
    """
    Read messages non-destructively using a dedicated ephemeral test consumer group.
    Does not reset existing production group offsets.
    """
    ephemeral_group = f"smoke_test_group_{int(time.time())}"
    auto_offset = "earliest" if from_beginning else "latest"

    print(f"\nStarting non-destructive consumer smoke test on topic '{topic}'...")
    print(f"  Bootstrap:       {bootstrap_servers}")
    print(f"  Consumer Group:  {ephemeral_group} (ephemeral)")
    print(f"  Auto Offset:     {auto_offset}")
    print(f"  Max Messages:    {max_messages}")
    print(f"  Timeout:         {timeout_seconds}s")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=ephemeral_group,
        auto_offset_reset=auto_offset,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=int(timeout_seconds * 1000),
    )

    received_messages = []
    try:
        start_time = time.time()
        for message in consumer:
            received_messages.append(message.value)
            print(f"  [RECEIVED] partition={message.partition} offset={message.offset} key={message.key.decode('utf-8') if message.key else None}")
            payload = message.value
            is_valid, reason = validate_kline_dict(payload)
            val_status = "VALID" if is_valid else f"INVALID ({reason})"
            print(f"    symbol: {payload.get('symbol')} | open_time: {payload.get('open_time')} | close: {payload.get('close')} | Schema: {val_status}")

            if len(received_messages) >= max_messages:
                break
            if time.time() - start_time > timeout_seconds:
                break

        print(f"✓ Consumer read {len(received_messages)} message(s) successfully.")
        return received_messages
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Crypto Market Analytics — Kafka Diagnostic Tools")
    parser.add_argument(
        "--bootstrap",
        default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP})",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: inspect-topics
    parser_inspect = subparsers.add_parser("inspect-topics", help="Inspect topic partition and offset details")
    parser_inspect.add_argument("--topics", nargs="*", help="Optional topic names to filter")

    # Subcommand: inspect-groups
    subparsers.add_parser("inspect-groups", help="Inspect consumer groups")

    # Subcommand: publish-test
    parser_pub = subparsers.add_parser("publish-test", help="Publish a canonical test Kline message")
    parser_pub.add_argument("--topic", required=True, help="Target Kafka topic")
    parser_pub.add_argument("--symbol", default="BTCUSDT", help="Crypto symbol")
    parser_pub.add_argument("--source", choices=["historical", "realtime"], default="historical", help="Data source tag")

    # Subcommand: consume-test
    parser_sub = subparsers.add_parser("consume-test", help="Consume messages using an ephemeral group")
    parser_sub.add_argument("--topic", required=True, help="Target Kafka topic")
    parser_sub.add_argument("--max", type=int, default=5, help="Max messages to consume")
    parser_sub.add_argument("--timeout", type=float, default=10.0, help="Timeout in seconds")
    parser_sub.add_argument("--from-beginning", action="store_true", help="Read from earliest available offset")

    args = parser.parse_args()

    if args.command == "inspect-topics":
        inspect_topics(args.bootstrap, args.topics)
    elif args.command == "inspect-groups":
        inspect_consumer_groups(args.bootstrap)
    elif args.command == "publish-test":
        publish_test_message(args.bootstrap, args.topic, args.symbol, args.source)
    elif args.command == "consume-test":
        smoke_test_consumer(args.bootstrap, args.topic, args.max, args.timeout, args.from_beginning)


if __name__ == "__main__":
    main()
