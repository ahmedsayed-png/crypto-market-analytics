"""
==============================================================================
producer/kafka_client.py
Thread-safe, singleton Kafka producer wrapper.

Usage
-----
    from kafka_client import get_kafka_producer
    producer = get_kafka_producer()
    producer.send_message(topic, payload_dict, key="BTCUSDT")
==============================================================================
"""

import json
import logging
import threading
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import CONFIG

logger = logging.getLogger(__name__)

_producer_instance: Optional["CryptoKafkaProducer"] = None
_lock = threading.Lock()


class CryptoKafkaProducer:
    """
    Wraps kafka-python's KafkaProducer and exposes a simple
    ``send_message(topic, payload, key)`` interface used by both the
    historical and the real-time ingestion paths.
    """

    def __init__(self) -> None:
        cfg = CONFIG.kafka
        self._producer = KafkaProducer(
            bootstrap_servers=cfg.bootstrap_servers,
            acks=cfg.acks,
            retries=cfg.retries,
            retry_backoff_ms=cfg.retry_backoff_ms,
            linger_ms=cfg.linger_ms,
            batch_size=cfg.batch_size,
            # Serialize keys and values to UTF-8 JSON bytes
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        logger.info(
            "KafkaProducer connected to %s", cfg.bootstrap_servers
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_message(
        self,
        topic: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
    ) -> None:
        """
        Asynchronously send *payload* to *topic*.
        On delivery error, the error is logged — the pipeline is designed
        to be idempotent so occasional drops are acceptable.
        """
        future = self._producer.send(topic, value=payload, key=key)
        future.add_errback(self._on_send_error, topic=topic)

    def flush(self) -> None:
        """Block until all pending messages are delivered."""
        self._producer.flush()

    def close(self) -> None:
        """Gracefully shut down the underlying Kafka producer."""
        self._producer.flush()
        self._producer.close()
        logger.info("KafkaProducer closed.")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _on_send_error(exc: KafkaError, topic: str = "") -> None:
        logger.error("Failed to deliver message to topic '%s': %s", topic, exc)


# ---------------------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------------------

def get_kafka_producer() -> CryptoKafkaProducer:
    """Return the module-level singleton KafkaProducer (thread-safe)."""
    global _producer_instance
    if _producer_instance is None:
        with _lock:
            if _producer_instance is None:
                _producer_instance = CryptoKafkaProducer()
    return _producer_instance
