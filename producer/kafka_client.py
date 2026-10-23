"""
==============================================================================
producer/kafka_client.py
Thread-safe, singleton Kafka producer wrapper with schema validation,
configurable retry callbacks, and metrics tracking.

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
from schema import validate_kline_dict

logger = logging.getLogger(__name__)

_producer_instance: Optional["CryptoKafkaProducer"] = None
_lock = threading.Lock()


class CryptoKafkaProducer:
    """
    Wraps kafka-python's KafkaProducer and exposes a robust
    ``send_message(topic, payload, key, validate_schema)`` interface.
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
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        self.sent_count = 0
        self.error_count = 0
        self.validation_failure_count = 0
        logger.info(
            "CryptoKafkaProducer connected to bootstrap_servers=%s (acks=%s, retries=%d)",
            cfg.bootstrap_servers,
            cfg.acks,
            cfg.retries,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_message(
        self,
        topic: str,
        payload: Dict[str, Any],
        key: Optional[str] = None,
        validate: bool = False,
    ) -> bool:
        """
        Asynchronously send *payload* to *topic*.
        If validate is True, validates against the canonical Kline schema before send.
        Returns True if sent/enqueued, False if rejected by schema validation.
        """
        if validate:
            is_valid, reason = validate_kline_dict(payload)
            if not is_valid:
                self.validation_failure_count += 1
                logger.error(
                    "Schema validation failed for record on topic '%s' (key=%s): %s. Record: %s",
                    topic,
                    key,
                    reason,
                    payload,
                )
                return False

        try:
            future = self._producer.send(topic, value=payload, key=key)
            future.add_errback(self._on_send_error, topic=topic, key=key)
            self.sent_count += 1
            return True
        except KafkaError as exc:
            self.error_count += 1
            logger.error("Failed to enqueue message to topic '%s': %s", topic, exc)
            return False

    def flush(self, timeout: Optional[float] = None) -> None:
        """Block until all pending messages are delivered."""
        self._producer.flush(timeout=timeout)

    def close(self, timeout: Optional[float] = None) -> None:
        """Gracefully flush and close the underlying Kafka producer."""
        try:
            self._producer.flush(timeout=timeout)
        finally:
            self._producer.close(timeout=timeout)
            logger.info("CryptoKafkaProducer cleanly closed. Total sent: %d, errors: %d", self.sent_count, self.error_count)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _on_send_error(self, exc: Exception, topic: str = "", key: Optional[str] = None) -> None:
        self.error_count += 1
        logger.error("Asynchronous delivery error to topic '%s' (key=%s): %s", topic, key, exc)


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
