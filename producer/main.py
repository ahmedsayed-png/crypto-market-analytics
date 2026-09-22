"""
==============================================================================
producer/main.py
Producer service entry point.

Modes (selected via RUN_MODE env var):
  historical  — run the REST historical ingestion pipeline then exit
  streaming   — run the WebSocket real-time producer indefinitely
  both        — run historical ingestion first, then switch to streaming

Default is "streaming".
==============================================================================
"""

import logging
import os
import sys
import threading

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("producer.main")


def _check_dry_run() -> bool:
    api_key = os.environ.get("BINANCE_API_KEY", "")
    return not api_key or api_key == "your_binance_api_key_here"


def run_historical() -> None:
    from historical.binance_rest_producer import run_historical_ingestion
    run_historical_ingestion()


def run_streaming() -> None:
    from streaming.binance_ws_producer import BinanceWebSocketProducer
    producer = BinanceWebSocketProducer()
    producer.run()


def main() -> None:
    mode = os.environ.get("RUN_MODE", "streaming").lower()
    dry_run = _check_dry_run()

    if dry_run:
        logger.warning(
            "========================================================\n"
            "  DRY RUN MODE — BINANCE_API_KEY is not configured.\n"
            "  No external API calls or WebSocket connections will\n"
            "  be made. Set BINANCE_API_KEY in .env to activate.\n"
            "========================================================"
        )
        # Standby — keep the container alive for inspection
        import time
        logger.info("Producer service standby (dry-run). Ctrl+C to exit.")
        while True:
            time.sleep(60)
        return

    logger.info("Producer service starting in mode: %s", mode)

    if mode == "historical":
        run_historical()

    elif mode == "streaming":
        run_streaming()

    elif mode == "both":
        # Historical first (blocking), then streaming
        logger.info("Running historical ingestion before switching to streaming...")
        run_historical()
        logger.info("Historical ingestion done. Starting streaming producer...")
        run_streaming()

    else:
        logger.error("Unknown RUN_MODE '%s'. Must be: historical | streaming | both", mode)
        sys.exit(1)


if __name__ == "__main__":
    main()
