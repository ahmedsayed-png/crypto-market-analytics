"""
==============================================================================
airflow/dags/dag_crypto_historical_ingestion.py
Airflow DAG: On-Demand Binance REST Historical Ingestion Workflow.

Execution:
- Manually triggered ONLY (schedule_interval=None) — never runs automatically.
- Safely parameterizable via DAG run configuration or params.
- Submits REST ingestion request to the producer service or container execution.
- Idempotent and bounded: does not produce duplicate candles or overwrite data.
==============================================================================
"""

from datetime import datetime, timedelta
import os
import subprocess

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}


def verify_ingestion_prerequisites():
    """Verify Kafka broker connectivity before launching historical ingestion."""
    import socket

    kafka_host = os.environ.get("KAFKA_HOST", "kafka")
    kafka_port = int(os.environ.get("KAFKA_PORT", "9092"))

    s = socket.socket()
    s.settimeout(5)
    try:
        s.connect((kafka_host, kafka_port))
        s.close()
        print(f"✓ Apache Kafka ({kafka_host}:{kafka_port}) is reachable.")
    except Exception as e:
        s.close()
        raise ConnectionError(f"Precondition failed: Apache Kafka ({kafka_host}:{kafka_port}) is unreachable: {e}")


with DAG(
    dag_id="crypto_historical_ingestion_pipeline",
    default_args=default_args,
    description="On-demand manual trigger for Binance REST Historical Ingestion to Kafka",
    schedule_interval=None,  # Explicitly manual execution only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "binance", "rest", "kafka", "historical"],
    params={
        "symbols": Param("BTCUSDT,ETHUSDT", type="string", description="Comma-separated trading pairs"),
        "interval": Param("1m", type="string", description="Kline candlestick timeframe (e.g. 1m, 5m, 1h)"),
        "lookback_days": Param(30, type="integer", description="Days of historical data to request"),
        "max_batches": Param(10, type="integer", description="Max batches per symbol (safety limiter)"),
        "force_run": Param(True, type="boolean", description="Allow execution against public endpoints"),
    },
) as dag:

    check_prerequisites = PythonOperator(
        task_id="check_kafka_prerequisites",
        python_callable=verify_ingestion_prerequisites,
    )

    # Ingestion command targeting python producer inside crypto-net
    ingest_command = """
    python3 /opt/airflow/producer/historical/binance_rest_producer.py \
        --symbols "{{ params.symbols }}" \
        --interval "{{ params.interval }}" \
        --lookback-days {{ params.lookback_days }} \
        --max-batches {{ params.max_batches }} \
        {% if params.force_run %}--force-run{% endif %}
    """

    run_rest_ingestion = BashOperator(
        task_id="run_binance_rest_historical_ingestion",
        bash_command=ingest_command,
        env={
            "PYTHONPATH": "/opt/airflow/producer",
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            "KAFKA_TOPIC_HISTORICAL_KLINES": os.environ.get("KAFKA_TOPIC_HISTORICAL_KLINES", "crypto_klines_historical"),
            "BINANCE_API_KEY": os.environ.get("BINANCE_API_KEY", ""),
            "BINANCE_API_SECRET": os.environ.get("BINANCE_API_SECRET", ""),
            "BINANCE_BASE_URL": os.environ.get("BINANCE_BASE_URL", "https://api.binance.com"),
        },
    )

    check_prerequisites >> run_rest_ingestion
