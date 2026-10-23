"""
==============================================================================
airflow/dags/dag_crypto_historical_batch.py
Airflow DAG: Bounded Historical Spark Batch Processing Workflow.

Execution:
- Manually triggered (schedule=None) or on-demand batch run.
- Zero Spark jobs executed at DAG import time.
- Submits PySpark batch job to Spark Master with Kafka & PostgreSQL packages.
- Safe, bounded offset boundaries, automatic resumption, and dynamic secret management.
==============================================================================
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def verify_cluster_endpoints():
    """Verify connectivity to HDFS, Kafka, and PostgreSQL before Spark job launch."""
    import socket

    hdfs_host = os.environ.get("HDFS_HOST", "namenode")
    hdfs_port = int(os.environ.get("HDFS_RPC_PORT", "9000"))
    kafka_host = os.environ.get("KAFKA_HOST", "kafka")
    kafka_port = int(os.environ.get("KAFKA_PORT", "9092"))
    pg_host = os.environ.get("POSTGRES_DW_HOST", "postgres-dw")
    pg_port = int(os.environ.get("POSTGRES_DW_PORT", "5432"))

    endpoints = [
        (hdfs_host, hdfs_port, "HDFS NameNode"),
        (kafka_host, kafka_port, "Apache Kafka"),
        (pg_host, pg_port, "PostgreSQL Data Warehouse"),
    ]
    for host, port, name in endpoints:
        s = socket.socket()
        s.settimeout(5)
        try:
            s.connect((host, port))
            s.close()
            print(f"✓ {name} ({host}:{port}) is reachable.")
        except Exception as e:
            s.close()
            raise ConnectionError(f"Precondition failed: {name} ({host}:{port}) is unreachable: {e}")


with DAG(
    dag_id="crypto_historical_batch_pipeline",
    default_args=default_args,
    description="Manually triggered bounded Spark historical batch ingestion from Kafka to HDFS & PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "spark", "kafka", "hdfs", "postgres", "batch"],
) as dag:

    check_prerequisites = PythonOperator(
        task_id="check_cluster_prerequisites",
        python_callable=verify_cluster_endpoints,
    )

    # Submit Spark batch processing job using environment variables (no credentials passed in CLI arguments)
    # Bounded to maximum 1000 records per partition by default with auto-resumption from HDFS checkpoint
    spark_submit_command = """
    spark-submit \
        --master "${SPARK_MASTER:-spark://spark-master:7077}" \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 \
        --conf spark.sql.session.timeZone=UTC \
        /opt/spark-apps/historical_processing.py \
            --kafka-bootstrap "${KAFKA_BOOTSTRAP:-kafka:9092}" \
            --kafka-topic "${KAFKA_TOPIC_HISTORICAL:-crypto_klines_historical}" \
            --starting-offsets "${STARTING_OFFSETS:-auto}" \
            --max-records-per-partition "${MAX_RECORDS_PER_PARTITION:-1000}" \
            --hdfs-namenode "${HDFS_NAMENODE:-hdfs://namenode:9000}" \
            --hdfs-output-path "${HDFS_HISTORICAL_OUTPUT:-/data/crypto/historical/klines}" \
            --hdfs-rejected-path "${HDFS_HISTORICAL_REJECTED:-/data/crypto/historical/rejected}" \
            --offset-state-path "${HDFS_OFFSET_STATE_PATH:-/data/crypto/checkpoints/historical_batch/offsets.json}" \
            --postgres-host "${POSTGRES_DW_HOST:-postgres-dw}" \
            --postgres-port "${POSTGRES_DW_PORT:-5432}" \
            --postgres-db "${POSTGRES_DW_DB:-crypto_dw}" \
            --postgres-user "${POSTGRES_DW_USER:-crypto_admin}" \
            --enable-postgres
    """

    run_spark_batch = BashOperator(
        task_id="run_spark_historical_batch",
        bash_command=spark_submit_command,
        env={
            "POSTGRES_DW_PASSWORD": os.environ.get("POSTGRES_DW_PASSWORD", "crypto_secure_password_123"),
            "POSTGRES_DW_USER": os.environ.get("POSTGRES_DW_USER", "crypto_admin"),
            "POSTGRES_DW_DB": os.environ.get("POSTGRES_DW_DB", "crypto_dw"),
            "POSTGRES_DW_HOST": os.environ.get("POSTGRES_DW_HOST", "postgres-dw"),
            "POSTGRES_DW_PORT": os.environ.get("POSTGRES_DW_PORT", "5432"),
            "KAFKA_BOOTSTRAP": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            "HDFS_NAMENODE": os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000"),
            "SPARK_MASTER": os.environ.get("SPARK_MASTER", "spark://spark-master:7077"),
        },
    )

    check_prerequisites >> run_spark_batch
