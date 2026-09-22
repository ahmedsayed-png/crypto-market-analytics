ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.9.3-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

USER root
# Install Java OpenJDK 11 (required for PySpark / Spark submission within Airflow)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --default-timeout=1000 --retries 10 -r /requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH
