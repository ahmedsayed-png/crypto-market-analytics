ARG SUPERSET_IMAGE_TAG=4.1.2
FROM apache/superset:${SUPERSET_IMAGE_TAG}

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
