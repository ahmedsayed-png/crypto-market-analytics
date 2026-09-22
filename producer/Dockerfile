FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# RUN_MODE controls the ingestion path:
#   historical  — REST API full backfill then exit
#   streaming   — WebSocket real-time producer (default)
#   both        — historical first, then streaming
ENV RUN_MODE=streaming

CMD ["python", "main.py"]
