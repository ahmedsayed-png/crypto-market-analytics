# Dockerfile placeholder for Binance Python Producers
FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer scripts (to be added in implementation phase)
COPY . .

CMD ["python", "--version"]
