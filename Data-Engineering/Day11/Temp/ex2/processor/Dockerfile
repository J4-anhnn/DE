FROM python:3.9-slim

WORKDIR /app

# Cài đặt các dependencies và netcat
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các dependencies với phiên bản cụ thể
COPY processor/requirements.txt .
RUN pip install --no-cache-dir numpy==1.20.3 && \
    pip install --no-cache-dir pandas==1.3.5 && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY processor/config.py .
COPY processor/kafka_processor_avro.py .
COPY processor/start.sh .
COPY processor/utils/ ./utils/
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/bigquery-key.json

CMD ["./start.sh"]
