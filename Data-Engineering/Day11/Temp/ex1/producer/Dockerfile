FROM python:3.9-slim

WORKDIR /app

# Cài đặt netcat để kiểm tra kết nối Kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY click_stream_producer.py .
COPY config.py .
COPY start.sh .
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1

CMD ["./start.sh"]