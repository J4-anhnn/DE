FROM python:3.9-slim

WORKDIR /app

# Cài đặt các gói phụ thuộc
COPY requirements-kafka.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-kafka.txt

# Cài đặt thêm google-cloud-bigquery
RUN pip install --no-cache-dir google-cloud-bigquery>=3.0.0

# Copy source code
COPY kafka/consumer/ /app/

# Chạy consumer
CMD ["python", "weather_consumer.py"]
