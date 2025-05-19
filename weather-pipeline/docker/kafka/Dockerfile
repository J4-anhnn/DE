FROM python:3.9-slim

WORKDIR /app

# Cài đặt các gói phụ thuộc
COPY requirements-kafka.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-kafka.txt

# Copy source code
COPY kafka/producer/ /app/

# Chạy producer
CMD ["python", "weather_producer.py"]
