FROM python:3.9-slim

WORKDIR /app

# Cài đặt các dependencies C và netcat cho kiểm tra kết nối
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các dependencies với phiên bản cụ thể và có sẵn
COPY requirements.txt .
# Cài đặt numpy trước để tránh lỗi biên dịch
RUN pip install --no-cache-dir numpy==1.20.3 && \
    pip install --no-cache-dir pandas==1.3.5 && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY kafka_processor.py .
COPY config.py .
COPY utils/ ./utils/
COPY start.sh .
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/bigquery-key.json

CMD ["./start.sh"]