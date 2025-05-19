FROM python:3.9-slim

WORKDIR /app

# Cài đặt dependencies và netcat cho kiểm tra kết nối
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Python dependencies
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY producer/config.py .
COPY producer/click_stream_producer_avro.py .
COPY producer/start.sh .
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1

CMD ["./start.sh"]