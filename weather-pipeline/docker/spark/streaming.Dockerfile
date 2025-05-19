FROM bitnami/spark:3.4.1

USER root

# Cài đặt các gói phụ thuộc
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các gói Python cần thiết
RUN pip3 install --no-cache-dir \
    pyspark==3.4.1 \
    google-cloud-storage>=2.0.0 \
    google-cloud-bigquery>=3.0.0 \
    kafka-python>=2.0.0

# Thiết lập biến môi trường
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV SPARK_DRIVER_MEMORY=1G
ENV SPARK_EXECUTOR_MEMORY=1G

WORKDIR /app

# Copy source code
COPY spark/streaming/weather_streaming_processing.py /app/

# Chạy Spark streaming job
CMD ["python3", "/app/weather_streaming_processing.py"]
