FROM bitnami/spark:3.4.1

USER root

# Cài đặt các gói phụ thuộc
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các gói Python cần thiết
COPY requirements-spark.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

# Thiết lập biến môi trường
ENV SPARK_MODE=master
ENV SPARK_RPC_AUTHENTICATION_ENABLED=no
ENV SPARK_RPC_ENCRYPTION_ENABLED=no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
ENV SPARK_SSL_ENABLED=no
