FROM apache/airflow:2.6.3

USER root

# Cài đặt các gói phụ thuộc
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Cài đặt các gói Python cần thiết
COPY requirements-airflow.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Thiết lập biến môi trường
ENV PYTHONPATH=/opt/airflow
