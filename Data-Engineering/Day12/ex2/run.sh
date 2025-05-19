#!/bin/bash

set -e  # nếu có lỗi thì dừng luôn script

# Đảm bảo rằng file docker-compose.yml là chính xác
COMPOSE_FILE=docker/docker-compose.yml

echo "===> Khởi động docker container..."
docker compose -f $COMPOSE_FILE up --build -d

# Kiểm tra xem container có khởi động thành công không
if [ $? -eq 0 ]; then
    echo "Docker container đã khởi động thành công."
else
    echo "Lỗi khi khởi động Docker container."
    exit 1
fi

echo "===> Chạy dbt seed..."
docker compose -f $COMPOSE_FILE exec dbt dbt seed

echo "===> Chạy dbt run..."
docker compose -f $COMPOSE_FILE exec dbt dbt run

echo "===> Chạy dbt test..."
docker compose -f $COMPOSE_FILE exec dbt dbt test

echo "===> Tạo tài liệu..."
docker compose -f $COMPOSE_FILE exec dbt dbt docs generate

echo "===> Phục vụ tài liệu tại localhost:8080"
docker compose -f $COMPOSE_FILE exec -d dbt dbt docs serve --port 8080
