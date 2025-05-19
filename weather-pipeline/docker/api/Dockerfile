FROM python:3.9-slim

WORKDIR /app

# Cài đặt các gói phụ thuộc
COPY requirements-api.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-api.txt

# Copy source code
COPY api/ /app/

# Thiết lập biến môi trường
ENV PYTHONPATH=/app

# Chạy API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
