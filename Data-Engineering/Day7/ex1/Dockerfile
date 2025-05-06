# Sử dụng image Python chính thức
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Sao chép các file vào container
COPY . /app

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Mở cổng Flask sẽ chạy
EXPOSE 5001

# Lệnh chạy ứng dụng
CMD ["python", "app.py"]
