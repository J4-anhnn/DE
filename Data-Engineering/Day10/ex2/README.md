🚀 Tổng quan
Project này triển khai một pipeline đơn giản nhưng đầy đủ để xử lý dữ liệu, sử dụng:

Kestra: Quản lý và điều phối workflow
Kafka: Message broker để truyền dữ liệu
Spark: Xử lý dữ liệu phân tán
🏗️ Kiến trúc hệ thống
[Kestra Flow] --> [Kafka Producer] --> [Kafka Topic] --> [Spark Consumer] --> [Storage]
📋 Chi tiết các thành phần
1️⃣ Kestra Flow
Kestra được sử dụng để điều phối toàn bộ luồng dữ liệu với hai tác vụ chính:

Flow: “simple-data-pipeline”
Tác vụ 1: ingest_data

Kích hoạt script Python kafka_producer.py
Tạo và gửi 20 bản ghi dữ liệu giả lập vào Kafka topic “input-data”
Mỗi bản ghi chứa id, value ngẫu nhiên, và timestamp
Tác vụ 2: process_data

Phụ thuộc vào hoàn thành của tác vụ “ingest_data”
Kích hoạt Spark job qua spark-submit
Chạy script spark_consumer.py để xử lý dữ liệu từ Kafka
Cách Kestra điều phối:
Theo dõi trạng thái: Giám sát và báo cáo tiến trình của từng tác vụ
Xử lý phụ thuộc: Đảm bảo thứ tự chạy đúng các tác vụ
Retry logic: Tự động thử lại khi có lỗi
2️⃣ Kafka
Topic: “input-data”
Partitions: 1 (đơn giản cho demo)
Retention: Mặc định
Message format: JSON với schema đơn giản
{
"id": 1,
"value": 75,
"timestamp": 1715181600.123
}
Copy
3️⃣ Spark
Spark xử lý dữ liệu với các bước sau:

Xử lý chính:
Đọc dữ liệu từ Kafka:

Sử dụng Structured Streaming
Đọc từ topic “input-data”
Parse dữ liệu JSON thành DataFrame có cấu trúc
Biến đổi dữ liệu:

Trích xuất các trường từ JSON
Thêm cột mới “processed_value” = value \* 2
Áp dụng các transformation đơn giản trên dữ liệu gốc
Xử lý dữ liệu streaming:

Xử lý theo micro-batch
Mỗi batch được xử lý và ghi ra ngoài
Lưu trữ kết quả:

Ghi dữ liệu đã xử lý ra định dạng CSV
Lưu vào đường dẫn định trước với checkpoint để đảm bảo exactly-once processing
🔧 Cấu hình và triển khai
Docker Compose
File docker-compose.yml cung cấp các service cần thiết:

Zookeeper: Quản lý cluster Kafka
Kafka: Message broker
Kestra: Orchestration engine
Spark: Processing engine
Khởi động hệ thống

# Khởi động tất cả service

docker-compose up -d

# Kiểm tra trạng thái

docker-compose ps
Copy
Truy cập UI
Kestra UI: http://localhost:8081
Spark UI: http://localhost:8090
📊 Luồng dữ liệu
Kestra khởi chạy producer script để tạo dữ liệu giả lập
Dữ liệu được gửi vào Kafka topic “input-data”
Kestra kích hoạt Spark job để xử lý dữ liệu
Spark đọc dữ liệu từ Kafka, xử lý và lưu kết quả
Kết quả cuối cùng lưu dưới dạng file CSV
💻 Mã nguồn
Kafka Producer (kafka_producer.py)

# Tạo 20 bản ghi dữ liệu giả lập và gửi vào Kafka

# Mỗi bản ghi có id, giá trị ngẫu nhiên, và timestamp

Copy
Spark Consumer (spark_consumer.py)

# Kết nối với Kafka và đọc dữ liệu từ topic "input-data"

# Parse JSON thành DataFrame Spark có cấu trúc

# Tạo cột mới "processed_value" = value \* 2

# Lưu kết quả ra định dạng CSV
