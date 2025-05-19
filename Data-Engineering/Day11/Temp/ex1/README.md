📊 Tổng Quan
Hệ thống streaming xử lý dữ liệu click stream (như các sự kiện click, page view, v.v.) được tạo ra bởi một producer giả lập. Dữ liệu được gửi tới Kafka, sau đó được xử lý và ghi vào BigQuery để phân tích và báo cáo.

Luồng Dữ Liệu:
Tạo dữ liệu: Producer tạo dữ liệu click stream giả lập
Truyền dữ liệu: Dữ liệu được gửi đến Kafka broker
Xử lý: Processor nhận và xử lý dữ liệu từ Kafka
Lưu trữ: Dữ liệu sau khi xử lý được lưu vào BigQuery
🏗️ Kiến Trúc Hệ Thống
┌─────────────┐
│ │
┌─────────────► │ ZooKeeper │
│ │ │
│ └─────────────┘
│ ▲
│ │
│ ▼
┌─┴────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ │ │ │ │ │ │ │
│ Producer ├─────►│ Kafka ├────►│ Processor ├────►│ BigQuery │
│ │ │ │ │ │ │ │
└───────────┘ └─────────────┘ └─────────────┘ └─────────────┘
🧩 Các Thành Phần

1. ZooKeeper
   Quản lý và điều phối Kafka cluster
   Theo dõi trạng thái của các Kafka broker
2. Kafka
   Message broker cho streaming data
   Lưu trữ các message trong partitioned log
   Topic: click_stream_data
3. Producer
   Tạo dữ liệu click stream giả lập sử dụng Faker
   Gửi dữ liệu đến Kafka topic
   Cấu hình tốc độ tạo dữ liệu có thể điều chỉnh
4. Processor
   Tiêu thụ dữ liệu từ Kafka
   Xử lý và tổng hợp dữ liệu
   Ghi kết quả vào BigQuery
   Theo dõi hiệu suất xử lý
5. BigQuery
   Lưu trữ dữ liệu đã xử lý
   Cho phép phân tích dữ liệu quy mô lớn
   📋 Yêu Cầu Trước Khi Cài Đặt
   Docker và Docker Compose

# Kiểm tra phiên bản Docker

docker --version

# Kiểm tra phiên bản Docker Compose

docker-compose --version
Copy
Google Cloud Platform Account

Project BigQuery đã tạo
Service account với quyền truy cập BigQuery
File key JSON của service account

Cấu Trúc Thư Mục
kafka_bigquery_project/
│
├── docker-compose.yml # Cấu hình Docker Compose
├── .env # Environment variables
│
├── producer/
│ ├── Dockerfile # Producer image build
│ ├── requirements.txt # Producer dependencies
│ ├── config.py # Producer configuration
│ ├── click_stream_producer.py # Main producer code
│ └── start.sh # Startup script with retry logic
│
├── processor/
│ ├── Dockerfile # Processor image build
│ ├── requirements.txt # Processor dependencies
│ ├── config.py # Processor configuration
│ ├── kafka_processor.py # Main processor code
│ ├── start.sh # Startup script with retry logic
│ └── utils/
│ ├── **init**.py
│ ├── bigquery_connector.py # BigQuery connection logic
│ └── monitoring.py # Performance monitoring
│
└── credentials/
└── bigquery-key.json # BigQuery service account key
