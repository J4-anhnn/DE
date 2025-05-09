## Tổng quan

Apache Avro là hệ thống serialization cung cấp khả năng định nghĩa schema cho dữ liệu, giúp đảm bảo tính nhất quán và cho phép tiến hóa schema theo thời gian. Kết hợp với Schema Registry của Confluent, hệ thống quản lý schema trở nên mạnh mẽ và linh hoạt.

Lợi ích của việc sử dụng Avro và Schema Registry:

- Tự mô tả: Dữ liệu đi kèm với metadata mô tả cấu trúc
- Hiệu suất cao: Dữ liệu được nén hiệu quả
- Schema Evolution: Phát triển schema theo thời gian mà không gây gián đoạn
- Quản lý tập trung: Schema được lưu trữ và quản lý tập trung
- Kiểm tra tính tương thích: Tự động kiểm tra tính tương thích giữa các phiên bản schema

## Các thành phần

### 1. Schema Registry

- Quản lý và lưu trữ tập trung các schema Avro
- Kiểm tra tính tương thích giữa các phiên bản
- Cung cấp API để producer và consumer truy vấn schema

### 2. Producer với Avro

- Đăng ký schema với Schema Registry
- Serialize dữ liệu theo định dạng Avro
- Gửi dữ liệu đến Kafka với định danh schema

### 3. Consumer với Avro

- Truy vấn schema từ Schema Registry
- Deserialize dữ liệu Avro thành cấu trúc dữ liệu Python
- Xử lý dữ liệu và lưu vào BigQuery

### 4. Công cụ Schema Evolution

- Script kiểm tra tính tương thích của schema mới
- Tự động đăng ký và cập nhật schema
- Quản lý quá trình tiến hóa schema
