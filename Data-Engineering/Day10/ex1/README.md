# ⚡ Kafka + Spark Streaming Demo

Hệ thống này minh họa cách sử dụng Apache Kafka để thu thập dữ liệu thời gian thực từ các sensor và xử lý chúng bằng Apache Spark Structured Streaming.

## 📦 Thành phần chính

### 1. 🐘 Zookeeper

- Hỗ trợ quản lý Kafka broker.

### 2. 📡 Kafka

- Message broker để giao tiếp giữa `producer` và `Spark`.

### 3. 🛠️ Producer

- Sinh dữ liệu sensor ngẫu nhiên gồm:
  - `sensor_id`: ID của cảm biến
  - `value`: giá trị đo (float)
  - `timestamp`: thời gian đo (epoch millis)
- Gửi dữ liệu vào Kafka topic `sensor-data`.

### 4. 🔥 Spark Structured Streaming

- Đọc dữ liệu từ Kafka topic `sensor-data`.
- Parse chuỗi JSON từ Kafka thành DataFrame.
- Thực hiện thống kê `groupBy(sensor_id).count()` để đếm số lượng bản ghi theo từng sensor.

---

Output ví dụ

+---------+-----+
|sensor_id|count|
+---------+-----+
| 1| 100|
| 2| 85|
+---------+-----+
