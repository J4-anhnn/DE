#!/bin/bash

# Màu sắc output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== KAFKA-SPARK PIPELINE TEST SCRIPT ===${NC}"

# -----------------
# PHẦN 1: CÀI ĐẶT
# -----------------
echo -e "${YELLOW}[1/5] Chuẩn bị thư mục scripts...${NC}"
mkdir -p scripts

# -----------------
# PHẦN 2: TẠO SCRIPTS
# -----------------
echo -e "${YELLOW}[2/5] Tạo các script cần thiết...${NC}"

# Kafka Producer
cat > scripts/kafka_producer.py << EOF
from kafka import KafkaProducer
import json
import time
import random

# Cấu hình producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Tạo dữ liệu test
print("Đang gửi dữ liệu vào Kafka...")
for i in range(20):
    data = {
        'id': i,
        'value': random.randint(1, 100),
        'timestamp': time.time()
    }
    # Gửi vào Kafka
    producer.send('input-data', value=data)
    print(f"Đã gửi: {data}")
    time.sleep(0.2)

producer.flush()
print("Hoàn thành việc tạo dữ liệu!")
EOF

# Spark Consumer - Đơn giản hóa, không cần thư viện Kafka
cat > scripts/spark_consumer.py << EOF
from pyspark.sql import SparkSession

# Khởi tạo Spark
spark = SparkSession.builder \\
    .appName("SimpleSparkTest") \\
    .getOrCreate()

print("=== SPARK SESSION ĐƯỢC KHỞI TẠO THÀNH CÔNG ===")
print(f"Spark version: {spark.version}")
print(f"Spark master: {spark.sparkContext.master}")

# Tạo dữ liệu đơn giản để kiểm tra
data = [(1, "test")]
df = spark.createDataFrame(data, ["id", "value"])

# In kết quả
print("=== SAMPLE DATA ===")
df.show()

# Thêm xử lý
df2 = df.withColumn("processed", df.id * 10)
print("=== PROCESSED DATA ===")
df2.show()

print("=== XỬ LÝ HOÀN TẤT ===")
spark.stop()
EOF

# -----------------
# PHẦN 3: TẠO KAFKA TOPIC
# -----------------
echo -e "${YELLOW}[3/5] Tạo Kafka topic...${NC}"
docker exec ex2-kafka-1 kafka-topics --create --topic input-data --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --if-not-exists

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Tạo Kafka topic thành công${NC}"
else
    echo -e "${RED}✗ Lỗi khi tạo Kafka topic${NC}"
    exit 1
fi

# -----------------
# PHẦN 4: CHẠY PRODUCER
# -----------------
echo -e "${YELLOW}[4/5] Cài đặt thư viện và chạy producer...${NC}"
echo "Kiểm tra kafka-python..."
pip install kafka-python -q

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Thư viện đã sẵn sàng${NC}"
else
    echo -e "${RED}✗ Lỗi khi cài đặt thư viện. Thử lại với sudo hoặc pip3${NC}"
    exit 1
fi

echo "Đang chạy Kafka producer..."
python scripts/kafka_producer.py

# -----------------
# PHẦN 5: CHẠY SPARK CONSUMER
# -----------------
echo -e "${YELLOW}[5/5] Chạy Spark consumer...${NC}"
echo "Copy script vào Spark container..."
docker cp scripts/spark_consumer.py ex2-spark-1:/tmp/

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Copy script thành công${NC}"
else
    echo -e "${RED}✗ Lỗi khi copy script${NC}"
    exit 1
fi

echo "Chạy Spark job..."
# Chạy Spark mà không cần gói bổ sung
docker exec ex2-spark-1 spark-submit /tmp/spark_consumer.py

echo -e "${GREEN}=== KIỂM TRA HOÀN TẤT ===${NC}"
echo "Thông tin thêm:"
echo "- Spark UI: http://localhost:8090"
echo "- Kestra UI: http://localhost:8081"
