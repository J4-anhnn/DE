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
