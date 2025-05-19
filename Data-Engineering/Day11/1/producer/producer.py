from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Đảm bảo địa chỉ Kafka đúng
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        "user_id": random.randint(1, 100),
        "action": random.choice(["click", "view", "purchase"]),
        "timestamp": int(time.time())
    }
    producer.send("clickstream", message)
    print("Sent:", message)
    time.sleep(1)
