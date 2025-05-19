import json
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gửi 10 bản ghi giả lập
for i in range(10):
    message = {
        "user_id": f"user_{i}",
        "timestamp": datetime.utcnow().isoformat(),
        "action": "click"
    }
    producer.send('click_topic', value=message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
