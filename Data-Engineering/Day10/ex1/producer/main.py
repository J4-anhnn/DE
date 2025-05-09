import time
from kafka import KafkaProducer
import json
import random

for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except Exception as e:
        print(f"Kafka not available yet, retrying in 5s... ({i+1}/10)")
        time.sleep(5)
else:
    raise Exception("Kafka not available after multiple retries")

topic = 'sensor-data'

while True:
    message = {
        "sensor_id": random.randint(1, 10),
        "value": round(random.uniform(20.0, 30.0), 2),
        "timestamp": int(time.time())
    }
    print(f"Sending: {message}")
    producer.send(topic, value=message)
    time.sleep(1)
