from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
time.sleep(10)

while True:
    event = {
        'sensor_id': random.randint(1, 5),
        'value': random.uniform(20.0, 40.0),
        'timestamp': int(time.time())
    }
    producer.send('sensor-data', event)
    print(f"Sent: {event}")
    time.sleep(1)
