from confluent_kafka import Producer
import time
import os

broker = os.getenv("KAFKA_BROKER", "localhost:9092")
topic = "transactions"

p = Producer({'bootstrap.servers': broker})

for i in range(10):
    msg = f"Transaction {i}"
    p.produce(topic, msg.encode('utf-8'))
    print(f"Produced: {msg}")
    time.sleep(1)

p.flush()
