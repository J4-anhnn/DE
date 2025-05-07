from confluent_kafka import Consumer

topic = "transactions"
broker = "kafka:9092"

consumer_conf = {
    'bootstrap.servers': broker,
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

c = Consumer(consumer_conf)
c.subscribe([topic])

print("🚀 consumer1 started and subscribed to topic:", topic)

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Error: {msg.error()}")
            continue

except KeyboardInterrupt:
    print("consumer1 interrupted")
finally:
    c.close()
    print("consumer1 closed")
