from confluent_kafka import Consumer

topic = "transactions"
broker = "kafka:9092"

consumer_conf = {
    'bootstrap.servers': broker,
    'group.id': 'group2',
    'auto.offset.reset': 'earliest'
}

c = Consumer(consumer_conf)
c.subscribe([topic])

print("consumer2 started and subscribed to topic:", topic)

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Error: {msg.error()}")
            continue

        print(f"✅ consumer2 received: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    print("consumer2 interrupted")
finally:
    c.close()
    print("consumer2 closed")
