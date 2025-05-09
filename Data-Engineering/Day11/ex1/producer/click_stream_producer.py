import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import config

fake = Faker()

class ClickStreamProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.pages = ['/home', '/products', '/about', '/contact', '/cart', '/checkout']
        self.event_types = ['page_view', 'click', 'scroll', 'hover', 'form_submit']
        
    def generate_click_data(self):
        """Tạo dữ liệu click stream giả lập"""
        return {
            'event_id': str(uuid.uuid4()),
            'user_id': fake.uuid4(),
            'session_id': fake.uuid4(),
            'timestamp': datetime.now().isoformat(),
            'page': random.choice(self.pages),
            'event_type': random.choice(self.event_types),
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent(),
            'country': fake.country(),
            'city': fake.city(),
            'referrer': fake.uri(),
            'duration_ms': random.randint(100, 10000)
        }
    
    def send_data(self, num_messages=None):
        """Gửi dữ liệu đến Kafka topic"""
        count = 0
        try:
            print(f"Bắt đầu gửi dữ liệu đến topic {config.KAFKA_TOPIC}...")
            while num_messages is None or count < num_messages:
                data = self.generate_click_data()
                self.producer.send(config.KAFKA_TOPIC, data)
                print(f"Đã gửi: {data['event_type']} từ {data['country']} - {count+1}")
                
                # Chờ theo tốc độ đã cấu hình
                time.sleep(1.0 / config.PRODUCER_RATE)
                count += 1
                
        except KeyboardInterrupt:
            print("Đã dừng producer")
        finally:
            self.producer.flush()
            print(f"Đã gửi tổng cộng {count} messages")
    
    def close(self):
        """Đóng kết nối Kafka producer"""
        self.producer.close()

if __name__ == "__main__":
    producer = ClickStreamProducer()
    try:
        producer.send_data()
    finally:
        producer.close()
