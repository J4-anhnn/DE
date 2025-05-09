#!/usr/bin/env python
import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from faker import Faker
import config
import os

fake = Faker()

class ClickStreamAvroProducer:
    def __init__(self, max_retries=10, retry_interval=5):
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        
        # Xác định phiên bản schema từ config
        schema_version = config.SCHEMA_VERSION
        schema_path = f'schemas/v{schema_version}/click_event.avsc'
        
        # Kiểm tra file schema tồn tại
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        # Đọc schema Avro
        with open(schema_path, 'r') as f:
            schema_str = f.read()
        
        self.value_schema = avro.loads(schema_str)
        
        # Tạo Avro Producer
        self.producer = self._connect_kafka()
        
        self.pages = ['/home', '/products', '/about', '/contact', '/cart', '/checkout']
        self.event_types = ['page_view', 'click', 'scroll', 'hover', 'form_submit']
        self.device_types = ['desktop', 'mobile', 'tablet']
        self.os_list = ['Windows', 'MacOS', 'Linux', 'iOS', 'Android']
        self.browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    
    def _connect_kafka(self):
        """Kết nối đến Kafka broker với cơ chế retry"""
        retries = 0
        while retries < self.max_retries:
            try:
                print(f"Đang kết nối đến Kafka và Schema Registry ({retries+1}/{self.max_retries})...")
                
                producer_config = {
                    'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
                    'schema.registry.url': config.SCHEMA_REGISTRY_URL
                }
                
                producer = AvroProducer(producer_config, 
                                        default_value_schema=self.value_schema)
                
                print("Đã kết nối thành công đến Kafka và Schema Registry!")
                return producer
            except Exception as e:
                retries += 1
                if retries >= self.max_retries:
                    raise Exception(f"Không thể kết nối đến Kafka sau {self.max_retries} lần thử: {str(e)}")
                print(f"Kafka hoặc Schema Registry chưa sẵn sàng. Thử lại sau {self.retry_interval} giây...")
                print(f"Lỗi: {str(e)}")
                time.sleep(self.retry_interval)
        
    def generate_click_data(self):
        """Tạo dữ liệu click stream giả lập theo schema Avro"""
        # Dữ liệu cơ bản cho schema v1
        data = {
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
        
        # Thêm các trường mới cho schema v2 (nếu đang sử dụng v2)
        if config.SCHEMA_VERSION >= 2:
            data.update({
                'browser': random.choice(self.browsers),
                'os': random.choice(self.os_list),
                'device_type': random.choice(self.device_types)
            })
            
        return data
    
    def delivery_report(self, err, msg):
        """Callback được gọi mỗi khi một message được gửi thành công hoặc thất bại"""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def send_data(self, num_messages=None):
        """Gửi dữ liệu đến Kafka topic với định dạng Avro"""
        count = 0
        try:
            print(f"Bắt đầu gửi dữ liệu Avro đến topic {config.KAFKA_TOPIC}...")
            print(f"Sử dụng schema phiên bản v{config.SCHEMA_VERSION}")
            
            while num_messages is None or count < num_messages:
                # Tạo dữ liệu
                data = self.generate_click_data()
                
                # Gửi dữ liệu với định dạng Avro
                self.producer.produce(
                    topic=config.KAFKA_TOPIC,
                    value=data,
                    callback=self.delivery_report
                )
                
                # Flush định kỳ để đảm bảo dữ liệu được gửi
                if count % 10 == 0:
                    self.producer.flush()
                
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
        """Đóng kết nối với Kafka"""
        self.producer.flush()
        print("Producer đã đóng kết nối với Kafka")

if __name__ == "__main__":
    producer = ClickStreamAvroProducer()
    try:
        producer.send_data()
    finally:
        producer.close()