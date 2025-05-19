#!/usr/bin/env python
import json
import time
from datetime import datetime
import os
from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import pandas as pd
from utils.bigquery_connector import BigQueryConnector
from utils.monitoring import StreamingMonitor
import config

def process_avro_message(msg, bigquery_connector, monitor):
    """Xử lý một message Avro từ Kafka và lưu vào BigQuery"""
    start_time = time.time()
    
    try:
        # Lấy dữ liệu từ Avro message (đã được deserialize tự động)
        data = msg.value()
        
        # Xử lý dữ liệu đơn giản
        event_time = data.get('timestamp', datetime.now().isoformat())
        event_type = data.get('event_type', 'unknown')
        country = data.get('country', 'unknown')
        duration = data.get('duration_ms', 0)
        
        # Chuẩn bị dữ liệu để ghi vào BigQuery
        processed_data = {
            'event_time': event_time,
            'event_type': event_type,
            'country': country,
            'count': 1,  # Mỗi message là 1 sự kiện
            'avg_duration': duration
        }
        
        # Thêm các trường bổ sung nếu có (từ schema v2)
        if 'browser' in data:
            processed_data['browser'] = data.get('browser')
        if 'os' in data:
            processed_data['os'] = data.get('os')
        if 'device_type' in data:
            processed_data['device_type'] = data.get('device_type')
        
        # Ghi vào BigQuery
        df = pd.DataFrame([processed_data])
        bigquery_connector.insert_data(df)
        
        # Ghi nhận metrics
        processing_time = (time.time() - start_time) * 1000  # ms
        monitor.record_message(processing_time)
        
        print(f"Đã xử lý và lưu: {event_type} từ {country}")
        
    except Exception as e:
        print(f"Lỗi xử lý message: {e}")

def main():
    """Hàm chính để xử lý dữ liệu Avro từ Kafka"""
    # Thiết lập kết nối BigQuery
    bigquery_connector = BigQueryConnector()
    bigquery_connector.ensure_table_exists()
    
    # Thiết lập giám sát
    monitor = StreamingMonitor()
    monitor.start_monitoring()
    
    # Thiết lập Avro Consumer
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': config.CONSUMER_GROUP,
        'auto.offset.reset': 'latest',
        'schema.registry.url': config.SCHEMA_REGISTRY_URL
    }
    
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe([config.KAFKA_TOPIC])
    
    print(f"Bắt đầu lắng nghe từ Kafka topic {config.KAFKA_TOPIC} với định dạng Avro...")
    
    try:
        # Loop xử lý message
        while True:
            try:
                # Poll for message
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                # Xử lý message
                process_avro_message(msg, bigquery_connector, monitor)
                
            except SerializerError as e:
                # Lỗi khi deserialize (ví dụ: không tương thích schema)
                print(f"Message deserialization failed: {str(e)}")
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                
    except KeyboardInterrupt:
        print("Đã dừng processor")
    finally:
        consumer.close()
        monitor.stop_monitoring()
        print("Đã đóng kết nối")

if __name__ == "__main__":
    main()
