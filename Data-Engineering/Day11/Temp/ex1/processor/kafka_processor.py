import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd
from utils.bigquery_connector import BigQueryConnector
from utils.monitoring import StreamingMonitor
import config

def process_message(msg, bigquery_connector, monitor):
    """Xử lý một message từ Kafka và lưu vào BigQuery"""
    start_time = time.time()
    
    try:
        # Parse message
        data = json.loads(msg.value.decode('utf-8'))
        
        # Xử lý dữ liệu đơn giản (tổng hợp đôi chút)
        event_time = datetime.now().isoformat()
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
    """Hàm chính để xử lý dữ liệu từ Kafka"""
    # Thiết lập kết nối BigQuery
    bigquery_connector = BigQueryConnector()
    bigquery_connector.ensure_table_exists()
    
    # Thiết lập giám sát
    monitor = StreamingMonitor()
    monitor.start_monitoring()
    
    # Thiết lập Kafka consumer
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        group_id='python-processor-group'
    )
    
    print(f"Bắt đầu lắng nghe từ Kafka topic {config.KAFKA_TOPIC}...")
    
    try:
        # Loop xử lý message
        for msg in consumer:
            process_message(msg, bigquery_connector, monitor)
            
    except KeyboardInterrupt:
        print("Đã dừng processor")
    finally:
        consumer.close()
        monitor.stop_monitoring()
        print("Đã đóng kết nối")

if __name__ == "__main__":
    main()
