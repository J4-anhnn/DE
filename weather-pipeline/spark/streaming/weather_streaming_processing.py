#!/usr/bin/env python3
"""
Python script to process weather data from Kafka and send to BigQuery
This is a simplified version that uses kafka-python instead of Spark Structured Streaming
"""
import json
import time
import logging
import os
from kafka import KafkaConsumer
from google.cloud import bigquery
from datetime import datetime, timedelta

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherStreamProcessor:
    def __init__(self, max_retries=30, retry_interval=10):
        logger.info("Initializing Weather Stream Processor")
        
        # Wait for Kafka to be ready
        logger.info("Waiting for Kafka to be ready...")
        time.sleep(60)  # Give Kafka some time to start up
        
        # Khởi tạo Kafka consumer with retry
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka")
                self.consumer = KafkaConsumer(
                    'weather-raw',
                    bootstrap_servers=['kafka:9092'],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='weather-stream-processor',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("✅ Successfully connected to Kafka")
                break
            except Exception as e:
                logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_interval} seconds before next attempt...")
                    time.sleep(retry_interval)
                else:
                    logger.error("❌ Failed to connect to Kafka after multiple attempts")
                    raise
        
        # Khởi tạo BigQuery client
        try:
            self.bq_client = bigquery.Client()
            self.dataset_id = "weather_data"
            self.table_id = "weather_streaming_hourly"
            
            # Đảm bảo dataset và table tồn tại
            self.setup_bigquery()
            
            logger.info("Weather stream processor initialized")
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            # Continue without BigQuery if there's an error
            self.bq_client = None
            
        # Khởi tạo các biến để tính toán thống kê
        self.city_data = {}  # Lưu trữ dữ liệu theo thành phố
        self.window_size = timedelta(hours=1)  # Cửa sổ 1 giờ
    
    def setup_bigquery(self):
        """Tạo dataset và table nếu chưa tồn tại"""
        if not self.bq_client:
            logger.warning("BigQuery client not available, skipping setup")
            return
            
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            # Create dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = self.bq_client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
        
        # Định nghĩa schema cho table
        schema = [
            bigquery.SchemaField("city_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("window_start", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("window_end", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("avg_temperature", "FLOAT"),
            bigquery.SchemaField("max_temperature", "FLOAT"),
            bigquery.SchemaField("min_temperature", "FLOAT"),
            bigquery.SchemaField("avg_humidity", "FLOAT"),
            bigquery.SchemaField("avg_pressure", "FLOAT"),
            bigquery.SchemaField("processing_time", "TIMESTAMP", mode="REQUIRED")
        ]
        
        table_ref = dataset_ref.table(self.table_id)
        try:
            self.bq_client.get_table(table_ref)
            logger.info(f"Table {self.table_id} already exists")
        except Exception:
            # Create table
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="window_start"
            )
            table.clustering_fields = ["city_name"]
            
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    def add_to_window(self, city, data):
        """Add data to the current window for a city"""
        now = datetime.utcnow()
        window_start = datetime(now.year, now.month, now.day, now.hour)
        window_end = window_start + self.window_size
        
        if city not in self.city_data:
            self.city_data[city] = {}
        
        window_key = window_start.isoformat()
        if window_key not in self.city_data[city]:
            self.city_data[city][window_key] = {
                'window_start': window_start,
                'window_end': window_end,
                'temperatures': [],
                'humidities': [],
                'pressures': []
            }
        
        # Add data to window
        self.city_data[city][window_key]['temperatures'].append(data.get('temperature'))
        self.city_data[city][window_key]['humidities'].append(data.get('humidity'))
        self.city_data[city][window_key]['pressures'].append(data.get('pressure'))
        
        # Check if window is complete
        current_window = self.city_data[city][window_key]
        if now >= window_end and len(current_window['temperatures']) > 0:
            self.process_window(city, window_key)
    
    def process_window(self, city, window_key):
        """Process a completed window and send to BigQuery"""
        window_data = self.city_data[city][window_key]
        temperatures = [t for t in window_data['temperatures'] if t is not None]
        humidities = [h for h in window_data['humidities'] if h is not None]
        pressures = [p for p in window_data['pressures'] if p is not None]
        
        if not temperatures:
            logger.warning(f"No valid temperature data for {city} in window {window_key}")
            return
        
        # Calculate statistics
        avg_temp = sum(temperatures) / len(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidities) / len(humidities) if humidities else None
        avg_pressure = sum(pressures) / len(pressures) if pressures else None
        
        # Create row for BigQuery
        row = {
            'city_name': city,
            'window_start': window_data['window_start'].isoformat(),
            'window_end': window_data['window_end'].isoformat(),
            'avg_temperature': avg_temp,
            'max_temperature': max_temp,
            'min_temperature': min_temp,
            'avg_humidity': avg_humidity,
            'avg_pressure': avg_pressure,
            'processing_time': datetime.utcnow().isoformat()
        }
        
        # Log the statistics
        logger.info(f"Window statistics for {city} ({window_key}): " +
                   f"avg_temp={avg_temp:.1f}°C, " +
                   f"max_temp={max_temp:.1f}°C, " +
                   f"min_temp={min_temp:.1f}°C, " +
                   f"avg_humidity={avg_humidity:.1f}%, " +
                   f"avg_pressure={avg_pressure:.1f}hPa")
        
        # Insert into BigQuery if client is available
        if self.bq_client:
            try:
                table_ref = f"{self.bq_client.project}.{self.dataset_id}.{self.table_id}"
                errors = self.bq_client.insert_rows_json(table_ref, [row])
                
                if errors == []:
                    logger.info(f"Successfully inserted window data for {city}")
                else:
                    logger.error(f"Errors inserting window data: {errors}")
            except Exception as e:
                logger.error(f"Error inserting to BigQuery: {str(e)}")
        
        # Save to local file for backup
        os.makedirs("data/streaming", exist_ok=True)
        filename = f"data/streaming/{city.lower().replace(' ', '_')}_{window_key}.json"
        
        with open(filename, 'w') as f:
            json.dump(row, f, indent=2)
        
        logger.info(f"Saved window data to {filename}")
        
        # Remove processed window
        del self.city_data[city][window_key]
    
    def process_message(self, message):
        """Process a single message from Kafka"""
        try:
            city = message.get('city_name')
            if not city:
                logger.warning(f"Message missing city_name: {message}")
                return
            
            # Add to current window
            self.add_to_window(city, message)
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
    
    def run(self):
        """Run the stream processor"""
        logger.info("Starting to process messages...")
        
        try:
            for message in self.consumer:
                data = message.value
                logger.info(f"Received message: {data}")
                self.process_message(data)
                
                # Check for completed windows
                now = datetime.utcnow()
                for city in list(self.city_data.keys()):
                    for window_key in list(self.city_data[city].keys()):
                        window_end = self.city_data[city][window_key]['window_end']
                        if now >= window_end:
                            self.process_window(city, window_key)
        except KeyboardInterrupt:
            logger.info("Stream processor stopped by user")
        except Exception as e:
            logger.error(f"Error processing messages: {str(e)}")
        finally:
            self.consumer.close()
            logger.info("Stream processor closed")

def main():
    try:
        processor = WeatherStreamProcessor()
        processor.run()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        # Don't raise, just exit with error code
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
