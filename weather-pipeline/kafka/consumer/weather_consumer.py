#!/usr/bin/env python3
"""
Kafka consumer for weather data
Reads data from Kafka topic and processes it
"""
import json
import os
import time
import sys
import logging
from kafka import KafkaConsumer
from google.cloud import bigquery

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherConsumer:
    def __init__(self, max_retries=30, retry_interval=10):
        logger.info("Initializing Weather Consumer")
        
        # Wait for Kafka to be ready
        logger.info("Waiting for Kafka to be ready...")
        time.sleep(30)  # Give Kafka some time to start up
        
        # Khởi tạo Kafka consumer with retry
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka")
                self.consumer = KafkaConsumer(
                    'weather-raw',
                    bootstrap_servers=['kafka:9092'],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='weather-consumer-group',
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
            self.table_id = "weather_streaming"
            
            # Đảm bảo dataset và table tồn tại
            self.setup_bigquery()
            
            logger.info("Weather consumer initialized")
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            # Continue without BigQuery if there's an error
            self.bq_client = None
    
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
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("humidity", "FLOAT"),
            bigquery.SchemaField("pressure", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
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
                field="timestamp"
            )
            table.clustering_fields = ["city_name"]
            
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    def process_message(self, message):
        """Process a single message from Kafka"""
        try:
            # Save to local file for backup
            os.makedirs("data", exist_ok=True)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            city = message.get('city_name', 'unknown').lower().replace(' ', '_')
            filename = f"data/{city}_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(message, f)
            
            logger.info(f"Saved data to {filename}")
            
            # Insert into BigQuery if client is available
            if self.bq_client:
                table_ref = f"{self.bq_client.project}.{self.dataset_id}.{self.table_id}"
                errors = self.bq_client.insert_rows_json(table_ref, [message])
                
                if errors == []:
                    logger.info(f"Successfully inserted data for {message['city_name']}")
                else:
                    logger.error(f"Errors inserting data: {errors}")
                    
            # Thực hiện phân tích dữ liệu (có thể mở rộng)
            if message.get('temperature') > 35:
                logger.warning(f"High temperature alert: {message['city_name']} - {message['temperature']}°C")
            elif message.get('temperature') < 10:
                logger.warning(f"Low temperature alert: {message['city_name']} - {message['temperature']}°C")
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
    
    def run(self):
        """Run the consumer"""
        logger.info("Starting to consume messages...")
        
        try:
            for message in self.consumer:
                data = message.value
                logger.info(f"Received message: {data}")
                self.process_message(data)
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")

def main():
    try:
        consumer = WeatherConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        # Don't raise, just exit with error code
        sys.exit(1)

if __name__ == "__main__":
    main()
