#!/usr/bin/env python3
"""
Kafka consumer for weather data

Consumes weather data from Kafka and processes it
"""
import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaConsumer
from google.cloud import storage

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, 
        GCS_PROCESSED_BUCKET, setup_logging
    )
    logger = setup_logging(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Fallback nếu không import được config
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPICS = {
        'RAW': 'weather-raw',
        'PROCESSED': 'weather-processed'
    }
    GCS_PROCESSED_BUCKET = os.environ.get('GCS_PROCESSED_BUCKET')

class WeatherConsumer:
    """Consumer for weather data from Kafka"""
    
    def __init__(self, bootstrap_servers=None, topic=None, group_id="weather-consumer"):
        """
        Initialize the weather consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPICS.get('RAW', 'weather-raw')
        self.group_id = group_id
        
        # Initialize Kafka consumer
        self.consumer = self._initialize_consumer()
        
        # Initialize GCS client if bucket is specified
        self.storage_client = None
        self.bucket = None
        if GCS_PROCESSED_BUCKET:
            try:
                self.storage_client = storage.Client()
                self.bucket = self.storage_client.bucket(GCS_PROCESSED_BUCKET)
                logger.info(f"GCS bucket initialized: {GCS_PROCESSED_BUCKET}")
            except Exception as e:
                logger.error(f"Error initializing GCS: {str(e)}")
        
        logger.info(f"Weather consumer initialized")
        logger.info(f"Consuming from topic: {self.topic}")
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with retry logic"""
        max_retries = 30
        retry_interval = 10
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka")
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.bootstrap_servers],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=self.group_id,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("✅ Successfully connected to Kafka")
                return consumer
            except Exception as e:
                logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_interval} seconds before next attempt...")
                    time.sleep(retry_interval)
                else:
                    logger.error("❌ Failed to connect to Kafka after multiple attempts")
                    raise
    
    def process_message(self, message):
        """
        Process a message from Kafka
        
        Args:
            message: Kafka message
        
        Returns:
            Processed data
        """
        data = message.value
        
        # Extract relevant fields
        city = data.get('city_name')
        if not city:
            logger.warning(f"Message missing city_name: {data}")
            return None
        
        # Extract weather data
        temperature = None
        humidity = None
        pressure = None
        wind_speed = None
        weather_condition = None
        timestamp = None
        
        # Handle different message formats
        if 'main' in data and isinstance(data['main'], dict):
            temperature = data['main'].get('temp')
            humidity = data['main'].get('humidity')
            pressure = data['main'].get('pressure')
            if 'wind' in data and isinstance(data['wind'], dict):
                wind_speed = data['wind'].get('speed')
            if 'weather' in data and isinstance(data['weather'], list) and len(data['weather']) > 0:
                weather_condition = data['weather'][0].get('main')
            timestamp = data.get('dt')
        else:
            temperature = data.get('temperature')
            humidity = data.get('humidity')
            pressure = data.get('pressure')
            wind_speed = data.get('wind_speed')
            weather_condition = data.get('weather_condition')
            timestamp = data.get('timestamp')
        
        # Use current time if timestamp is missing
        if not timestamp:
            timestamp = datetime.utcnow().isoformat()
        
        # Create processed data structure
        processed_data = {
            'city_name': city,
            'temperature': temperature,
            'humidity': humidity,
            'pressure': pressure,
            'wind_speed': wind_speed,
            'weather_condition': weather_condition,
            'timestamp': timestamp,
            'processing_time': datetime.utcnow().isoformat()
        }
        
        return processed_data
    
    def save_to_gcs(self, data):
        """
        Save processed data to GCS
        
        Args:
            data: Processed data
        """
        if not self.bucket:
            return
        
        try:
            # Create directory structure by date
            now = datetime.utcnow()
            date_str = now.strftime("%Y-%m-%d")
            city = data['city_name'].lower().replace(' ', '_')
            timestamp = now.strftime("%H%M%S")
            
            # Create filename
            filename = f"streaming/{date_str}/{city}_{timestamp}.json"
            
            # Upload to GCS
            blob = self.bucket.blob(filename)
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type='application/json'
            )
            
            logger.info(f"Saved data to GCS: gs://{self.bucket.name}/{filename}")
        except Exception as e:
            logger.error(f"Error saving to GCS: {str(e)}")
    
    def save_to_local(self, data, directory="data/processed"):
        """
        Save processed data to local file
        
        Args:
            data: Processed data
            directory: Directory to save to
        """
        try:
            # Create directory structure by date
            now = datetime.utcnow()
            date_str = now.strftime("%Y-%m-%d")
            city = data['city_name'].lower().replace(' ', '_')
            timestamp = now.strftime("%H%M%S")
            
            # Create directory if it doesn't exist
            os.makedirs(f"{directory}/{date_str}", exist_ok=True)
            
            # Create filename
            filename = f"{directory}/{date_str}/{city}_{timestamp}.json"
            
            # Save to file
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Saved data to local file: {filename}")
        except Exception as e:
            logger.error(f"Error saving to local file: {str(e)}")
    
    def consume_messages(self, count=None):
        """
        Consume messages from Kafka
        
        Args:
            count: Number of messages to consume (None for infinite)
        """
        message_count = 0
        
        try:
            for message in self.consumer:
                # Process message
                processed_data = self.process_message(message)
                
                if processed_data:
                    # Log processed data
                    city = processed_data['city_name']
                    temp = processed_data['temperature']
                    humidity = processed_data['humidity']
                    condition = processed_data['weather_condition']
                    logger.info(f"Processed data for {city}: {temp}°C, {humidity}%, {condition}")
                    
                    # Save to GCS if configured
                    if self.bucket:
                        self.save_to_gcs(processed_data)
                    
                    # Save to local file
                    self.save_to_local(processed_data)
                
                message_count += 1
                
                # Stop if we've reached the count
                if count is not None and message_count >= count:
                    break
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Consume weather data from Kafka')
    parser.add_argument('--count', type=int, help='Number of messages to consume (default: infinite)')
    args = parser.parse_args()
    
    consumer = WeatherConsumer()
    consumer.consume_messages(args.count)

if __name__ == "__main__":
    main()
