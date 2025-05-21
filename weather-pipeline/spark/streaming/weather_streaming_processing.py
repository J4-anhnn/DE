#!/usr/bin/env python3
"""
Spark Streaming job to process weather data from Kafka and load into BigQuery
"""
import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from google.cloud import bigquery

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLES,
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, setup_logging
    )
    logger = setup_logging(__name__)
except ImportError:
    # Fallback nếu không import được config
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'weather_data')
    BIGQUERY_TABLES = {
        'STREAMING_HOURLY': 'weather_streaming_hourly'
    }
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPICS = {
        'RAW': 'weather-raw'
    }

class WeatherStreamProcessor:
    """Processor for streaming weather data from Kafka to BigQuery"""
    
    def __init__(self, max_retries=30, retry_interval=10):
        """
        Initialize the stream processor
        
        Args:
            max_retries: Maximum number of connection retries
            retry_interval: Seconds between retries
        """
        logger.info("Initializing Weather Stream Processor")
        
        # Wait for Kafka to be ready
        logger.info("Waiting for Kafka to be ready...")
        time.sleep(30)  # Give Kafka some time to start up
        
        # Initialize Kafka consumer with retry
        self.consumer = self._init_kafka_consumer(max_retries, retry_interval)
        
        # Initialize BigQuery client
        try:
            self.bq_client = bigquery.Client()
            self.project_id = GCP_PROJECT_ID
            self.dataset_id = BIGQUERY_DATASET
            self.table_id = BIGQUERY_TABLES.get('STREAMING_HOURLY', 'weather_streaming_hourly')
            
            # Check if BigQuery resources exist
            self._check_bigquery_resources()
            
            logger.info("Weather stream processor initialized")
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            self.bq_client = None
            
        # Initialize variables for window calculations
        self.city_data = {}  # Store data by city
        self.window_size = timedelta(hours=1)  # 1 hour window
    
    def _init_kafka_consumer(self, max_retries, retry_interval):
        """Initialize Kafka consumer with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka")
                consumer = KafkaConsumer(
                    KAFKA_TOPICS.get('RAW', 'weather-raw'),
                    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='weather-stream-processor',
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
    
    def _check_bigquery_resources(self):
        """Check if BigQuery dataset and table exist"""
        if not self.bq_client:
            logger.warning("BigQuery client not available, skipping resource check")
            return
            
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} exists")
        except Exception:
            logger.error(f"Dataset {self.dataset_id} does not exist. It should be created by Terraform.")
            raise
        
        table_ref = dataset_ref.table(self.table_id)
        try:
            self.bq_client.get_table(table_ref)
            logger.info(f"Table {self.table_id} exists")
        except Exception:
            logger.error(f"Table {self.table_id} does not exist. It should be created by Terraform.")
            raise
    
    def add_to_window(self, city, data):
        """
        Add data to the current window for a city
        
        Args:
            city: City name
            data: Weather data dictionary
        """
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
        """
        Process a completed window and send to BigQuery
        
        Args:
            city: City name
            window_key: Window key (ISO format of window start time)
        """
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
                table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
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
        """
        Process a single message from Kafka
        
        Args:
            message: Kafka message
        """
        try:
            data = message.value
            city = data.get('city_name')
            
            if not city:
                logger.warning(f"Message missing city_name: {data}")
                return
            
            # Extract relevant fields
            temperature = None
            humidity = None
            pressure = None
            
            # Handle different message formats
            if 'main' in data and isinstance(data['main'], dict):
                temperature = data['main'].get('temp')
                humidity = data['main'].get('humidity')
                pressure = data['main'].get('pressure')
            else:
                temperature = data.get('temperature')
                humidity = data.get('humidity')
                pressure = data.get('pressure')
            
            # Create simplified data structure
            processed_data = {
                'city_name': city,
                'temperature': temperature,
                'humidity': humidity,
                'pressure': pressure,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Add to current window
            self.add_to_window(city, processed_data)
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
    
    def run(self):
        """Run the stream processor"""
        logger.info("Starting to process messages...")
        
        try:
            for message in self.consumer:
                logger.info(f"Received message: {message.value}")
                self.process_message(message)
                
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
    """Main entry point"""
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
