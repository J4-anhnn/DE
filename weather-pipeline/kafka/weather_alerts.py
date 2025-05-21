#!/usr/bin/env python3
"""
Weather alerts service

Monitors weather data and generates alerts for extreme conditions
"""
import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaConsumer, KafkaProducer

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, 
        WEATHER_ALERT_THRESHOLDS, setup_logging
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
        'ALERTS': 'weather-alerts'
    }
    WEATHER_ALERT_THRESHOLDS = {
        'temperature_high': 35.0,
        'temperature_low': 5.0,
        'wind_speed_high': 15.0,
        'humidity_high': 90.0
    }

class WeatherAlertService:
    """Service to monitor weather data and generate alerts"""
    
    def __init__(self, bootstrap_servers=None, input_topic=None, output_topic=None):
        """
        Initialize the weather alert service
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            input_topic: Kafka topic to consume from
            output_topic: Kafka topic to produce alerts to
        """
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.input_topic = input_topic or KAFKA_TOPICS.get('RAW', 'weather-raw')
        self.output_topic = output_topic or KAFKA_TOPICS.get('ALERTS', 'weather-alerts')
        
        # Initialize thresholds
        self.thresholds = WEATHER_ALERT_THRESHOLDS
        
        # Initialize Kafka consumer and producer
        self.consumer = self._initialize_consumer()
        self.producer = self._initialize_producer()
        
        # Track recent alerts to avoid duplicates
        self.recent_alerts = {}
        
        logger.info(f"Weather alert service initialized")
        logger.info(f"Consuming from topic: {self.input_topic}")
        logger.info(f"Producing alerts to topic: {self.output_topic}")
        logger.info(f"Alert thresholds: {self.thresholds}")
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with retry logic"""
        max_retries = 30
        retry_interval = 10
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka (consumer)")
                consumer = KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=[self.bootstrap_servers],
                    auto_offset_reset='latest',  # Only process new messages
                    enable_auto_commit=True,
                    group_id='weather-alerts',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("✅ Successfully connected to Kafka (consumer)")
                return consumer
            except Exception as e:
                logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_interval} seconds before next attempt...")
                    time.sleep(retry_interval)
                else:
                    logger.error("❌ Failed to connect to Kafka after multiple attempts")
                    raise
    
    def _initialize_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 30
        retry_interval = 10
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka (producer)")
                producer = KafkaProducer(
                    bootstrap_servers=[self.bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("✅ Successfully connected to Kafka (producer)")
                return producer
            except Exception as e:
                logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_interval} seconds before next attempt...")
                    time.sleep(retry_interval)
                else:
                    logger.error("❌ Failed to connect to Kafka after multiple attempts")
                    raise
    
    def check_alerts(self, data):
        """
        Check weather data for alert conditions
        
        Args:
            data: Weather data
            
        Returns:
            List of alerts
        """
        alerts = []
        city = data.get('city_name')
        timestamp = data.get('timestamp') or datetime.utcnow().isoformat()
        
        # Extract weather data
        temperature = None
        humidity = None
        wind_speed = None
        
        # Handle different message formats
        if 'main' in data and isinstance(data['main'], dict):
            temperature = data['main'].get('temp')
            humidity = data['main'].get('humidity')
            if 'wind' in data and isinstance(data['wind'], dict):
                wind_speed = data['wind'].get('speed')
        else:
            temperature = data.get('temperature')
            humidity = data.get('humidity')
            wind_speed = data.get('wind_speed')
        
        # Check temperature (high)
        if temperature is not None and temperature > self.thresholds['temperature_high']:
            alert_key = f"{city}_high_temp"
            # Only send alert if we haven't sent one in the last hour
            if self._should_send_alert(alert_key):
                alerts.append({
                    'city_name': city,
                    'alert_type': 'HIGH_TEMPERATURE',
                    'alert_level': 'critical' if temperature > self.thresholds['temperature_high'] + 3 else 'warning',
                    'message': f"High temperature detected: {temperature}°C",
                    'value': temperature,
                    'threshold': self.thresholds['temperature_high'],
                    'timestamp': timestamp
                })
        
        # Check temperature (low)
        if temperature is not None and temperature < self.thresholds['temperature_low']:
            alert_key = f"{city}_low_temp"
            if self._should_send_alert(alert_key):
                alerts.append({
                    'city_name': city,
                    'alert_type': 'LOW_TEMPERATURE',
                    'alert_level': 'critical' if temperature < self.thresholds['temperature_low'] - 3 else 'warning',
                    'message': f"Low temperature detected: {temperature}°C",
                    'value': temperature,
                    'threshold': self.thresholds['temperature_low'],
                    'timestamp': timestamp
                })
        
        # Check wind speed
        if wind_speed is not None and wind_speed > self.thresholds['wind_speed_high']:
            alert_key = f"{city}_high_wind"
            if self._should_send_alert(alert_key):
                alerts.append({
                    'city_name': city,
                    'alert_type': 'HIGH_WIND',
                    'alert_level': 'critical' if wind_speed > self.thresholds['wind_speed_high'] + 5 else 'warning',
                    'message': f"High wind speed detected: {wind_speed} m/s",
                    'value': wind_speed,
                    'threshold': self.thresholds['wind_speed_high'],
                    'timestamp': timestamp
                })
        
        # Check humidity
        if humidity is not None and humidity > self.thresholds['humidity_high']:
            alert_key = f"{city}_high_humidity"
            if self._should_send_alert(alert_key):
                alerts.append({
                    'city_name': city,
                    'alert_type': 'HIGH_HUMIDITY',
                    'alert_level': 'warning',
                    'message': f"High humidity detected: {humidity}%",
                    'value': humidity,
                    'threshold': self.thresholds['humidity_high'],
                    'timestamp': timestamp
                })
        
        return alerts
    
    def _should_send_alert(self, alert_key):
        """
        Check if we should send an alert (avoid duplicates)
        
        Args:
            alert_key: Unique key for the alert
            
        Returns:
            True if we should send the alert, False otherwise
        """
        now = datetime.utcnow()
        
        # If we've sent this alert in the last hour, don't send it again
        if alert_key in self.recent_alerts:
            last_sent = self.recent_alerts[alert_key]
            if (now - last_sent).total_seconds() < 3600:  # 1 hour
                return False
        
        # Update the last sent time
        self.recent_alerts[alert_key] = now
        return True
    
    def run(self):
        """Run the alert service"""
        logger.info("Starting weather alert service...")
        
        try:
            for message in self.consumer:
                data = message.value
                
                # Check for alerts
                alerts = self.check_alerts(data)
                
                # Send alerts
                for alert in alerts:
                    self.producer.send(self.output_topic, alert)
                    logger.warning(f"ALERT: {alert['alert_type']} in {alert['city_name']}: {alert['message']} ({alert['alert_level']})")
                
                # Flush producer to ensure messages are sent
                if alerts:
                    self.producer.flush()
                
        except KeyboardInterrupt:
            logger.info("Alert service stopped by user")
        except Exception as e:
            logger.error(f"Error in alert service: {str(e)}")
        finally:
            self.consumer.close()
            self.producer.close()
            logger.info("Alert service closed")

def main():
    """Main entry point"""
    alert_service = WeatherAlertService()
    alert_service.run()

if __name__ == "__main__":
    main()
