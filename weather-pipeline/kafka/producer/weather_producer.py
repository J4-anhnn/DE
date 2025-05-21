#!/usr/bin/env python3
"""
Kafka producer for weather data

Simulates real-time weather data and sends to Kafka
"""
import os
import sys
import json
import time
import random
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaProducer

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, CITIES, setup_logging
    )
    logger = setup_logging(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Fallback nếu không import được config
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPICS = {
        'RAW': 'weather-raw'
    }
    CITIES = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Can Tho', 'Hue']

class WeatherProducer:
    """Producer for simulated weather data"""
    
    def __init__(self, bootstrap_servers=None, topic=None, cities=None, interval=60):
        """
        Initialize the weather producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to produce to
            cities: List of cities to generate data for
            interval: Interval between messages in seconds
        """
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPICS.get('RAW', 'weather-raw')
        self.cities = cities or CITIES
        self.interval = interval
        
        # Initialize base weather data for each city
        self.city_base_data = self._initialize_city_data()
        
        # Initialize Kafka producer
        self.producer = self._initialize_producer()
        
        logger.info(f"Weather producer initialized for cities: {', '.join(self.cities)}")
        logger.info(f"Producing to topic: {self.topic}")
        logger.info(f"Message interval: {self.interval} seconds")
    
    def _initialize_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 30
        retry_interval = 10
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt+1}/{max_retries} to connect to Kafka")
                producer = KafkaProducer(
                    bootstrap_servers=[self.bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("✅ Successfully connected to Kafka")
                return producer
            except Exception as e:
                logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
                if attempt < max_retries - 1:
                    logger.info(f"Waiting {retry_interval} seconds before next attempt...")
                    time.sleep(retry_interval)
                else:
                    logger.error("❌ Failed to connect to Kafka after multiple attempts")
                    raise
    
    def _initialize_city_data(self) -> Dict[str, Dict[str, Any]]:
        """Initialize base weather data for each city"""
        city_data = {}
        
        # Define base temperature ranges for each city
        base_temps = {
            'Hanoi': (20, 35),
            'Ho Chi Minh City': (25, 38),
            'Da Nang': (22, 36),
            'Can Tho': (24, 37),
            'Hue': (21, 34)
        }
        
        # Define base humidity ranges for each city
        base_humidity = {
            'Hanoi': (60, 90),
            'Ho Chi Minh City': (65, 85),
            'Da Nang': (70, 95),
            'Can Tho': (75, 95),
            'Hue': (70, 90)
        }
        
        # Define base pressure ranges for each city
        base_pressure = {
            'Hanoi': (1000, 1015),
            'Ho Chi Minh City': (1005, 1020),
            'Da Nang': (1000, 1018),
            'Can Tho': (1002, 1016),
            'Hue': (1000, 1015)
        }
        
        # Define base wind speed ranges for each city
        base_wind = {
            'Hanoi': (0, 10),
            'Ho Chi Minh City': (0, 8),
            'Da Nang': (2, 15),
            'Can Tho': (0, 7),
            'Hue': (1, 12)
        }
        
        # Define possible weather conditions for each city
        weather_conditions = [
            'Clear', 'Clouds', 'Rain', 'Drizzle', 'Thunderstorm', 'Mist'
        ]
        
        # Initialize data for each city
        for city in self.cities:
            temp_range = base_temps.get(city, (20, 35))
            humidity_range = base_humidity.get(city, (60, 90))
            pressure_range = base_pressure.get(city, (1000, 1020))
            wind_range = base_wind.get(city, (0, 10))
            
            city_data[city] = {
                'base_temp': random.uniform(temp_range[0], temp_range[1]),
                'temp_range': temp_range,
                'base_humidity': random.uniform(humidity_range[0], humidity_range[1]),
                'humidity_range': humidity_range,
                'base_pressure': random.uniform(pressure_range[0], pressure_range[1]),
                'pressure_range': pressure_range,
                'base_wind': random.uniform(wind_range[0], wind_range[1]),
                'wind_range': wind_range,
                'weather_conditions': weather_conditions,
                'current_condition': random.choice(weather_conditions)
            }
        
        return city_data
    
    def generate_weather_data(self, city: str) -> Dict[str, Any]:
        """
        Generate simulated weather data for a city
        
        Args:
            city: City name
            
        Returns:
            Dictionary with weather data
        """
        base_data = self.city_base_data[city]
        
        # Add some random variation to base values
        temp_variation = random.uniform(-1.5, 1.5)
        humidity_variation = random.uniform(-5, 5)
        pressure_variation = random.uniform(-2, 2)
        wind_variation = random.uniform(-1, 1)
        
        # Calculate new values
        temperature = base_data['base_temp'] + temp_variation
        humidity = base_data['base_humidity'] + humidity_variation
        pressure = base_data['base_pressure'] + pressure_variation
        wind_speed = max(0, base_data['base_wind'] + wind_variation)
        
        # Occasionally change weather condition (10% chance)
        weather_condition = base_data['current_condition']
        if random.random() < 0.1:
            weather_condition = random.choice(base_data['weather_conditions'])
            base_data['current_condition'] = weather_condition
        
        # Create weather data object
        weather_data = {
            'city_name': city,
            'temperature': round(temperature, 1),
            'humidity': round(humidity, 1),
            'pressure': round(pressure, 1),
            'wind_speed': round(wind_speed, 1),
            'weather_condition': weather_condition,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Update base values with some drift (5% of the variation)
        base_data['base_temp'] += temp_variation * 0.05
        base_data['base_humidity'] += humidity_variation * 0.05
        base_data['base_pressure'] += pressure_variation * 0.05
        base_data['base_wind'] += wind_variation * 0.05
        
        # Keep values within range
        base_data['base_temp'] = max(base_data['temp_range'][0], min(base_data['temp_range'][1], base_data['base_temp']))
        base_data['base_humidity'] = max(base_data['humidity_range'][0], min(base_data['humidity_range'][1], base_data['base_humidity']))
        base_data['base_pressure'] = max(base_data['pressure_range'][0], min(base_data['pressure_range'][1], base_data['base_pressure']))
        base_data['base_wind'] = max(base_data['wind_range'][0], min(base_data['wind_range'][1], base_data['base_wind']))
        
        return weather_data
    
    def produce_messages(self, count=None):
        """
        Produce weather data messages to Kafka
        
        Args:
            count: Number of messages to produce (None for infinite)
        """
        message_count = 0
        
        try:
            while count is None or message_count < count:
                for city in self.cities:
                    # Generate weather data
                    weather_data = self.generate_weather_data(city)
                    
                    # Send to Kafka
                    self.producer.send(self.topic, weather_data)
                    logger.info(f"Sent data for {city}: {weather_data['temperature']}°C, {weather_data['humidity']}%, {weather_data['weather_condition']}")
                    
                    message_count += 1
                
                # Flush to ensure messages are sent
                self.producer.flush()
                
                # Wait for next interval
                if count is None or message_count < count:
                    time.sleep(self.interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Error producing messages: {str(e)}")
        finally:
            self.producer.close()
            logger.info("Producer closed")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Produce simulated weather data to Kafka')
    parser.add_argument('--interval', type=int, default=60, help='Interval between messages in seconds')
    parser.add_argument('--count', type=int, help='Number of messages to produce (default: infinite)')
    args = parser.parse_args()
    
    producer = WeatherProducer(interval=args.interval)
    producer.produce_messages(args.count)

if __name__ == "__main__":
    main()
