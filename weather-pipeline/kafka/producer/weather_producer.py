#!/usr/bin/env python3
import json
import time
import os
import sys
import logging
import requests
from kafka import KafkaProducer
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_producer(max_retries=30, retry_interval=10):
    """Create Kafka producer with retry logic"""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info(f"✅ Connected to Kafka after {attempt+1} attempts")
            return producer
        except Exception as e:
            logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
            if attempt < max_retries - 1:
                logger.info(f"Waiting {retry_interval} seconds before next attempt...")
                time.sleep(retry_interval)
    
    logger.error("❌ Failed to connect to Kafka after multiple attempts")
    sys.exit(1)

def get_weather_data(city, api_key):
    """Get weather data from OpenWeather API"""
    url = f"http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching weather for {city}: {e}")
        return None

def produce_weather_data():
    """Produce weather data to Kafka"""
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(30)
    
    producer = create_producer()
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        logger.error("❌ OPENWEATHER_API_KEY environment variable not set")
        sys.exit(1)
    
    logger.info(f"Using API key: {api_key[:5]}...")
    
    cities = [
        "Hanoi", 
        "Ho Chi Minh City", 
        "Da Nang", 
        "Hue", 
        "Can Tho"
    ]
    
    while True:
        for city in cities:
            try:
                data = get_weather_data(city, api_key)
                if data:
                    message = {
                        'city_name': city,
                        'temperature': data['main']['temp'],
                        'humidity': data['main']['humidity'],
                        'pressure': data['main']['pressure'],
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    producer.send('weather-raw', value=message)
                    logger.info(f"✅ Sent data for {city}: {message['temperature']}°C, {message['humidity']}% humidity")
            except Exception as e:
                logger.error(f"❌ Error sending data for {city}: {e}")
        
        # Flush to ensure all messages are sent
        producer.flush()
        
        # Wait before next batch
        logger.info("⏳ Waiting 5sec before next data fetch...")
        time.sleep(5)  # 5 minutes

if __name__ == "__main__":
    logger.info("Weather producer starting...")
    produce_weather_data()
