"""
Kafka producer to simulate real-time weather data
"""
import json
import time
import os
import sys
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cities to generate weather data for
CITIES = [
    "Hanoi", 
    "Ho Chi Minh City", 
    "Da Nang", 
    "Hue", 
    "Can Tho",
    "Hai Phong",
    "Nha Trang",
    "Vinh",
    "Quy Nhon",
    "Buon Ma Thuot"
]

def create_kafka_producer(max_retries=10, wait_seconds=5):
    """Create Kafka producer with retry mechanism"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"✅ Connected to Kafka at {bootstrap_servers}")
            return producer
        except Exception as e:
            logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: Kafka not available yet ({e})")
            time.sleep(wait_seconds)
    
    logger.error("❌ Failed to connect to Kafka after multiple attempts.")
    sys.exit(1)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
def get_weather_data(city, api_key):
    """Get real weather data from OpenWeather API with retry"""
    url = f"http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Extract relevant fields
    return {
        "city": city,
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "wind_speed": data["wind"]["speed"],
        "weather_condition": data["weather"][0]["main"],
        "timestamp": time.time()
    }

def add_random_variation(data):
    """Add small random variations to simulate changing conditions"""
    data["temperature"] += random.uniform(-0.5, 0.5)
    data["humidity"] += random.uniform(-2, 2)
    data["pressure"] += random.uniform(-1, 1)
    data["wind_speed"] += random.uniform(-0.3, 0.3)
    data["timestamp"] = time.time()
    return data

def simulate_weather_data(city, base_data=None):
    """Simulate weather data for a city"""
    if base_data:
        return add_random_variation(base_data.copy())
    
    # Generate random data if no base data
    return {
        "city": city,
        "temperature": random.uniform(15, 35),
        "humidity": random.uniform(30, 90),
        "pressure": random.uniform(1000, 1020),
        "wind_speed": random.uniform(0, 10),
        "weather_condition": random.choice(["Clear", "Clouds", "Rain", "Thunderstorm"]),
        "timestamp": time.time()
    }

def produce_weather_data():
    """Main function to produce weather data to Kafka"""
    producer = create_kafka_producer()
    api_key = os.getenv("OPENWEATHER_API_KEY")
    use_real_data = api_key is not None
    
    # Store base data for each city
    city_data = {}
    
    # Initialize with real data if API key is available
    if use_real_data:
        logger.info("Using real weather data from OpenWeather API")
        for city in CITIES:
            try:
                city_data[city] = get_weather_data(city, api_key)
                logger.info(f"Initialized real data for {city}")
            except Exception as e:
                logger.error(f"Failed to get initial data for {city}: {e}")
                # Fall back to simulated data
                city_data[city] = simulate_weather_data(city)
    else:
        logger.info("Using simulated weather data (no API key provided)")
        for city in CITIES:
            city_data[city] = simulate_weather_data(city)
    
    # Main loop to produce data
    try:
        while True:
            for city in CITIES:
                # Get data with variation
                data = simulate_weather_data(city, city_data[city])
                city_data[city] = data  # Update base data
                
                # Check for extreme weather conditions and add alerts
                if data["temperature"] > 35:
                    data["alert"] = "EXTREME_HEAT"
                elif data["temperature"] < 5:
                    data["alert"] = "EXTREME_COLD"
                elif data["wind_speed"] > 15:
                    data["alert"] = "HIGH_WIND"
                elif data["humidity"] > 90:
                    data["alert"] = "HIGH_HUMIDITY"
                
                # Send to Kafka
                producer.send("weather-events", value=data)
                logger.info(f"Produced data for {city}: {data}")
            
            # Flush to ensure all messages are sent
            producer.flush()
            
            # Refresh real data occasionally if using API
            if use_real_data and random.random() < 0.1:  # 10% chance each cycle
                city = random.choice(CITIES)
                try:
                    city_data[city] = get_weather_data(city, api_key)
                    logger.info(f"Refreshed real data for {city}")
                except Exception as e:
                    logger.error(f"Failed to refresh data for {city}: {e}")
            
            # Wait before next batch
            time.sleep(5)  # 5 seconds between batches
            
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_weather_data()
