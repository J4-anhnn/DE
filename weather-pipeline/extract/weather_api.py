"""
Utility module for interacting with OpenWeather API
"""
import os
import requests
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherAPI:
    """Class to interact with OpenWeather API with retry mechanism"""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize with API key from env var or parameter"""
        self.api_key = api_key or os.getenv('OPENWEATHER_API_KEY')
        if not self.api_key:
            raise ValueError("OpenWeather API key is required. Set OPENWEATHER_API_KEY environment variable or pass as parameter.")
        
        self.base_url = "http://api.openweathermap.org/data/2.5"
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
    def get_current_weather(self, city: str, units: str = "metric") -> Dict[str, Any]:
        """
        Get current weather for a city with exponential backoff retry
        
        Args:
            city: City name (e.g., "Hanoi")
            units: Units of measurement (metric, imperial, standard)
            
        Returns:
            Weather data as dictionary
        """
        url = f"{self.base_url}/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": units
        }
        
        try:
            logger.info(f"Fetching weather data for {city}")
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            
            data = response.json()
            # Add metadata
            data['collected_at'] = datetime.utcnow().isoformat()
            data['processing_date'] = datetime.utcnow().strftime("%Y-%m-%d")
            data['city_name'] = city  # Ensure city name is consistent
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather for {city}: {str(e)}")
            raise
    
    def get_weather_for_cities(self, cities: List[str], units: str = "metric") -> Dict[str, Dict[str, Any]]:
        """
        Get weather data for multiple cities
        
        Args:
            cities: List of city names
            units: Units of measurement
            
        Returns:
            Dictionary mapping city names to their weather data
        """
        results = {}
        for city in cities:
            try:
                results[city] = self.get_current_weather(city, units)
            except Exception as e:
                logger.error(f"Failed to get weather for {city}: {str(e)}")
                # Continue with other cities even if one fails
        
        return results
    
    def save_weather_data(self, data: Dict[str, Any], output_dir: str, city: str) -> str:
        """
        Save weather data to a JSON file
        
        Args:
            data: Weather data dictionary
            output_dir: Directory to save the file
            city: City name
            
        Returns:
            Path to the saved file
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Create filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/{city.lower().replace(' ', '_')}_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved weather data to {filename}")
        return filename
