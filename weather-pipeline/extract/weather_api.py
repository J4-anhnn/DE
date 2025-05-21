"""
Module để tương tác với OpenWeather API
"""
import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
import sys

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import OPENWEATHER_API_KEY, OPENWEATHER_API_URL, setup_logging

logger = setup_logging(__name__)

class WeatherAPI:
    """Lớp để tương tác với OpenWeather API"""
    
    def __init__(self, api_key: Optional[str] = None, max_retries: int = 3, retry_delay: int = 5):
        """
        Khởi tạo WeatherAPI
        
        Args:
            api_key: API key cho OpenWeather API. Nếu None, sẽ sử dụng từ biến môi trường
            max_retries: Số lần thử lại tối đa khi gặp lỗi
            retry_delay: Thời gian chờ giữa các lần thử lại (giây)
        """
        self.api_key = api_key or OPENWEATHER_API_KEY
        if not self.api_key:
            raise ValueError("API key is required. Set OPENWEATHER_API_KEY environment variable or pass it directly.")
        
        self.base_url = OPENWEATHER_API_URL
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        logger.info("WeatherAPI initialized")
    
    def get_current_weather(self, city: str, units: str = "metric") -> Dict[str, Any]:
        """
        Lấy dữ liệu thời tiết hiện tại cho một thành phố
        
        Args:
            city: Tên thành phố
            units: Đơn vị đo (metric, imperial, standard)
            
        Returns:
            Dict chứa dữ liệu thời tiết
        """
        params = {
            "q": city,
            "appid": self.api_key,
            "units": units
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching weather data for {city} (attempt {attempt+1}/{self.max_retries})")
                response = requests.get(self.base_url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                # Thêm metadata
                data["city_name"] = city
                data["collected_at"] = datetime.utcnow().isoformat()
                
                logger.info(f"Successfully fetched weather data for {city}")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error fetching weather data for {city}: {str(e)}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to fetch weather data for {city} after {self.max_retries} attempts")
                    raise
    
    def get_weather_for_cities(self, cities: List[str], units: str = "metric") -> Dict[str, Dict[str, Any]]:
        """
        Lấy dữ liệu thời tiết cho nhiều thành phố
        
        Args:
            cities: Danh sách tên thành phố
            units: Đơn vị đo (metric, imperial, standard)
            
        Returns:
            Dict với key là tên thành phố và value là dữ liệu thời tiết
        """
        results = {}
        errors = []
        
        for city in cities:
            try:
                results[city] = self.get_current_weather(city, units)
            except Exception as e:
                logger.error(f"Failed to get weather data for {city}: {str(e)}")
                errors.append(city)
        
        if errors:
            logger.warning(f"Failed to get weather data for {len(errors)} cities: {', '.join(errors)}")
        
        logger.info(f"Successfully fetched weather data for {len(results)} out of {len(cities)} cities")
        return results

def main():
    """Hàm chính để chạy module độc lập"""
    from config.settings import CITIES
    
    api = WeatherAPI()
    results = api.get_weather_for_cities(CITIES)
    
    # In kết quả
    for city, data in results.items():
        print(f"\n{city}:")
        print(f"Temperature: {data['main']['temp']}°C")
        print(f"Humidity: {data['main']['humidity']}%")
        print(f"Weather: {data['weather'][0]['main']} - {data['weather'][0]['description']}")
        print(f"Wind: {data['wind']['speed']} m/s")

if __name__ == "__main__":
    main()
