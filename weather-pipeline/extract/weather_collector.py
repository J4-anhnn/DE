"""
Module để thu thập và lưu trữ dữ liệu thời tiết
"""
import os
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import sys
from google.cloud import storage
import tempfile

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import CITIES, GCS_RAW_BUCKET, setup_logging
from extract.weather_api import WeatherAPI

logger = setup_logging(__name__)

class WeatherCollector:
    """Lớp để thu thập và lưu trữ dữ liệu thời tiết"""
    
    def __init__(self, api_key: Optional[str] = None, cities: Optional[List[str]] = None, 
                 gcs_bucket: Optional[str] = None):
        """
        Khởi tạo WeatherCollector
        
        Args:
            api_key: API key cho OpenWeather API
            cities: Danh sách thành phố cần thu thập dữ liệu
            gcs_bucket: Tên bucket GCS để lưu trữ dữ liệu
        """
        self.api = WeatherAPI(api_key)
        self.cities = cities or CITIES
        self.gcs_bucket = gcs_bucket or GCS_RAW_BUCKET
        self.storage_client = storage.Client()
        
        # Đảm bảo bucket tồn tại
        try:
            self.bucket = self.storage_client.get_bucket(self.gcs_bucket)
            logger.info(f"Using existing GCS bucket: {self.gcs_bucket}")
        except Exception as e:
            logger.warning(f"Error accessing bucket {self.gcs_bucket}: {str(e)}")
            logger.info(f"Creating new bucket: {self.gcs_bucket}")
            self.bucket = self.storage_client.create_bucket(self.gcs_bucket)
        
        logger.info(f"WeatherCollector initialized for cities: {', '.join(self.cities)}")
    
    def collect_and_save(self, local_dir: Optional[str] = None) -> Dict[str, str]:
        """
        Thu thập dữ liệu thời tiết và lưu vào GCS
        
        Args:
            local_dir: Thư mục để lưu bản sao cục bộ (nếu cần)
            
        Returns:
            Dict với key là tên thành phố và value là đường dẫn GCS
        """
        # Thu thập dữ liệu
        weather_data = self.api.get_weather_for_cities(self.cities)
        
        # Tạo timestamp để đặt tên file
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # Lưu trữ dữ liệu
        gcs_paths = {}
        for city, data in weather_data.items():
            # Tạo tên file
            city_slug = city.lower().replace(' ', '_')
            filename = f"{city_slug}_{timestamp}.json"
            
            # Lưu vào GCS
            blob = self.bucket.blob(f"raw/{filename}")
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type='application/json'
            )
            gcs_path = f"gs://{self.gcs_bucket}/raw/{filename}"
            gcs_paths[city] = gcs_path
            logger.info(f"Uploaded weather data for {city} to {gcs_path}")
            
            # Lưu bản sao cục bộ nếu cần
            if local_dir:
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, filename)
                with open(local_path, 'w') as f:
                    json.dump(data, f, indent=2)
                logger.info(f"Saved local copy to {local_path}")
        
        return gcs_paths
    
    def collect_to_temp_and_upload(self) -> Dict[str, str]:
        """
        Thu thập dữ liệu thời tiết vào thư mục tạm và tải lên GCS
        
        Returns:
            Dict với key là tên thành phố và value là đường dẫn GCS
        """
        # Thu thập dữ liệu
        weather_data = self.api.get_weather_for_cities(self.cities)
        
        # Tạo timestamp để đặt tên file
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # Lưu trữ dữ liệu
        gcs_paths = {}
        with tempfile.TemporaryDirectory() as temp_dir:
            for city, data in weather_data.items():
                # Tạo tên file
                city_slug = city.lower().replace(' ', '_')
                filename = f"{city_slug}_{timestamp}.json"
                temp_path = os.path.join(temp_dir, filename)
                
                # Lưu vào file tạm
                with open(temp_path, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Tải lên GCS
                blob = self.bucket.blob(f"raw/{filename}")
                blob.upload_from_filename(temp_path)
                
                gcs_path = f"gs://{self.gcs_bucket}/raw/{filename}"
                gcs_paths[city] = gcs_path
                logger.info(f"Uploaded weather data for {city} to {gcs_path}")
        
        return gcs_paths

def main():
    """Hàm chính để chạy module độc lập"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect weather data and save to GCS')
    parser.add_argument('--local-dir', help='Directory to save local copies')
    args = parser.parse_args()
    
    collector = WeatherCollector()
    gcs_paths = collector.collect_and_save(args.local_dir)
    
    print("\nWeather data collected and saved to GCS:")
    for city, path in gcs_paths.items():
        print(f"{city}: {path}")

if __name__ == "__main__":
    main()
