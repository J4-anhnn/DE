#!/usr/bin/env python3
import os
import time
import json
import logging
from datetime import datetime
import requests
from google.cloud import bigquery
from google.cloud import storage

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Danh sách các thành phố
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

class WeatherCollector:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY environment variable is required")
        
        logger.info(f"Using API key: {self.api_key[:5]}...")
        
        # Khởi tạo BigQuery client
        try:
            self.bq_client = bigquery.Client()
            self.dataset_id = "weather_data"
            self.table_id = "weather_raw"
            
            # Đảm bảo dataset và table tồn tại
            self.setup_bigquery()
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {str(e)}")
            # Vẫn tiếp tục chạy, lưu dữ liệu vào file
    
    def setup_bigquery(self):
        """Tạo dataset và table nếu chưa tồn tại"""
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
            bigquery.SchemaField("feels_like", "FLOAT"),
            bigquery.SchemaField("humidity", "FLOAT"),
            bigquery.SchemaField("pressure", "FLOAT"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_direction", "FLOAT"),
            bigquery.SchemaField("cloudiness", "FLOAT"),
            bigquery.SchemaField("weather_condition", "STRING"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("measurement_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processing_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processing_date", "DATE", mode="REQUIRED")
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
                field="processing_date"
            )
            table.clustering_fields = ["city_name", "weather_condition"]
            
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    def get_weather_data(self, city):
        """Lấy dữ liệu thời tiết từ OpenWeather API"""
        url = f"http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Transform data
            now = datetime.utcnow()
            transformed_data = {
                "city_name": city,
                "temperature": data.get("main", {}).get("temp"),
                "feels_like": data.get("main", {}).get("feels_like"),
                "humidity": data.get("main", {}).get("humidity"),
                "pressure": data.get("main", {}).get("pressure"),
                "wind_speed": data.get("wind", {}).get("speed"),
                "wind_direction": data.get("wind", {}).get("deg"),
                "cloudiness": data.get("clouds", {}).get("all"),
                "weather_condition": data.get("weather", [{}])[0].get("main"),
                "weather_description": data.get("weather", [{}])[0].get("description"),
                "measurement_time": datetime.fromtimestamp(data.get("dt", time.time())).isoformat(),
                "processing_time": now.isoformat(),
                "processing_date": now.date().isoformat()
            }
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error fetching weather for {city}: {str(e)}")
            return None
    
    def save_to_file(self, data, city):
        """Lưu dữ liệu vào file"""
        os.makedirs("data", exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"data/{city.lower().replace(' ', '_')}_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved data to {filename}")
    
    def collect_and_store(self):
        """Thu thập và lưu dữ liệu thời tiết"""
        while True:
            rows_to_insert = []
            
            for city in CITIES:
                try:
                    data = self.get_weather_data(city)
                    if data:
                        rows_to_insert.append(data)
                        logger.info(f"Collected data for {city}: {data['temperature']}°C, {data['weather_condition']}")
                        
                        # Lưu vào file
                        self.save_to_file(data, city)
                except Exception as e:
                    logger.error(f"Error processing {city}: {str(e)}")
            
            if rows_to_insert and hasattr(self, 'bq_client'):
                try:
                    # Insert vào BigQuery
                    table_ref = f"{self.bq_client.project}.{self.dataset_id}.{self.table_id}"
                    errors = self.bq_client.insert_rows_json(table_ref, rows_to_insert)
                    
                    if errors == []:
                        logger.info(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery")
                    else:
                        logger.error(f"Errors inserting rows into BigQuery: {errors}")
                except Exception as e:
                    logger.error(f"Error inserting to BigQuery: {str(e)}")
            
            # Đợi 5 phút trước khi thu thập dữ liệu tiếp
            logger.info("Waiting 5sec before next collection...")
            time.sleep(5)

def main():
    try:
        logger.info("Weather collector starting...")
        collector = WeatherCollector()
        collector.collect_and_store()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
