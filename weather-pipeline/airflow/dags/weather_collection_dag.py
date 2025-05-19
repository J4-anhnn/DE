from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

CITIES = [
    "Hanoi", 
    "Ho Chi Minh City", 
    "Da Nang", 
    "Hue", 
    "Can Tho",
    "Haiphong",
    "Nha Trang",
    "Vinh",
    "Quy Nhon",
    "Buon Ma Thuot"
]

def fetch_weather_data(**context):
    """Fetch weather data from OpenWeather API"""
    api_key = os.getenv('OPENWEATHER_API_KEY')
    execution_date = context['execution_date']
    
    successful_cities = 0
    failed_cities = 0
    
    for city in CITIES:
        try:
            # Call OpenWeather API
            url = f"http://api.openweathermap.org/data/2.5/weather"
            params = {
                "q": city,
                "appid": api_key,
                "units": "metric"
            }
            response = requests.get(url, params=params)
            
            # Thay vì raise_for_status(), kiểm tra status code
            if response.status_code == 200:
                data = response.json()
                
                # Add metadata
                data['collected_at'] = execution_date.isoformat()
                data['city_name'] = city
                
                # Save to local file
                output_dir = f"/tmp/weather_data/{execution_date.strftime('%Y/%m/%d')}"
                os.makedirs(output_dir, exist_ok=True)
                
                filename = f"{output_dir}/{city.lower().replace(' ', '_')}.json"
                with open(filename, 'w') as f:
                    json.dump(data, f)
                    
                logging.info(f"Saved weather data for {city} to {filename}")
                successful_cities += 1
            else:
                logging.warning(f"Could not fetch weather for {city}: {response.status_code} {response.reason}")
                failed_cities += 1
                
        except Exception as e:
            logging.error(f"Error fetching weather for {city}: {str(e)}")
            failed_cities += 1
    
    # Trả về thông tin tổng hợp
    return {
        'successful_cities': successful_cities,
        'failed_cities': failed_cities,
        'total_cities': len(CITIES)
    }

def upload_to_gcs(**context):
    """Upload files to Google Cloud Storage"""
    execution_date = context['execution_date']
    date_path = execution_date.strftime('%Y/%m/%d')
    local_path = f"/tmp/weather_data/{date_path}"
    
    # Kiểm tra thư mục tồn tại
    if not os.path.exists(local_path):
        logging.warning(f"Directory {local_path} does not exist. No files to upload.")
        return 0
    
    # Khởi tạo GCS client
    try:
        storage_client = storage.Client.from_service_account_json('/opt/airflow/creds/creds.json')
        bucket = storage_client.bucket('weather-data-lake-2024')
        
        # Tạo bucket nếu chưa tồn tại
        if not bucket.exists():
            bucket = storage_client.create_bucket('weather-data-lake-2024')
            logging.info(f"Created bucket: weather-data-lake-2024")
    except Exception as e:
        logging.error(f"Error initializing GCS client: {str(e)}")
        raise
    
    # Upload files
    uploaded_files = 0
    for filename in os.listdir(local_path):
        if filename.endswith('.json'):
            local_file = os.path.join(local_path, filename)
            gcs_path = f"raw/weather/{date_path}/{filename}"
            
            try:
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file)
                logging.info(f"Uploaded {local_file} to gs://weather-data-lake-2024/{gcs_path}")
                uploaded_files += 1
            except Exception as e:
                logging.error(f"Error uploading {local_file}: {str(e)}")
    
    logging.info(f"Uploaded {uploaded_files} files to GCS")
    return uploaded_files

with DAG(
    'weather_collection',
    default_args=default_args,
    description='Collect weather data and store in GCS',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    fetch_weather >> upload_to_gcs
