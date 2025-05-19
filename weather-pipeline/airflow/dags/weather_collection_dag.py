from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging

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
    "Hai Phong",
    "Nha Trang",
    "Vinh",
    "Quy Nhon",
    "Buon Ma Thuot"
]

def fetch_weather_data(**context):
    """Fetch weather data from OpenWeather API"""
    api_key = os.getenv('OPENWEATHER_API_KEY')
    execution_date = context['execution_date']
    
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
            response.raise_for_status()
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
            
        except Exception as e:
            logging.error(f"Error fetching weather for {city}: {str(e)}")
            raise

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

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/weather_data/{{ ds_nodash }}/*.json',
        dst='raw/weather/{{ ds_nodash }}/',
        bucket='{{ var.value.gcs_bucket }}',
        gcp_conn_id='google_cloud_default',
    )

    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id='weather_data',
        location='US',
    )

    fetch_weather >> upload_to_gcs >> create_bq_dataset
