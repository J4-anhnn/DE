"""
DAG to collect weather data from OpenWeather API and store in GCS
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import tempfile
import logging

# Import our utility module
from extract.weather_api import WeatherAPI

# Configure logging
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# List of cities to collect weather data for
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

def fetch_and_save_weather(**context):
    """
    Fetch weather data from OpenWeather API and save locally
    """
    # Get API key from Airflow variables
    api_key = Variable.get("openweather_api_key")
    
    # Create API client
    weather_api = WeatherAPI(api_key=api_key)
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix='weather_data_')
    logger.info(f"Created temp directory: {temp_dir}")
    
    # Get execution date for partitioning
    execution_date = context['execution_date']
    date_path = execution_date.strftime("%Y/%m/%d")
    
    # Dictionary to store file paths
    file_paths = {}
    
    # Fetch and save data for each city
    for city in CITIES:
        try:
            # Get weather data
            weather_data = weather_api.get_current_weather(city)
            
            # Add execution metadata
            weather_data['airflow_execution_date'] = execution_date.isoformat()
            weather_data['airflow_dag_run_id'] = context['run_id']
            
            # Save to file
            file_path = weather_api.save_weather_data(weather_data, temp_dir, city)
            file_paths[city] = file_path
            
            logger.info(f"Successfully processed weather data for {city}")
            
        except Exception as e:
            logger.error(f"Error processing {city}: {str(e)}")
            # Continue with other cities if one fails
    
    # Push the directory and file paths to XCom for the next task
    context['task_instance'].xcom_push(key='weather_data_dir', value=temp_dir)
    context['task_instance'].xcom_push(key='weather_data_files', value=file_paths)
    context['task_instance'].xcom_push(key='date_path', value=date_path)
    
    return file_paths

# Create the DAG
with DAG(
    'weather_collection',
    default_args=default_args,
    description='Collect weather data from OpenWeather API and store in GCS',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'extract'],
) as dag:
    
    # Task 1: Ensure GCS bucket exists
    ensure_bucket = GCSCreateBucketOperator(
        task_id='ensure_gcs_bucket',
        bucket_name="{{ var.value.weather_raw_bucket }}",
        project_id="{{ var.value.gcp_project_id }}",
        location="ASIA-SOUTHEAST1",
        gcp_conn_id="google_cloud_default",
        exists_ok=True
    )
    
    # Task 2: Fetch weather data and save locally
    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_and_save_weather
    )
    
    # Task 3: Upload data to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ task_instance.xcom_pull(task_ids='fetch_weather_data', key='weather_data_dir') }}/*",
        dst="raw/weather/{{ task_instance.xcom_pull(task_ids='fetch_weather_data', key='date_path') }}/",
        bucket="{{ var.value.weather_raw_bucket }}",
        gcp_conn_id="google_cloud_default"
    )
    
    # Define task dependencies
    ensure_bucket >> fetch_weather >> upload_to_gcs
