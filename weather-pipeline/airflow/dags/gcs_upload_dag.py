from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging
from google.cloud import storage
import json
import re

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Định nghĩa các hàm xử lý
def initialize_gcs_client():
    """Khởi tạo Google Cloud Storage client"""
    try:
        # Sử dụng service account credentials
        client = storage.Client.from_service_account_json('/opt/airflow/creds/creds.json')
        return client
    except Exception as e:
        logger.error(f"Error initializing GCS client: {str(e)}")
        raise

def create_bucket_if_not_exists(client, bucket_name):
    """Tạo bucket nếu chưa tồn tại"""
    try:
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            bucket = client.create_bucket(bucket_name)
            logger.info(f"Created new bucket: {bucket_name}")
        return bucket
    except Exception as e:
        logger.error(f"Error creating/checking bucket: {str(e)}")
        raise

def upload_file_to_gcs(bucket, source_file, destination_blob_name):
    """Upload một file lên GCS"""
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)
        logger.info(f"Uploaded {source_file} to gs://{bucket.name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"Error uploading file {source_file}: {str(e)}")
        raise

def process_and_upload_files(local_dir="/opt/airflow/data", bucket_name="weather-data-lake-2024", **context):
    """Xử lý và upload tất cả file từ thư mục local"""
    try:
        # Khởi tạo GCS client
        client = initialize_gcs_client()
        
        # Tạo/lấy bucket
        bucket = create_bucket_if_not_exists(client, bucket_name)
        
        # Đếm số file đã xử lý
        processed_files = 0
        total_files = sum(len(files) for _, _, files in os.walk(local_dir))
        
        # Duyệt qua tất cả file trong thư mục
        for root, _, files in os.walk(local_dir):
            for file in files:
                if file.endswith('.json'):
                    try:
                        local_file_path = os.path.join(root, file)
                        
                        # Đọc file để lấy thông tin thời gian
                        with open(local_file_path, 'r') as f:
                            data = json.load(f)
                        
                        # Sử dụng regex để tìm mẫu ngày tháng trong tên file
                        match = re.search(r'(\d{8}-\d{6})', file)
                        if match:
                            date_str = match.group(1)
                            file_date = datetime.strptime(date_str, '%Y%m%d-%H%M%S')
                            
                            # Lấy tên thành phố từ phần đầu của tên file
                            city_name = file.split('_' + date_str)[0]
                            
                            # Tạo đường dẫn trên GCS theo cấu trúc: raw/weather/YYYY/MM/DD/city/
                            gcs_path = f"raw/weather/{file_date.strftime('%Y/%m/%d')}/{city_name}/{file}"
                            
                            # Upload file
                            upload_file_to_gcs(bucket, local_file_path, gcs_path)
                            
                            processed_files += 1
                            if processed_files % 100 == 0:
                                logger.info(f"Progress: {processed_files}/{total_files} files processed")
                        else:
                            logger.warning(f"Could not find date pattern in filename: {file}")
                            
                    except Exception as e:
                        logger.error(f"Error processing file {file}: {str(e)}")
                        continue
        
        logger.info(f"Completed! Total files processed: {processed_files}")
        return f"Processed {processed_files} files"
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'upload_weather_data_to_gcs',
    default_args=default_args,
    description='Upload weather data from local to GCS',
    schedule_interval='0 */1 * * *',  # Chạy mỗi 1 giờ
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'gcs', 'data-lake'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_weather_data',
        python_callable=process_and_upload_files,
        op_kwargs={
            'local_dir': '/opt/airflow/data',
            'bucket_name': 'weather-data-lake-2024'
        },
        provide_context=True,
    )
