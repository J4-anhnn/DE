"""
Airflow DAG for uploading files to Google Cloud Storage
"""
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        GCP_PROJECT_ID, GCS_PROCESSED_BUCKET,
        AIRFLOW_DAG_OWNER, AIRFLOW_RETRY_DELAY, AIRFLOW_RETRY_COUNT
    )
except ImportError:
    # Fallback nếu không import được config
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    GCS_PROCESSED_BUCKET = os.environ.get('GCS_PROCESSED_BUCKET')
    AIRFLOW_DAG_OWNER = 'airflow'
    AIRFLOW_RETRY_DELAY = 300  # 5 phút
    AIRFLOW_RETRY_COUNT = 3

# Định nghĩa các tham số mặc định cho DAG
default_args = {
    'owner': AIRFLOW_DAG_OWNER,
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': AIRFLOW_RETRY_COUNT,
    'retry_delay': timedelta(seconds=AIRFLOW_RETRY_DELAY),
}

# Tạo DAG
dag = DAG(
    'gcs_upload',
    default_args=default_args,
    description='Upload files to Google Cloud Storage',
    schedule_interval=None,  # Chạy thủ công
    start_date=days_ago(1),
    tags=['gcs', 'upload'],
    catchup=False,
)

def prepare_files(**context):
    """
    Chuẩn bị danh sách file cần upload
    
    Args:
        context: Airflow context
        
    Returns:
        Dict chứa danh sách file và thư mục đích
    """
    # Lấy tham số từ context
    params = context['params']
    source_dir = params.get('source_dir', '/tmp/upload')
    destination_prefix = params.get('destination_prefix', 'uploads')
    
    # Kiểm tra thư mục nguồn
    if not os.path.exists(source_dir):
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    
    # Lấy danh sách file trong thư mục
    files = []
    for root, _, filenames in os.walk(source_dir):
        for filename in filenames:
            source_path = os.path.join(root, filename)
            relative_path = os.path.relpath(source_path, source_dir)
            destination_path = f"{destination_prefix}/{relative_path}"
            files.append({
                'source_path': source_path,
                'destination_path': destination_path
            })
    
    # Lưu danh sách file vào XCom
    context['ti'].xcom_push(key='files', value=files)
    context['ti'].xcom_push(key='file_count', value=len(files))
    
    return {
        'files': files,
        'file_count': len(files)
    }

# Task chuẩn bị file
prepare_files_task = PythonOperator(
    task_id='prepare_files',
    python_callable=prepare_files,
    provide_context=True,
    dag=dag,
)

def upload_file(source_path, destination_path, bucket_name, **context):
    """
    Upload một file lên GCS
    
    Args:
        source_path: Đường dẫn đến file nguồn
        destination_path: Đường dẫn đích trên GCS
        bucket_name: Tên bucket GCS
        context: Airflow context
    """
    from google.cloud import storage
    
    # Khởi tạo client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Upload file
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(source_path)
    
    return f"gs://{bucket_name}/{destination_path}"

def upload_files(**context):
    """
    Upload nhiều file lên GCS
    
    Args:
        context: Airflow context
    """
    # Lấy danh sách file từ XCom
    ti = context['ti']
    files = ti.xcom_pull(key='files', task_ids='prepare_files')
    
    if not files:
        raise ValueError("No files to upload")
    
    # Upload từng file
    uploaded_paths = []
    for file_info in files:
        source_path = file_info['source_path']
        destination_path = file_info['destination_path']
        
        gcs_path = upload_file(
            source_path=source_path,
            destination_path=destination_path,
            bucket_name=GCS_PROCESSED_BUCKET,
            **context
        )
        
        uploaded_paths.append(gcs_path)
    
    # Lưu đường dẫn đã upload vào XCom
    ti.xcom_push(key='uploaded_paths', value=uploaded_paths)
    
    return uploaded_paths

# Task upload file
upload_files_task = PythonOperator(
    task_id='upload_files',
    python_callable=upload_files,
    provide_context=True,
    dag=dag,
)

# Task thông báo hoàn thành
notify_task = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Uploaded {{ ti.xcom_pull(key="file_count", task_ids="prepare_files") }} files to GCS at $(date)"',
    dag=dag,
)

# Định nghĩa thứ tự thực hiện các task
prepare_files_task >> upload_files_task >> notify_task
