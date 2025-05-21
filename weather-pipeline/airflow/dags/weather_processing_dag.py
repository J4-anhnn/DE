"""
Airflow DAG for processing weather data with Spark
"""
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        GCP_PROJECT_ID, GCP_REGION, GCP_ZONE,
        GCS_RAW_BUCKET, GCS_PROCESSED_BUCKET, GCS_TEMP_BUCKET,
        BIGQUERY_DATASET, BIGQUERY_TABLES,
        AIRFLOW_DAG_OWNER, AIRFLOW_RETRY_DELAY, AIRFLOW_RETRY_COUNT
    )
except ImportError:
    # Fallback nếu không import được config
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    GCP_REGION = os.environ.get('GCP_REGION', 'asia-southeast1')
    GCP_ZONE = os.environ.get('GCP_ZONE', 'asia-southeast1-a')
    GCS_RAW_BUCKET = os.environ.get('GCS_RAW_BUCKET')
    GCS_PROCESSED_BUCKET = os.environ.get('GCS_PROCESSED_BUCKET')
    GCS_TEMP_BUCKET = os.environ.get('GCS_TEMP_BUCKET')
    BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'weather_data')
    BIGQUERY_TABLES = {
        'BATCH': 'weather_batch'
    }
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
    'weather_processing',
    default_args=default_args,
    description='Process weather data with Spark',
    schedule_interval='0 */6 * * *',  # Mỗi 6 giờ
    start_date=days_ago(1),
    tags=['weather', 'data-processing', 'spark'],
    catchup=False,
)

# Cấu hình Dataproc cluster
CLUSTER_NAME = 'weather-processing-cluster'
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "software_config": {
        "image_version": "2.0-debian10",
        "optional_components": ["JUPYTER"],
        "properties": {
            "spark:spark.executor.memory": "2g",
            "spark:spark.driver.memory": "2g",
        },
    },
}

# Tạo Dataproc cluster
create_cluster_task = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    cluster_config=CLUSTER_CONFIG,
    region=GCP_REGION,
    zone=GCP_ZONE,
    dag=dag,
)

def prepare_spark_job(**context):
    """
    Chuẩn bị tham số cho Spark job
    
    Args:
        context: Airflow context
        
    Returns:
        Dict chứa cấu hình Spark job
    """
    # Lấy execution date từ context
    execution_date = context['execution_date']
    processing_date = execution_date.strftime('%Y-%m-%d')
    
    # Đường dẫn đến dữ liệu đầu vào
    input_path = f"gs://{GCS_RAW_BUCKET}/raw/{processing_date}/*.json"
    
    # Bảng BigQuery đầu ra
    output_table = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLES.get('BATCH', 'weather_batch')}"
    
    # Cấu hình Spark job
    spark_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_PROCESSED_BUCKET}/spark/weather_batch_processing.py",
            "args": [
                "--input-path", input_path,
                "--output-table", output_table,
                "--processing-date", processing_date
            ],
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
        },
    }
    
    # Lưu cấu hình job vào XCom
    context['ti'].xcom_push(key='spark_job', value=spark_job)
    context['ti'].xcom_push(key='processing_date', value=processing_date)
    
    return spark_job

# Task chuẩn bị Spark job
prepare_job_task = PythonOperator(
    task_id='prepare_spark_job',
    python_callable=prepare_spark_job,
    provide_context=True,
    dag=dag,
)

# Task chạy Spark job
submit_job_task = DataprocSubmitJobOperator(
    task_id='submit_spark_job',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    job="{{ ti.xcom_pull(task_ids='prepare_spark_job', key='spark_job') }}",
    dag=dag,
)

# Task xóa Dataproc cluster
delete_cluster_task = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=GCP_REGION,
    trigger_rule='all_done',  # Luôn chạy, kể cả khi task trước đó thất bại
    dag=dag,
)

def generate_analytics(**context):
    """
    Tạo dữ liệu phân tích từ dữ liệu đã xử lý
    
    Args:
        context: Airflow context
    """
    # Import ở đây để tránh lỗi khi Airflow parse DAG
    from load.bigquery_loader import BigQueryLoader
    
    # Lấy processing_date từ XCom
    ti = context['ti']
    processing_date = ti.xcom_pull(key='processing_date', task_ids='prepare_spark_job')
    
    # Tạo dữ liệu phân tích
    loader = BigQueryLoader()
    row_count = loader.generate_analytics(processing_date)
    
    # Lưu kết quả vào XCom
    ti.xcom_push(key='analytics_row_count', value=row_count)
    
    return row_count

# Task tạo dữ liệu phân tích
analytics_task = PythonOperator(
    task_id='generate_analytics',
    python_callable=generate_analytics,
    provide_context=True,
    dag=dag,
)

# Task thông báo hoàn thành
notify_task = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Weather data processing completed successfully at $(date)"',
    dag=dag,
)

# Định nghĩa thứ tự thực hiện các task
create_cluster_task >> prepare_job_task >> submit_job_task >> delete_cluster_task >> analytics_task >> notify_task
