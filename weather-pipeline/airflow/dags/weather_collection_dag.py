"""
Airflow DAG for collecting weather data from OpenWeather API
"""
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from config.settings import (
        CITIES, OPENWEATHER_API_KEY, GCS_RAW_BUCKET,
        AIRFLOW_DAG_OWNER, AIRFLOW_SCHEDULE_INTERVAL, 
        AIRFLOW_RETRY_DELAY, AIRFLOW_RETRY_COUNT
    )
    from extract.weather_api import WeatherAPI
    from extract.weather_collector import WeatherCollector
except ImportError:
    # Fallback nếu không import được config
    CITIES = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Can Tho', 'Hue']
    OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY')
    GCS_RAW_BUCKET = os.environ.get('GCS_RAW_BUCKET')
    AIRFLOW_DAG_OWNER = 'airflow'
    AIRFLOW_SCHEDULE_INTERVAL = '0 */3 * * *'  # Mỗi 3 giờ
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
    'weather_collection',
    default_args=default_args,
    description='Collect weather data from OpenWeather API',
    schedule_interval=AIRFLOW_SCHEDULE_INTERVAL,
    start_date=days_ago(1),
    tags=['weather', 'data-collection'],
    catchup=False,
)

def collect_weather_data(**context):
    """
    Thu thập dữ liệu thời tiết từ OpenWeather API
    
    Args:
        context: Airflow context
        
    Returns:
        Dict với key là tên thành phố và value là đường dẫn GCS
    """
    # Lấy execution date từ context
    execution_date = context['execution_date']
    
    # Tạo thư mục lưu trữ tạm thời
    local_dir = f"/tmp/weather_data/{execution_date.strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(local_dir, exist_ok=True)
    
    # Thu thập dữ liệu
    collector = WeatherCollector(
        api_key=OPENWEATHER_API_KEY,
        cities=CITIES,
        gcs_bucket=GCS_RAW_BUCKET
    )
    
    # Thêm metadata Airflow
    context['ti'].xcom_push(key='execution_date', value=execution_date.isoformat())
    context['ti'].xcom_push(key='dag_run_id', value=context['run_id'])
    
    # Thu thập và lưu trữ dữ liệu
    gcs_paths = collector.collect_and_save(local_dir)
    
    # Lưu đường dẫn GCS vào XCom
    for city, path in gcs_paths.items():
        context['ti'].xcom_push(key=f'gcs_path_{city}', value=path)
    
    return gcs_paths

def check_data_quality(**context):
    """
    Kiểm tra chất lượng dữ liệu đã thu thập
    
    Args:
        context: Airflow context
        
    Returns:
        True nếu dữ liệu đạt chất lượng, False nếu không
    """
    # Lấy đường dẫn GCS từ XCom
    ti = context['ti']
    gcs_paths = {}
    
    for city in CITIES:
        path = ti.xcom_pull(key=f'gcs_path_{city}', task_ids='collect_weather_data')
        if path:
            gcs_paths[city] = path
    
    # Kiểm tra xem đã thu thập đủ dữ liệu cho tất cả các thành phố chưa
    if len(gcs_paths) != len(CITIES):
        missing_cities = set(CITIES) - set(gcs_paths.keys())
        raise ValueError(f"Missing data for cities: {', '.join(missing_cities)}")
    
    # Kiểm tra xem tất cả các đường dẫn GCS có hợp lệ không
    for city, path in gcs_paths.items():
        if not path.startswith('gs://'):
            raise ValueError(f"Invalid GCS path for {city}: {path}")
    
    return True

# Tạo task thu thập dữ liệu
collect_task = PythonOperator(
    task_id='collect_weather_data',
    python_callable=collect_weather_data,
    provide_context=True,
    dag=dag,
)

# Tạo task kiểm tra chất lượng dữ liệu
quality_check_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

# Tạo task thông báo hoàn thành
notify_task = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Weather data collection completed successfully at $(date)"',
    dag=dag,
)

# Định nghĩa thứ tự thực hiện các task
collect_task >> quality_check_task >> notify_task
