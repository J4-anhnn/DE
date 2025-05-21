"""
Cấu_hình_trung_tâm_cho_dự_án_Weather_Data_Pipeline
"""
import_os
import_logging
from_pathlib_import_Path
from_typing_import_Dict,_Optional,_List,_Any

#_Đường_dẫn_gốc_của_dự_án
PROJECT_ROOT_=_Path(__file__).parent.parent.absolute()

#_Cấu_hình_Google_Cloud
GCP_PROJECT_ID_=_os.environ.get('GCP_PROJECT_ID')
GCP_REGION_=_os.environ.get('GCP_REGION',_'asia-southeast1')
GCP_ZONE_=_os.environ.get('GCP_ZONE',_'asia-southeast1-a')

#_Cấu_hình_BigQuery
BIGQUERY_DATASET_=_os.environ.get('BIGQUERY_DATASET',_'weather_data')
BIGQUERY_TABLES_=_{
____'STREAMING':_'weather_streaming',
____'STREAMING_HOURLY':_'weather_streaming_hourly',
____'ANALYTICS':_'weather_analytics'
}

#_Cấu_hình_Google_Cloud_Storage
GCS_BUCKET_=_os.environ.get('GCS_BUCKET',_'weather-data-lake-2024')
#_Để_tương_thích_ngược_với_code_cũ
GCS_RAW_BUCKET_=_GCS_BUCKET
GCS_PROCESSED_BUCKET_=_GCS_BUCKET
GCS_TEMP_BUCKET_=_GCS_BUCKET

GCS_RAW_PREFIX_=_'raw'
GCS_PROCESSED_PREFIX_=_'processed'
GCS_TEMP_PREFIX_=_'temp'

#_Cấu_hình_Kafka
KAFKA_BOOTSTRAP_SERVERS_=_os.environ.get('KAFKA_BOOTSTRAP_SERVERS',_'kafka:9092')
KAFKA_TOPICS_=_{
____'RAW':_'weather-raw',
____'PROCESSED':_'weather-processed',
____'ALERTS':_'weather-alerts'
}

#_Cấu_hình_API
OPENWEATHER_API_KEY_=_os.environ.get('OPENWEATHER_API_KEY')
OPENWEATHER_API_URL_=_"https://api.openweathermap.org/data/2.5/weather"
CITIES_=_['Hanoi',_'Ho_Chi_Minh_City',_'Da_Nang',_'Can_Tho',_'Hue']

#_Cấu_hình_Airflow
AIRFLOW_DAG_OWNER_=_'weather_pipeline'
AIRFLOW_SCHEDULE_INTERVAL_=_os.environ.get('AIRFLOW_SCHEDULE_INTERVAL',_'0_*/3_*_*_*')__#_Mỗi_3_giờ
AIRFLOW_RETRY_DELAY_=_int(os.environ.get('AIRFLOW_RETRY_DELAY',_'300'))__#_5_phút
AIRFLOW_RETRY_COUNT_=_int(os.environ.get('AIRFLOW_RETRY_COUNT',_'3'))

#_Cấu_hình_logging
LOG_LEVEL_=_os.environ.get('LOG_LEVEL',_'INFO')
LOG_FORMAT_=_'%(asctime)s_-_%(name)s_-_%(levelname)s_-_%(message)s'

#_Đường_dẫn_đến_schema
SCHEMA_DIR_=_os.path.join(PROJECT_ROOT,_'load',_'schemas')

#_Cấu_hình_thời_tiết
WEATHER_ALERT_THRESHOLDS_=_{
____'temperature_high':_float(os.environ.get('WEATHER_ALERT_TEMP_HIGH',_'35')),
____'temperature_low':_float(os.environ.get('WEATHER_ALERT_TEMP_LOW',_'5')),
____'wind_speed_high':_float(os.environ.get('WEATHER_ALERT_WIND_HIGH',_'15')),
____'humidity_high':_float(os.environ.get('WEATHER_ALERT_HUMIDITY_HIGH',_'90'))
}

#_Cấu_hình_API_service
API_HOST_=_os.environ.get('API_HOST',_'0.0.0.0')
API_PORT_=_int(os.environ.get('API_PORT',_'8000'))
API_WORKERS_=_int(os.environ.get('API_WORKERS',_'4'))
API_RATE_LIMIT_=_int(os.environ.get('API_RATE_LIMIT',_'100'))__#_requests_per_minute

def_setup_logging(name:_str)_->_logging.Logger:
____"""Thiết_lập_và_trả_về_logger_với_cấu_hình_chuẩn"""
____level_=_getattr(logging,_LOG_LEVEL.upper(),_logging.INFO)
____logging.basicConfig(level=level,_format=LOG_FORMAT)
____return_logging.getLogger(name)

def_get_schema_path(table_name:_str)_->_str:
____"""Trả_về_đường_dẫn_đến_file_schema_cho_bảng_cụ_thể"""
____return_os.path.join(SCHEMA_DIR,_f"{table_name}_schema.json")

def_get_bigquery_table_id(table_key:_str)_->_str:
____"""Trả_về_ID_đầy_đủ_của_bảng_BigQuery"""
____table_name_=_BIGQUERY_TABLES.get(table_key.upper())
____if_not_table_name:
________raise_ValueError(f"Unknown_table_key:_{table_key}")
____return_f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

def_get_gcs_path(prefix:_str_=_"",_filename:_Optional[str]_=_None)_->_str:
____"""Tạo_đường_dẫn_GCS_đầy_đủ"""
____path_=_f"gs://{GCS_BUCKET}"
____if_prefix:
________path_=_f"{path}/{prefix}"
____if_filename:
________path_=_f"{path}/{filename}"
____return_path
