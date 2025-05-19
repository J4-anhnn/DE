from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Cấu hình cluster - giảm kích thước để tiết kiệm chi phí
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30}
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30}
    },
    "software_config": {
        "image_version": "2.0-debian10"
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 1800}  # Tự động xóa sau 30 phút không hoạt động
    }
}

# Cấu hình Spark job
PYSPARK_JOB = {
    "reference": {"project_id": "spartan-cosmos-457603-e0"},
    "placement": {"cluster_name": "weather-processing-cluster"},
    "pyspark_job": {
        "main_python_file_uri": "gs://weather-data-lake-2024/code/weather_batch_processing.py",
        "args": [
            "--input_path=gs://weather-data-lake-2024/raw/weather/",
            "--output_table=spartan-cosmos-457603-e0.weather_data.weather_streaming",  # Sửa tên bảng
            "--processing_date={{ ds }}"
        ],
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    }
}

with DAG(
    "weather_processing",
    default_args=default_args,
    description="Process weather data using Spark batch job",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id="spartan-cosmos-457603-e0",
        cluster_name="weather-processing-cluster",
        cluster_config=CLUSTER_CONFIG,
        region="us-central1",
        trigger_rule="all_done"
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id="spartan-cosmos-457603-e0",
        region="us-central1",
        job=PYSPARK_JOB,
        trigger_rule="all_done"
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id="spartan-cosmos-457603-e0",
        cluster_name="weather-processing-cluster",
        region="us-central1",
        trigger_rule="all_done"
    )

    create_cluster >> submit_job >> delete_cluster
