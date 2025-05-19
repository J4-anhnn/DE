"""
DAG to process weather data using Spark batch job
"""
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import uuid

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Generate a unique cluster name
def generate_cluster_name(**kwargs):
    cluster_name = f"weather-processing-{uuid.uuid4().hex[:8]}"
    kwargs['ti'].xcom_push(key='cluster_name', value=cluster_name)
    return cluster_name

with DAG(
    'weather_processing',
    default_args=default_args,
    description='Process weather data using Spark batch job',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Create BigQuery dataset if it doesn't exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='weather_data',
        location='US',
        exists_ok=True,
    )
    
    # Generate cluster name
    gen_cluster_name = PythonOperator(
        task_id='generate_cluster_name',
        python_callable=generate_cluster_name,
    )
    
    # Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id='{{ var.value.gcp_project_id }}',
        cluster_name='{{ ti.xcom_pull(task_ids="generate_cluster_name") }}',
        region='{{ var.value.gcp_region }}',
        cluster_config={
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-4',
                'disk_config': {'boot_disk_size_gb': 100}
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-4',
                'disk_config': {'boot_disk_size_gb': 100}
            }
        },
    )
    
    # Submit Spark job
    submit_job = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        project_id='{{ var.value.gcp_project_id }}',
        region='{{ var.value.gcp_region }}',
        job={
            'reference': {'job_id': 'weather-processing-{{ ds_nodash }}'},
            'placement': {'cluster_name': '{{ ti.xcom_pull(task_ids="generate_cluster_name") }}'},
            'pyspark_job': {
                'main_python_file_uri': 'gs://{{ var.value.code_bucket }}/spark/weather_batch_processing.py',
                'args': [
                    '--input_path=gs://{{ var.value.weather_raw_bucket }}/raw/weather/{{ ds_nodash }}/*',
                    '--output_table={{ var.value.gcp_project_id }}.weather_data.weather_batch',
                    '--processing_date={{ ds }}'
                ]
            }
        },
    )
    
    # Delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id='{{ var.value.gcp_project_id }}',
        cluster_name='{{ ti.xcom_pull(task_ids="generate_cluster_name") }}',
        region='{{ var.value.gcp_region }}',
        trigger_rule='all_done',  # Run even if upstream tasks fail
    )
    
    # Define task dependencies
    create_dataset >> gen_cluster_name >> create_cluster >> submit_job >> delete_cluster
