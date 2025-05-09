import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'click_stream_data')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')

# BigQuery Configuration
BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'your-project-id')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'click_stream')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'processed_clicks')

# Consumer Configuration
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'avro-processor-group')

# Schema Configuration
SCHEMA_VERSION = int(os.getenv('SCHEMA_VERSION', '1'))  # Phiên bản schema mặc định
