import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'click_stream_data')

# BigQuery Configuration
BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'your-project-id')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'click_stream')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'processed_clicks')
