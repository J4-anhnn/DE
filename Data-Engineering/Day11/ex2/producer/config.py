import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'click_stream_data')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')

# Application Configuration
PRODUCER_RATE = float(os.getenv('PRODUCER_RATE', '5.0'))  # số sự kiện/giây

# Schema Configuration
SCHEMA_VERSION = int(os.getenv('SCHEMA_VERSION', '1'))