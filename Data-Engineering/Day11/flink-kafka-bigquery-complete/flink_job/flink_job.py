import json
from kafka import KafkaConsumer
from google.cloud import bigquery

# Kafka Consumer
consumer = KafkaConsumer(
    'click_topic',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='flink-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# BigQuery Client
bq_client = bigquery.Client()
dataset_id = 'your_dataset'
table_id = 'click_logs'

table_ref = bq_client.dataset(dataset_id).table(table_id)

for message in consumer:
    record = message.value
    row = {
        "user_id": record["user_id"],
        "timestamp": record["timestamp"],
        "action": record["action"]
    }
    errors = bq_client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"BigQuery insertion errors: {errors}")
    else:
        print(f"Inserted row: {row}")
