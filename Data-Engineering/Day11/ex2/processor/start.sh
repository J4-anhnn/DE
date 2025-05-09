#!/bin/bash

echo "Waiting for Kafka and Schema Registry to be ready..."
MAX_RETRIES=30
RETRY_INTERVAL=5

# Check if Kafka and Schema Registry are up and running
for i in $(seq 1 $MAX_RETRIES); do
    if nc -z kafka 9092 && nc -z schema-registry 8081; then
        echo "Kafka and Schema Registry are ready! Starting processor..."
        python kafka_processor_avro.py
        exit 0
    fi
    
    echo "Waiting for services... attempt $i/$MAX_RETRIES"
    sleep $RETRY_INTERVAL
done

echo "Services are not available after $MAX_RETRIES attempts. Exiting."
exit 1
