#!/bin/bash

echo "Waiting for Kafka to be ready..."
MAX_RETRIES=30
RETRY_INTERVAL=5

# Check if Kafka is up and running
for i in $(seq 1 $MAX_RETRIES); do
    if nc -z kafka 9092; then
        echo "Kafka is ready! Starting producer..."
        python click_stream_producer.py
        exit 0
    fi
    
    echo "Waiting for Kafka... attempt $i/$MAX_RETRIES"
    sleep $RETRY_INTERVAL
done

echo "Kafka is not available after $MAX_RETRIES attempts. Exiting."
exit 1
