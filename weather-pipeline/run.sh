#!/bin/bash

# Check requirements
if [ ! -f ".env" ]; then
    echo "Error: .env file not found"
    exit 1
fi

if [ ! -f "creds/creds.json" ]; then
    echo "Error: Google Cloud credentials file not found at creds/creds.json"
    exit 1
fi

# Start services
echo "Starting services..."
docker-compose up --build -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Show logs
echo "Showing logs (Ctrl+C to exit log view, containers will continue running)"
docker-compose logs -f
