#!/bin/bash
# Script to start Airflow services

# Set up colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Load environment variables
if [ -f .env ]; then
    echo -e "${GREEN}Loading environment variables from .env file...${NC}"
    export $(grep -v '^#' .env | xargs)
else
    echo -e "${RED}Error: .env file not found. Please create one based on .env.example${NC}"
    exit 1
fi

# Check if AIRFLOW_UID is set
if [ -z "$AIRFLOW_UID" ]; then
    echo -e "${YELLOW}AIRFLOW_UID not set. Using default value 50000${NC}"
    export AIRFLOW_UID=50000
fi

# Start Airflow services
echo -e "${GREEN}Starting Airflow services...${NC}"
docker-compose -f docker-compose.airflow.yml up -d

# Check if services are running
echo -e "${GREEN}Checking if Airflow services are running...${NC}"
sleep 5
if docker-compose -f docker-compose.airflow.yml ps | grep -q "airflow-webserver.*Up"; then
    echo -e "${GREEN}Airflow services are running!${NC}"
    echo -e "${GREEN}Airflow UI is available at: http://localhost:8080${NC}"
    echo -e "${GREEN}Username: ${AIRFLOW_WWW_USER_USERNAME:-airflow}${NC}"
    echo -e "${GREEN}Password: ${AIRFLOW_WWW_USER_PASSWORD:-airflow}${NC}"
else
    echo -e "${RED}Error: Airflow services failed to start${NC}"
    docker-compose -f docker-compose.airflow.yml logs
    exit 1
fi

exit 0
