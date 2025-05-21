#!/bin/bash
# Script to set up the environment for the weather data pipeline

# Set up colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up environment for Weather Data Pipeline...${NC}"

# Create directories
echo -e "${GREEN}Creating necessary directories...${NC}"
mkdir -p data/raw data/processed logs creds

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file...${NC}"
    cat > .env << 'ENVFILE'
# Google Cloud Platform settings
GCP_PROJECT_ID=your-project-id
GCP_REGION=asia-southeast1
GCP_ZONE=asia-southeast1-a

# BigQuery settings
BIGQUERY_DATASET=weather_data

# GCS bucket settings
GCS_RAW_BUCKET=${GCP_PROJECT_ID}-weather-raw
GCS_PROCESSED_BUCKET=${GCP_PROJECT_ID}-weather-processed
GCS_TEMP_BUCKET=${GCP_PROJECT_ID}-weather-temp

# OpenWeather API settings
OPENWEATHER_API_KEY=your-api-key

# Airflow settings
AIRFLOW_UID=50000
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow
ENVFILE

    echo -e "${YELLOW}Please edit the .env file and fill in your GCP project ID and OpenWeather API key${NC}"
else
    echo -e "${GREEN}.env file already exists${NC}"
fi

# Check for GCP credentials
if [ ! -f creds/creds.json ]; then
    echo -e "${YELLOW}GCP credentials file not found at creds/creds.json${NC}"
    echo -e "${YELLOW}Please place your GCP service account key file at creds/creds.json${NC}"
else
    echo -e "${GREEN}GCP credentials file found${NC}"
fi

# Create requirements files
echo -e "${GREEN}Creating requirements files...${NC}"

# Main requirements
cat > requirements.txt << 'REQFILE'
google-cloud-storage==2.9.0
google-cloud-bigquery==3.9.0
requests==2.28.2
python-dotenv==1.0.0
fastapi==0.95.0
uvicorn==0.21.1
pydantic==1.10.7
REQFILE

# Spark requirements
cat > requirements-spark.txt << 'REQFILE'
pyspark==3.3.0
google-cloud-storage==2.9.0
google-cloud-bigquery==3.9.0
kafka-python==2.0.2
REQFILE

# Kafka requirements
cat > requirements-kafka.txt << 'REQFILE'
kafka-python==2.0.2
google-cloud-storage==2.9.0
REQFILE

echo -e "${GREEN}Environment setup complete!${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo -e "1. Edit the .env file with your GCP project ID and OpenWeather API key"
echo -e "2. Place your GCP service account key file at creds/creds.json"
echo -e "3. Run './run.sh terraform' to set up the infrastructure"
echo -e "4. Run './run.sh start' to start the services"

exit 0
