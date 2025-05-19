# Weather Data Pipeline

A comprehensive weather data pipeline system with real-time and batch processing capabilities.

## System Architecture

The system is divided into 6 modules:

1. **Extract Module** - Data Ingestion from OpenWeather API
2. **Transform Module** - Data Processing with Spark
3. **Load Module** - Data Warehouse with BigQuery
4. **Kafka Real-time Module** - Real-time data streaming
5. **REST API Module** - BI Layer for data access
6. **Dashboard Module** - Visualization

## Setup Instructions

[Setup instructions will be added here]

## Module Details

### 1. Extract Module
- Collects weather data from OpenWeather API
- Stores raw data in Google Cloud Storage
- Implements retry mechanism for API failures

### 2. Transform Module
- Processes data using Spark (batch) and Spark Streaming
- Cleans, transforms, and analyzes weather data
- Outputs processed data to BigQuery

### 3. Load Module
- Manages BigQuery data warehouse
- Implements partitioning and clustering for performance
- Uses Terraform for infrastructure setup

### 4. Kafka Real-time Module
- Simulates real-time weather data
- Processes streaming data with Spark Streaming
- Triggers weather alerts for extreme conditions

### 5. REST API Module
- Provides endpoints for querying weather data
- Supports filtering by region, date, and weather metrics
- Implements authentication and rate limiting

### 6. Dashboard Module
- Visualizes weather data and trends
- Displays real-time updates
- Supports interactive filtering and exploration
