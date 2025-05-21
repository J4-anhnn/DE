# Terraform configuration for setting up BigQuery resources

provider "google" {
  project = var.project_id
  region  = var.region
}

# Import existing dataset if it exists
data "google_bigquery_dataset" "existing_dataset" {
  dataset_id = var.dataset_id
  project    = var.project_id
}

# Use local variable to reference the dataset ID
locals {
  dataset_id = data.google_bigquery_dataset.existing_dataset.dataset_id
}

# Check if tables exist
data "google_bigquery_table" "existing_streaming" {
  dataset_id = local.dataset_id
  table_id   = "weather_streaming"
  project    = var.project_id
  
  # This will fail silently if the table doesn't exist
  depends_on = [data.google_bigquery_dataset.existing_dataset]
}

data "google_bigquery_table" "existing_streaming_hourly" {
  dataset_id = local.dataset_id
  table_id   = "weather_streaming_hourly"
  project    = var.project_id
  
  # This will fail silently if the table doesn't exist
  depends_on = [data.google_bigquery_dataset.existing_dataset]
}

data "google_bigquery_table" "existing_analytics" {
  dataset_id = local.dataset_id
  table_id   = "weather_analytics"
  project    = var.project_id
  
  # This will fail silently if the table doesn't exist
  depends_on = [data.google_bigquery_dataset.existing_dataset]
}

# Create table for streaming data if it doesn't exist
resource "google_bigquery_table" "weather_streaming" {
  count = data.google_bigquery_table.existing_streaming.table_id == "weather_streaming" ? 0 : 1
  
  dataset_id = local.dataset_id
  table_id   = "weather_streaming"
  
  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 2592000000  # 30 days
  }
  
  clustering = ["city_name"]
  
  schema = file("${path.module}/schemas/weather_streaming_schema.json")
  
  labels = {
    environment = var.environment
  }
}

# Create table for streaming hourly data if it doesn't exist
resource "google_bigquery_table" "weather_streaming_hourly" {
  count = data.google_bigquery_table.existing_streaming_hourly.table_id == "weather_streaming_hourly" ? 0 : 1
  
  dataset_id = local.dataset_id
  table_id   = "weather_streaming_hourly"
  
  time_partitioning {
    type          = "DAY"
    field         = "window_start"
    expiration_ms = 2592000000  # 30 days
  }
  
  clustering = ["city_name"]
  
  schema = file("${path.module}/schemas/weather_streaming_hourly_schema.json")
  
  labels = {
    environment = var.environment
  }
}

# Create table for weather analytics if it doesn't exist
resource "google_bigquery_table" "weather_analytics" {
  count = data.google_bigquery_table.existing_analytics.table_id == "weather_analytics" ? 0 : 1
  
  dataset_id = local.dataset_id
  table_id   = "weather_analytics"
  
  time_partitioning {
    type          = "DAY"
    field         = "date"
    expiration_ms = 15552000000  # 180 days
  }
  
  clustering = ["city_name", "metric_type"]
  
  schema = file("${path.module}/schemas/weather_analytics_schema.json")
  
  labels = {
    environment = var.environment
  }
}

# Check if buckets exist
data "google_storage_bucket" "existing_raw_bucket" {
  name = "${var.project_id}-weather-raw"
}

data "google_storage_bucket" "existing_processed_bucket" {
  name = "${var.project_id}-weather-processed"
}

# Create GCS bucket for raw data if it doesn't exist
resource "google_storage_bucket" "weather_raw_bucket" {
  count = data.google_storage_bucket.existing_raw_bucket.name == "${var.project_id}-weather-raw" ? 0 : 1
  
  name          = "${var.project_id}-weather-raw"
  location      = var.region
  storage_class = "STANDARD"
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90  # days
    }
    action {
      type = "Delete"
    }
  }
}

# Create GCS bucket for processed data if it doesn't exist
resource "google_storage_bucket" "weather_processed_bucket" {
  count = data.google_storage_bucket.existing_processed_bucket.name == "${var.project_id}-weather-processed" ? 0 : 1
  
  name          = "${var.project_id}-weather-processed"
  location      = var.region
  storage_class = "STANDARD"
  
  lifecycle_rule {
    condition {
      age = 30  # days
    }
    action {
      type = "Delete"
    }
  }
}
