# Terraform configuration for setting up BigQuery resources

provider "google" {
  project = var.project_id
  region  = var.region
}

# Create a dataset for weather data
resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Weather Data"
  description                 = "Dataset containing weather data and analytics"
  location                    = "US"  # Specify the location
  default_table_expiration_ms = 2592000000  # 30 days

  labels = {
    environment = var.environment
  }
}

# Create table for processed batch data
resource "google_bigquery_table" "weather_batch" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "weather_batch"
  
  time_partitioning {
    type          = "DAY"
    field         = "processing_date"
    expiration_ms = 7776000000  # 90 days
  }
  
  clustering = ["city_name", "weather_condition"]
  
  schema = file("${path.module}/schemas/weather_batch_schema.json")
  
  labels = {
    environment = var.environment
  }
}

# Create table for real-time streaming data
resource "google_bigquery_table" "weather_realtime" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "weather_realtime"
  
  time_partitioning {
    type          = "DAY"
    field         = "window.start"
    expiration_ms = 2592000000  # 30 days
  }
  
  clustering = ["city"]
  
  schema = file("${path.module}/schemas/weather_realtime_schema.json")
  
  labels = {
    environment = var.environment
  }
}

# Create table for weather analytics
resource "google_bigquery_table" "weather_analytics" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
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

# Create GCS bucket for raw data
resource "google_storage_bucket" "weather_raw_bucket" {
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

# Create GCS bucket for processed data
resource "google_storage_bucket" "weather_processed_bucket" {
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
