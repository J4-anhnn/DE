provider "google" {
  project = var.project_id
  region  = var.region
}

# Create GCS bucket
resource "google_storage_bucket" "weather_data" {
  name          = "${var.project_id}-weather-data"
  location      = var.region
  force_destroy = true
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "weather_data" {
  dataset_id                  = "weather_data"
  friendly_name              = "Weather Data"
  description                = "Dataset for weather data"
  location                   = "US"
  default_table_expiration_ms = null
}

# Create BigQuery tables
resource "google_bigquery_table" "weather_raw" {
  dataset_id = google_bigquery_dataset.weather_data.dataset_id
  table_id   = "weather_raw"

  time_partitioning {
    type = "DAY"
    field = "timestamp"
  }

  schema = file("${path.module}/schemas/weather_raw.json")
}

resource "google_bigquery_table" "weather_streaming" {
  dataset_id = google_bigquery_dataset.weather_data.dataset_id
  table_id   = "weather_streaming"

  time_partitioning {
    type = "DAY"
    field = "timestamp"
  }

  schema = file("${path.module}/schemas/weather_streaming.json")
}
