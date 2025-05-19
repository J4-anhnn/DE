variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  type        = string
  default     = "asia-southeast1"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "weather_data"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}
