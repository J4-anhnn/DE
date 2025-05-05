variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "training-de-457603"
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "asia-southeast1"
}

variable "instance_name" {
  description = "The name of the Cloud SQL instance"
  type        = string
  default     = "postgres-exercise"
}

variable "db_tier" {
  description = "The database instance tier"
  type        = string
  default     = "db-f1-micro"
}

variable "db_name" {
  description = "The name of the database"
  type        = string
  default     = "manhvd_exercise_db"
}

variable "db_user" {
  description = "The database user name"
  type        = string
  default     = "manhvd"
}

variable "db_password" {
  description = "The database user password"
  type        = string
  default     = "Exercise@2024"
  sensitive   = true
}

variable "app_name" {
  description = "The name of the Cloud Run service"
  type        = string
  default     = "node-postgres-app"
} 