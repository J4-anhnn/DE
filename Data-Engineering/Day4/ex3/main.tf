provider "google" {
  credentials = file("spartan-cosmos-457603-e0-b6068efd1827.json")
  project     = var.GOOGLE_PROJECT_ID
  region      = var.GOOGLE_REGION
}

resource "google_storage_bucket" "data_lake_bucket" {
  name          = "data-lake-bucket-${random_id.unique_id.hex}"
  location      = var.GOOGLE_REGION
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }
}

resource "google_storage_bucket_object" "file_upload" {
  name         = "myFile.txt"
  bucket       = google_storage_bucket.data_lake_bucket.name
  source       = "myFile.txt"
  content_type = "text/plain"
}

resource "random_id" "unique_id" {
  byte_length = 8
}


