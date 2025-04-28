output "bucket_name" {
  value = google_storage_bucket.data_lake_bucket.name
}

output "bucket_location" {
  value = google_storage_bucket.data_lake_bucket.location
}
