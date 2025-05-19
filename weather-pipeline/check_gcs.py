#!/usr/bin/env python3
from google.cloud import storage
import sys
import os

def list_blobs(bucket_name, prefix=None):
    """Lists all the blobs in the bucket."""
    # Sử dụng credentials từ file creds.json
    creds_path = 'creds.json'
    if not os.path.exists(creds_path):
        creds_path = 'creds/creds.json'
    
    storage_client = storage.Client.from_service_account_json(creds_path)
    
    # Note: Client.list_blobs requires at least project and bucket.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    print(f"Files in bucket {bucket_name}:")
    count = 0
    for blob in blobs:
        print(f"- {blob.name}")
        count += 1
    
    print(f"\nTotal: {count} files")

if __name__ == "__main__":
    bucket_name = "weather-data-lake-2024"
    prefix = None
    
    if len(sys.argv) > 1:
        prefix = sys.argv[1]
    
    list_blobs(bucket_name, prefix)
