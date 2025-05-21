#!/usr/bin/env python3
"""
Script to upload files to GCS
"""
import os
import sys
import argparse
from pathlib import Path
from google.cloud import storage

def upload_file(bucket_name, source_file_path, destination_blob_name=None):
    """Upload a file to GCS bucket"""
    # If destination blob name is not specified, use the source file name
    if destination_blob_name is None:
        destination_blob_name = os.path.basename(source_file_path)
    
    # Initialize the client
    client = storage.Client()
    
    try:
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Create a blob and upload the file
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

def upload_directory(bucket_name, source_dir, destination_prefix=None):
    """Upload all files in a directory to GCS bucket"""
    # Initialize the client
    client = storage.Client()
    
    try:
        # Get the bucket
        bucket = client.bucket(bucket_name)
        
        # Walk through the directory
        success_count = 0
        error_count = 0
        
        for root, _, files in os.walk(source_dir):
            for file in files:
                source_file_path = os.path.join(root, file)
                
                # Calculate relative path
                rel_path = os.path.relpath(source_file_path, source_dir)
                
                # Determine destination blob name
                if destination_prefix:
                    destination_blob_name = f"{destination_prefix}/{rel_path}"
                else:
                    destination_blob_name = rel_path
                
                # Upload the file
                try:
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_filename(source_file_path)
                    print(f"Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")
                    success_count += 1
                except Exception as e:
                    print(f"Error uploading {source_file_path}: {e}")
                    error_count += 1
        
        print(f"\nUpload summary:")
        print(f"- Successfully uploaded: {success_count} files")
        print(f"- Failed to upload: {error_count} files")
        
        return error_count == 0
    except Exception as e:
        print(f"Error uploading directory: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Upload files to GCS')
    parser.add_argument('--bucket', required=True, help='GCS bucket name')
    parser.add_argument('--source', required=True, help='Source file or directory')
    parser.add_argument('--destination', help='Destination blob name or prefix')
    
    args = parser.parse_args()
    
    source_path = Path(args.source)
    
    if not source_path.exists():
        print(f"Error: Source path {args.source} does not exist")
        return 1
    
    if source_path.is_file():
        success = upload_file(args.bucket, str(source_path), args.destination)
    else:
        success = upload_directory(args.bucket, str(source_path), args.destination)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
