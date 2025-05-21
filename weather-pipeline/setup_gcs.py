#!/usr/bin/env python3
"""
Script to check and create GCS bucket if needed
"""
import os
import sys
import argparse
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, Conflict

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
try:
    from config.settings import GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET, setup_logging
    logger = setup_logging(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Fallback nếu không import được config
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    GCP_REGION = os.environ.get('GCP_REGION', 'asia-southeast1')
    GCS_BUCKET = os.environ.get('GCS_BUCKET', 'weather-data-lake-2024')

# Đặt biến môi trường GOOGLE_APPLICATION_CREDENTIALS
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    # Thử các đường dẫn phổ biến
    possible_paths = [
        os.path.join(os.getcwd(), 'creds', 'creds.json'),
        os.path.join(os.path.dirname(__file__), 'creds', 'creds.json'),
        '/app/creds/creds.json'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
            logger.info(f"Using credentials from {path}")
            break
    else:
        logger.warning("Could not find credentials file automatically")

def check_and_create_bucket(bucket_name=None, region=None, project_id=None):
    """
    Kiểm tra xem bucket có tồn tại không và tạo mới nếu cần
    
    Args:
        bucket_name: Tên bucket, mặc định là GCS_BUCKET từ config
        region: Region cho bucket, mặc định là GCP_REGION từ config
        project_id: ID dự án, mặc định là GCP_PROJECT_ID từ config
        
    Returns:
        True nếu bucket đã tồn tại hoặc được tạo thành công, False nếu có lỗi
    """
    # Sử dụng giá trị mặc định từ config nếu không được chỉ định
    bucket_name = bucket_name or GCS_BUCKET
    region = region or GCP_REGION
    project_id = project_id or GCP_PROJECT_ID
    
    if not bucket_name:
        logger.error("Bucket name is required")
        return False
    
    if not project_id:
        logger.error("Project ID is required")
        return False
    
    try:
        # Khởi tạo client
        client = storage.Client(project=project_id)
        
        # Kiểm tra xem bucket có tồn tại không
        try:
            bucket = client.get_bucket(bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
            return True
        except Exception:
            logger.info(f"Bucket {bucket_name} does not exist, creating...")
            
            # Tạo bucket mới
            bucket = client.bucket(bucket_name)
            bucket.create(location=region)
            
            # Thiết lập lifecycle rules
            lifecycle_rules = [
                {
                    'action': {'type': 'Delete'},
                    'condition': {'age': 90}  # Xóa các object cũ hơn 90 ngày
                }
            ]
            bucket.lifecycle_rules = lifecycle_rules
            bucket.patch()
            
            logger.info(f"Successfully created bucket {bucket_name} in {region}")
            
            # Tạo các thư mục (prefixes)
            prefixes = ['raw', 'processed', 'temp']
            for prefix in prefixes:
                blob = bucket.blob(f"{prefix}/.keep")
                blob.upload_from_string('')
                logger.info(f"Created prefix {prefix}/ in bucket {bucket_name}")
            
            return True
    except Conflict:
        logger.warning(f"Bucket name {bucket_name} is already taken by another project")
        return False
    except GoogleCloudError as e:
        logger.error(f"Error with Google Cloud: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Check and create GCS bucket if needed')
    parser.add_argument('--bucket', help='Bucket name (default: from config)')
    parser.add_argument('--region', help='Region (default: from config)')
    parser.add_argument('--project', help='Project ID (default: from config)')
    parser.add_argument('--credentials', help='Path to credentials file')
    
    args = parser.parse_args()
    
    # Set credentials if provided
    if args.credentials:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.credentials
    
    success = check_and_create_bucket(args.bucket, args.region, args.project)
    
    if success:
        print(f"✅ Bucket {args.bucket or GCS_BUCKET} is ready to use")
        return 0
    else:
        print(f"❌ Failed to set up bucket {args.bucket or GCS_BUCKET}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
