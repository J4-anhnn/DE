#!/bin/bash

# Kiểm tra Python
command -v python3 >/dev/null 2>&1 || { echo "Error: python3 not found. Please install Python 3."; exit 1; }

# Thiết lập service account key
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/creds.json"
echo "Using service account key: $GOOGLE_APPLICATION_CREDENTIALS"

# Kiểm tra file key tồn tại
if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Error: Service account key file not found at $GOOGLE_APPLICATION_CREDENTIALS"
    exit 1
fi

# Lấy project ID từ file key
PROJECT_ID=$(cat "$GOOGLE_APPLICATION_CREDENTIALS" | python3 -c "import sys, json; print(json.load(sys.stdin)['project_id'])")
if [ -z "$PROJECT_ID" ]; then
    echo "Error: Could not extract project_id from service account key"
    exit 1
fi

# Cài đặt các thư viện Python cần thiết
echo "Installing required Python packages..."
python3 -m pip install --user google-cloud-bigquery

# Thiết lập các biến
DATASET_ID=${BIGQUERY_DATASET:-"weather_data"}
BATCH_TABLE=${BIGQUERY_BATCH_TABLE:-"weather_batch"}
REALTIME_TABLE=${BIGQUERY_REALTIME_TABLE:-"weather_realtime"}

echo "=== Weather Dashboard Setup ==="
echo "Project ID: $PROJECT_ID"
echo "Dataset ID: $DATASET_ID"
echo "Batch table: $BATCH_TABLE"
echo "Realtime table: $REALTIME_TABLE"

# Bước 1: Tạo các view trong BigQuery
echo -e "\n=== Step 1: Creating BigQuery views ==="
python3 ./dashboard/setup_views_python.py --project-id $PROJECT_ID --dataset-id $DATASET_ID --batch-table $BATCH_TABLE --realtime-table $REALTIME_TABLE

# Bước 2: Kiểm tra dữ liệu
echo -e "\n=== Step 2: Checking data availability ==="
python3 ./dashboard/check_data.py --project-id $PROJECT_ID --dataset-id $DATASET_ID

# Bước 3: Tạo hướng dẫn cho dashboard
echo -e "\n=== Step 3: Generating dashboard instructions ==="
python3 ./dashboard/create_dashboards.py --project-id $PROJECT_ID --dataset-id $DATASET_ID

echo -e "\n=== Dashboard Setup Complete ==="
echo "Please follow the instructions above to create your Looker Studio dashboards."
echo "For detailed instructions, see dashboard/README.md"
