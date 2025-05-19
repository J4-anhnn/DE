#!/bin/bash

# Cấu hình
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"your-project-id"}
DATASET_ID=${BIGQUERY_DATASET:-"weather_data"}
BATCH_TABLE=${BIGQUERY_BATCH_TABLE:-"weather_batch"}
REALTIME_TABLE=${BIGQUERY_REALTIME_TABLE:-"weather_realtime"}

echo "Setting up BigQuery views for Looker Studio dashboards..."
echo "Project ID: $PROJECT_ID"
echo "Dataset ID: $DATASET_ID"
echo "Batch table: $BATCH_TABLE"
echo "Realtime table: $REALTIME_TABLE"

# Thay thế biến trong file SQL
sed -e "s/\${project_id}/$PROJECT_ID/g" \
    -e "s/\${dataset_id}/$DATASET_ID/g" \
    -e "s/\${batch_table}/$BATCH_TABLE/g" \
    -e "s/\${realtime_table}/$REALTIME_TABLE/g" \
    dashboard/sql/create_views.sql > dashboard/sql/create_views_processed.sql

# Thực thi SQL
bq query --use_legacy_sql=false < dashboard/sql/create_views_processed.sql

echo "BigQuery views created successfully!"
