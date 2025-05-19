#!/usr/bin/env python3
"""
Script to check if data is available in BigQuery for dashboards
"""
import argparse
from google.cloud import bigquery

def check_data_availability(project_id, dataset_id):
    """Check if data is available in BigQuery views"""
    client = bigquery.Client(project=project_id)
    
    views = [
        "current_weather_view",
        "daily_weather_view",
        "weather_trends_view",
        "weather_alerts_view",
        "realtime_weather_view"
    ]
    
    print(f"Checking data availability in project {project_id}, dataset {dataset_id}...")
    
    for view in views:
        try:
            # Check if view exists
            view_ref = client.dataset(dataset_id).table(view)
            try:
                client.get_table(view_ref)
                print(f"✓ View {view} exists")
            except Exception as e:
                print(f"✗ View {view} does not exist: {str(e)}")
                continue
            
            # Check if view has data
            query = f"SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.{view}`"
            query_job = client.query(query)
            results = query_job.result()
            
            for row in results:
                if row.count > 0:
                    print(f"✓ View {view} has {row.count} records")
                else:
                    print(f"! View {view} exists but has no data")
        
        except Exception as e:
            print(f"✗ Error checking view {view}: {str(e)}")
    
    print("\nSample queries for each view:")
    
    sample_queries = {
        "current_weather_view": f"SELECT * FROM `{project_id}.{dataset_id}.current_weather_view` WHERE row_num = 1 LIMIT 10",
        "daily_weather_view": f"SELECT * FROM `{project_id}.{dataset_id}.daily_weather_view` ORDER BY date DESC LIMIT 10",
        "weather_trends_view": f"SELECT * FROM `{project_id}.{dataset_id}.weather_trends_view` ORDER BY date DESC LIMIT 10",
        "weather_alerts_view": f"SELECT * FROM `{project_id}.{dataset_id}.weather_alerts_view` LIMIT 10",
        "realtime_weather_view": f"SELECT * FROM `{project_id}.{dataset_id}.realtime_weather_view` ORDER BY window_start DESC LIMIT 10"
    }
    
    for view, query in sample_queries.items():
        print(f"\n{view}:")
        print(query)

def main():
    parser = argparse.ArgumentParser(description='Check data availability for Looker Studio dashboards')
    parser.add_argument('--project-id', required=True, help='Google Cloud Project ID')
    parser.add_argument('--dataset-id', default='weather_data', help='BigQuery Dataset ID')
    
    args = parser.parse_args()
    
    check_data_availability(args.project_id, args.dataset_id)

if __name__ == '__main__':
    main()
