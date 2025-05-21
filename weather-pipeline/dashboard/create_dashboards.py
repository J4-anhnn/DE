#!/usr/bin/env python3
"""
Script to create Looker Studio dashboards for weather data
"""
import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from config.settings import (
        GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLES,
        CITIES, setup_logging
    )
    logger = setup_logging(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Fallback nếu không import được config
    GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'weather_data')
    BIGQUERY_TABLES = {
        'BATCH': 'weather_batch',
        'STREAMING': 'weather_streaming',
        'STREAMING_HOURLY': 'weather_streaming_hourly',
        'ANALYTICS': 'weather_analytics'
    }
    CITIES = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Can Tho', 'Hue']

class DashboardCreator:
    """Class to create and manage Looker Studio dashboards"""
    
    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize the dashboard creator
        
        Args:
            credentials_path: Path to service account credentials JSON file
        """
        self.project_id = GCP_PROJECT_ID
        self.dataset_id = BIGQUERY_DATASET
        self.tables = BIGQUERY_TABLES
        
        # Load credentials if provided
        self.credentials = None
        if credentials_path:
            try:
                self.credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                logger.info(f"Loaded credentials from {credentials_path}")
            except Exception as e:
                logger.error(f"Error loading credentials: {str(e)}")
        
        logger.info(f"Dashboard creator initialized for project {self.project_id}")
    
    def create_dashboard_config(
        self, 
        dashboard_title: str, 
        description: str, 
        data_source_id: str
    ) -> Dict[str, Any]:
        """
        Create dashboard configuration
        
        Args:
            dashboard_title: Title of the dashboard
            description: Description of the dashboard
            data_source_id: ID of the data source
            
        Returns:
            Dashboard configuration
        """
        # Create a timestamp for the dashboard ID
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        dashboard_id = f"weather_dashboard_{timestamp}"
        
        # Create dashboard configuration
        dashboard_config = {
            "dashboard": {
                "id": dashboard_id,
                "title": dashboard_title,
                "description": description,
                "dataSources": [
                    {
                        "id": data_source_id,
                        "type": "BIGQUERY",
                        "config": {
                            "projectId": self.project_id,
                            "datasetId": self.dataset_id,
                            "tableId": self.tables.get('ANALYTICS', 'weather_analytics')
                        }
                    }
                ],
                "reports": [
                    self._create_temperature_report(data_source_id),
                    self._create_humidity_report(data_source_id),
                    self._create_city_comparison_report(data_source_id)
                ]
            }
        }
        
        return dashboard_config
    
    def _create_temperature_report(self, data_source_id: str) -> Dict[str, Any]:
        """
        Create temperature report configuration
        
        Args:
            data_source_id: ID of the data source
            
        Returns:
            Report configuration
        """
        return {
            "id": "temperature_report",
            "title": "Temperature Trends",
            "description": "Temperature trends over time by city",
            "charts": [
                {
                    "id": "temp_time_series",
                    "type": "TIME_SERIES",
                    "title": "Temperature Over Time",
                    "dataSourceId": data_source_id,
                    "dimensions": ["date"],
                    "metrics": ["avg_value"],
                    "filters": [
                        {
                            "fieldName": "metric_type",
                            "operator": "EQUAL",
                            "values": ["temperature"]
                        }
                    ],
                    "breakdowns": ["city_name"]
                },
                {
                    "id": "temp_heatmap",
                    "type": "HEATMAP",
                    "title": "Temperature Heatmap by City and Date",
                    "dataSourceId": data_source_id,
                    "dimensions": ["date", "city_name"],
                    "metrics": ["avg_value"],
                    "filters": [
                        {
                            "fieldName": "metric_type",
                            "operator": "EQUAL",
                            "values": ["temperature"]
                        }
                    ]
                }
            ]
        }
    
    def _create_humidity_report(self, data_source_id: str) -> Dict[str, Any]:
        """
        Create humidity report configuration
        
        Args:
            data_source_id: ID of the data source
            
        Returns:
            Report configuration
        """
        return {
            "id": "humidity_report",
            "title": "Humidity Analysis",
            "description": "Humidity patterns and analysis",
            "charts": [
                {
                    "id": "humidity_time_series",
                    "type": "TIME_SERIES",
                    "title": "Humidity Over Time",
                    "dataSourceId": data_source_id,
                    "dimensions": ["date"],
                    "metrics": ["avg_value"],
                    "filters": [
                        {
                            "fieldName": "metric_type",
                            "operator": "EQUAL",
                            "values": ["humidity"]
                        }
                    ],
                    "breakdowns": ["city_name"]
                },
                {
                    "id": "humidity_bar_chart",
                    "type": "BAR",
                    "title": "Average Humidity by City",
                    "dataSourceId": data_source_id,
                    "dimensions": ["city_name"],
                    "metrics": ["avg_value"],
                    "filters": [
                        {
                            "fieldName": "metric_type",
                            "operator": "EQUAL",
                            "values": ["humidity"]
                        }
                    ]
                }
            ]
        }
    
    def _create_city_comparison_report(self, data_source_id: str) -> Dict[str, Any]:
        """
        Create city comparison report configuration
        
        Args:
            data_source_id: ID of the data source
            
        Returns:
            Report configuration
        """
        return {
            "id": "city_comparison",
            "title": "City Weather Comparison",
            "description": "Compare weather metrics across cities",
            "charts": [
                {
                    "id": "city_metrics_table",
                    "type": "TABLE",
                    "title": "Weather Metrics by City",
                    "dataSourceId": data_source_id,
                    "dimensions": ["city_name", "metric_type"],
                    "metrics": ["avg_value", "min_value", "max_value", "std_dev"]
                },
                {
                    "id": "city_radar_chart",
                    "type": "RADAR",
                    "title": "City Weather Profile",
                    "dataSourceId": data_source_id,
                    "dimensions": ["metric_type"],
                    "metrics": ["avg_value"],
                    "breakdowns": ["city_name"]
                }
            ]
        }
    
    def save_dashboard_config(self, config: Dict[str, Any], output_path: str) -> None:
        """
        Save dashboard configuration to file
        
        Args:
            config: Dashboard configuration
            output_path: Path to save the configuration
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info(f"Dashboard configuration saved to {output_path}")
        except Exception as e:
            logger.error(f"Error saving dashboard configuration: {str(e)}")
            raise
    
    def create_dashboard_instructions(self, output_path: str) -> None:
        """
        Create instructions for setting up the dashboard manually
        
        Args:
            output_path: Path to save the instructions
        """
        instructions = {
            "title": "Weather Dashboard Setup Instructions",
            "created_at": datetime.now().isoformat(),
            "steps": [
                {
                    "step": 1,
                    "title": "Create a new Looker Studio report",
                    "description": "Go to https://lookerstudio.google.com/ and click 'Create' > 'Report'"
                },
                {
                    "step": 2,
                    "title": "Connect to BigQuery data source",
                    "description": f"Select BigQuery as the data source, then navigate to project '{self.project_id}', dataset '{self.dataset_id}', and select the table '{self.tables.get('ANALYTICS', 'weather_analytics')}'"
                },
                {
                    "step": 3,
                    "title": "Create temperature time series chart",
                    "description": "Add a time series chart with Date as dimension, AVG(avg_value) as metric, and filter by metric_type = 'temperature'. Add city_name as breakdown."
                },
                {
                    "step": 4,
                    "title": "Create humidity analysis",
                    "description": "Add a time series chart for humidity trends and a bar chart showing average humidity by city."
                },
                {
                    "step": 5,
                    "title": "Add city comparison table",
                    "description": "Create a table with city_name and metric_type as dimensions, and avg_value, min_value, max_value as metrics."
                },
                {
                    "step": 6,
                    "title": "Add filters and controls",
                    "description": "Add date range control, city selector, and metric type filter to make the dashboard interactive."
                }
            ],
            "data_sources": [
                {
                    "name": "Weather Analytics",
                    "type": "BigQuery",
                    "project": self.project_id,
                    "dataset": self.dataset_id,
                    "table": self.tables.get('ANALYTICS', 'weather_analytics')
                },
                {
                    "name": "Weather Streaming",
                    "type": "BigQuery",
                    "project": self.project_id,
                    "dataset": self.dataset_id,
                    "table": self.tables.get('STREAMING', 'weather_streaming')
                }
            ],
            "recommended_charts": [
                "Time series for temperature trends",
                "Bar chart for city comparison",
                "Heatmap for temperature patterns",
                "Scorecard for current weather metrics",
                "Gauge for humidity levels"
            ]
        }
        
        try:
            with open(output_path, 'w') as f:
                json.dump(instructions, f, indent=2)
            logger.info(f"Dashboard instructions saved to {output_path}")
        except Exception as e:
            logger.error(f"Error saving dashboard instructions: {str(e)}")
            raise

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Create Looker Studio dashboards for weather data')
    parser.add_argument('--credentials', help='Path to service account credentials JSON file')
    parser.add_argument('--output', default='dashboard_config.json', help='Path to save dashboard configuration')
    parser.add_argument('--instructions', default='dashboard_instructions.json', help='Path to save dashboard instructions')
    args = parser.parse_args()
    
    creator = DashboardCreator(args.credentials)
    
    # Create dashboard configuration
    dashboard_config = creator.create_dashboard_config(
        dashboard_title="Weather Data Analytics Dashboard",
        description="Comprehensive dashboard for weather data analysis",
        data_source_id="weather_analytics_source"
    )
    
    # Save dashboard configuration
    creator.save_dashboard_config(dashboard_config, args.output)
    
    # Create dashboard instructions
    creator.create_dashboard_instructions(args.instructions)
    
    logger.info("Dashboard creation completed")

if __name__ == "__main__":
    main()
