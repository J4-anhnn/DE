#!/usr/bin/env python3
"""
Script to set up BigQuery views for dashboards
"""
import os
import sys
import argparse
from typing import Dict, Any, List, Optional
from google.cloud import bigquery

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

class ViewCreator:
    """Class to create and manage BigQuery views for dashboards"""
    
    def __init__(self):
        """Initialize the view creator"""
        self.client = bigquery.Client()
        self.project_id = GCP_PROJECT_ID
        self.dataset_id = BIGQUERY_DATASET
        self.tables = BIGQUERY_TABLES
        
        logger.info(f"View creator initialized for project {self.project_id}")
    
    def create_view(self, view_id: str, query: str, description: str = None) -> bigquery.Table:
        """
        Create or update a BigQuery view
        
        Args:
            view_id: ID of the view to create
            query: SQL query for the view
            description: Optional description for the view
            
        Returns:
            The created or updated view
        """
        # Create view reference
        dataset_ref = self.client.dataset(self.dataset_id)
        view_ref = dataset_ref.table(view_id)
        
        # Create view
        view = bigquery.Table(view_ref)
        view.view_query = query
        
        if description:
            view.description = description
        
        # Check if view exists
        try:
            existing_view = self.client.get_table(view_ref)
            logger.info(f"Updating existing view {view_id}")
            view = self.client.update_table(view, ["view_query", "description"])
        except Exception:
            logger.info(f"Creating new view {view_id}")
            view = self.client.create_table(view)
        
        logger.info(f"View {view_id} created/updated successfully")
        return view
    
    def create_latest_weather_view(self) -> bigquery.Table:
        """
        Create a view for the latest weather data
        
        Returns:
            The created view
        """
        view_id = "latest_weather"
        description = "Latest weather data for each city"
        
        # Determine which table to use
        source_table = None
        date_field = None
        
        if self.tables.get('STREAMING') in self._get_existing_tables():
            source_table = self.tables.get('STREAMING')
            date_field = "timestamp"
        elif self.tables.get('BATCH') in self._get_existing_tables():
            source_table = self.tables.get('BATCH')
            date_field = "measurement_time"
        else:
            raise ValueError("No suitable source table found for latest weather view")
        
        # Create query
        query = f"""
        WITH ranked_data AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY {date_field} DESC) as row_num
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
        )
        SELECT
            city_name,
            {date_field} as measurement_time,
            temperature,
            humidity,
            pressure,
            wind_speed,
            weather_condition
        FROM
            ranked_data
        WHERE
            row_num = 1
        """
        
        return self.create_view(view_id, query, description)
    
    def create_daily_summary_view(self) -> bigquery.Table:
        """
        Create a view for daily weather summaries
        
        Returns:
            The created view
        """
        view_id = "daily_weather_summary"
        description = "Daily summary of weather data by city"
        
        # Determine which table to use
        source_table = None
        date_field = None
        
        if self.tables.get('ANALYTICS') in self._get_existing_tables():
            source_table = self.tables.get('ANALYTICS')
            query = f"""
            SELECT
                city_name,
                date,
                metric_type,
                avg_value,
                min_value,
                max_value,
                std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            """
        elif self.tables.get('STREAMING_HOURLY') in self._get_existing_tables():
            source_table = self.tables.get('STREAMING_HOURLY')
            query = f"""
            SELECT
                city_name,
                DATE(window_start) as date,
                'temperature' as metric_type,
                AVG(avg_temperature) as avg_value,
                MIN(min_temperature) as min_value,
                MAX(max_temperature) as max_value,
                STDDEV(avg_temperature) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(window_start) as date,
                'humidity' as metric_type,
                AVG(avg_humidity) as avg_value,
                MIN(avg_humidity) as min_value,
                MAX(avg_humidity) as max_value,
                STDDEV(avg_humidity) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(window_start) as date,
                'pressure' as metric_type,
                AVG(avg_pressure) as avg_value,
                MIN(avg_pressure) as min_value,
                MAX(avg_pressure) as max_value,
                STDDEV(avg_pressure) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            """
        elif self.tables.get('STREAMING') in self._get_existing_tables():
            source_table = self.tables.get('STREAMING')
            query = f"""
            SELECT
                city_name,
                DATE(timestamp) as date,
                'temperature' as metric_type,
                AVG(temperature) as avg_value,
                MIN(temperature) as min_value,
                MAX(temperature) as max_value,
                STDDEV(temperature) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(timestamp) as date,
                'humidity' as metric_type,
                AVG(humidity) as avg_value,
                MIN(humidity) as min_value,
                MAX(humidity) as max_value,
                STDDEV(humidity) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(timestamp) as date,
                'pressure' as metric_type,
                AVG(pressure) as avg_value,
                MIN(pressure) as min_value,
                MAX(pressure) as max_value,
                STDDEV(pressure) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            """
        elif self.tables.get('BATCH') in self._get_existing_tables():
            source_table = self.tables.get('BATCH')
            query = f"""
            SELECT
                city_name,
                DATE(measurement_time) as date,
                'temperature' as metric_type,
                AVG(temperature) as avg_value,
                MIN(temperature) as min_value,
                MAX(temperature) as max_value,
                STDDEV(temperature) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(measurement_time) as date,
                'humidity' as metric_type,
                AVG(humidity) as avg_value,
                MIN(humidity) as min_value,
                MAX(humidity) as max_value,
                STDDEV(humidity) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(measurement_time) as date,
                'pressure' as metric_type,
                AVG(pressure) as avg_value,
                MIN(pressure) as min_value,
                MAX(pressure) as max_value,
                STDDEV(pressure) as std_dev
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            """
        else:
            raise ValueError("No suitable source table found for daily summary view")
        
        return self.create_view(view_id, query, description)
    
    def create_city_comparison_view(self) -> bigquery.Table:
        """
        Create a view for city comparison
        
        Returns:
            The created view
        """
        view_id = "city_comparison"
        description = "Comparison of weather metrics across cities"
        
        # Determine which table to use
        source_table = None
        
        if self.tables.get('ANALYTICS') in self._get_existing_tables():
            source_table = self.tables.get('ANALYTICS')
            query = f"""
            WITH recent_data AS (
                SELECT
                    city_name,
                    metric_type,
                    AVG(avg_value) as avg_value
                FROM
                    `{self.project_id}.{self.dataset_id}.{source_table}`
                WHERE
                    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                GROUP BY
                    city_name, metric_type
            )
            
            SELECT
                city_name,
                MAX(CASE WHEN metric_type = 'temperature' THEN avg_value END) as avg_temperature,
                MAX(CASE WHEN metric_type = 'humidity' THEN avg_value END) as avg_humidity,
                MAX(CASE WHEN metric_type = 'pressure' THEN avg_value END) as avg_pressure,
                MAX(CASE WHEN metric_type = 'wind_speed' THEN avg_value END) as avg_wind_speed
            FROM
                recent_data
            GROUP BY
                city_name
            """
        elif self.tables.get('STREAMING_HOURLY') in self._get_existing_tables():
            source_table = self.tables.get('STREAMING_HOURLY')
            query = f"""
            SELECT
                city_name,
                AVG(avg_temperature) as avg_temperature,
                AVG(avg_humidity) as avg_humidity,
                AVG(avg_pressure) as avg_pressure,
                NULL as avg_wind_speed
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            WHERE
                window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY
                city_name
            """
        elif self.tables.get('STREAMING') in self._get_existing_tables():
            source_table = self.tables.get('STREAMING')
            query = f"""
            SELECT
                city_name,
                AVG(temperature) as avg_temperature,
                AVG(humidity) as avg_humidity,
                AVG(pressure) as avg_pressure,
                AVG(wind_speed) as avg_wind_speed
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            WHERE
                timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY
                city_name
            """
        elif self.tables.get('BATCH') in self._get_existing_tables():
            source_table = self.tables.get('BATCH')
            query = f"""
            SELECT
                city_name,
                AVG(temperature) as avg_temperature,
                AVG(humidity) as avg_humidity,
                AVG(pressure) as avg_pressure,
                AVG(wind_speed) as avg_wind_speed
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            WHERE
                measurement_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY
                city_name
            """
        else:
            raise ValueError("No suitable source table found for city comparison view")
        
        return self.create_view(view_id, query, description)
    
    def create_weather_trends_view(self) -> bigquery.Table:
        """
        Create a view for weather trends
        
        Returns:
            The created view
        """
        view_id = "weather_trends"
        description = "Weather trends over time"
        
        # Determine which table to use
        source_table = None
        
        if self.tables.get('ANALYTICS') in self._get_existing_tables():
            source_table = self.tables.get('ANALYTICS')
            query = f"""
            SELECT
                city_name,
                date,
                metric_type,
                avg_value,
                CASE
                    WHEN metric_type = 'temperature' THEN '°C'
                    WHEN metric_type = 'humidity' THEN '%'
                    WHEN metric_type = 'pressure' THEN 'hPa'
                    WHEN metric_type = 'wind_speed' THEN 'm/s'
                    ELSE ''
                END as unit,
                AVG(avg_value) OVER (
                    PARTITION BY city_name, metric_type
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            ORDER BY
                city_name, metric_type, date
            """
        elif self.tables.get('STREAMING_HOURLY') in self._get_existing_tables():
            source_table = self.tables.get('STREAMING_HOURLY')
            query = f"""
            SELECT
                city_name,
                DATE(window_start) as date,
                'temperature' as metric_type,
                AVG(avg_temperature) as avg_value,
                '°C' as unit,
                AVG(AVG(avg_temperature)) OVER (
                    PARTITION BY city_name
                    ORDER BY DATE(window_start)
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(window_start) as date,
                'humidity' as metric_type,
                AVG(avg_humidity) as avg_value,
                '%' as unit,
                AVG(AVG(avg_humidity)) OVER (
                    PARTITION BY city_name
                    ORDER BY DATE(window_start)
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE(window_start) as date,
                'pressure' as metric_type,
                AVG(avg_pressure) as avg_value,
                'hPa' as unit,
                AVG(AVG(avg_pressure)) OVER (
                    PARTITION BY city_name
                    ORDER BY DATE(window_start)
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            ORDER BY
                city_name, metric_type, date
            """
        elif self.tables.get('STREAMING') in self._get_existing_tables() or self.tables.get('BATCH') in self._get_existing_tables():
            # Choose the table
            if self.tables.get('STREAMING') in self._get_existing_tables():
                source_table = self.tables.get('STREAMING')
                time_field = "timestamp"
            else:
                source_table = self.tables.get('BATCH')
                time_field = "measurement_time"
            
            query = f"""
            SELECT
                city_name,
                DATE({time_field}) as date,
                'temperature' as metric_type,
                AVG(temperature) as avg_value,
                '°C' as unit,
                AVG(AVG(temperature)) OVER (
                    PARTITION BY city_name
                    ORDER BY DATE({time_field})
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE({time_field}) as date,
                'humidity' as metric_type,
                AVG(humidity) as avg_value,
                '%' as unit,
                AVG(AVG(humidity)) OVER (
                    PARTITION BY city_name
                    ORDER BY DATE({time_field})
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            UNION ALL
            
            SELECT
                city_name,
                DATE({time_field}) as date,
                'pressure' as metric_type,
                AVG(pressure) as avg_value,
                'hPa' as unit,
                AVG(AVG(pressure)) OVER (
                    PARTITION BY city_name
                    ORDER BY DATE({time_field})
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM
                `{self.project_id}.{self.dataset_id}.{source_table}`
            GROUP BY
                city_name, date
            
            ORDER BY
                city_name, metric_type, date
            """
        else:
            raise ValueError("No suitable source table found for weather trends view")
        
        return self.create_view(view_id, query, description)
    
    def _get_existing_tables(self) -> List[str]:
        """
        Get list of existing tables in the dataset
        
        Returns:
            List of table IDs
        """
        tables = []
        dataset_ref = self.client.dataset(self.dataset_id)
        
        try:
            # List all tables in the dataset
            tables_iter = self.client.list_tables(dataset_ref)
            tables = [table.table_id for table in tables_iter]
            logger.info(f"Found {len(tables)} tables in dataset {self.dataset_id}")
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
        
        return tables
    
    def create_all_views(self) -> Dict[str, bigquery.Table]:
        """
        Create all dashboard views
        
        Returns:
            Dictionary of created views
        """
        views = {}
        
        try:
            views['latest_weather'] = self.create_latest_weather_view()
        except Exception as e:
            logger.error(f"Error creating latest_weather view: {str(e)}")
        
        try:
            views['daily_summary'] = self.create_daily_summary_view()
        except Exception as e:
            logger.error(f"Error creating daily_summary view: {str(e)}")
        
        try:
            views['city_comparison'] = self.create_city_comparison_view()
        except Exception as e:
            logger.error(f"Error creating city_comparison view: {str(e)}")
        
        try:
            views['weather_trends'] = self.create_weather_trends_view()
        except Exception as e:
            logger.error(f"Error creating weather_trends view: {str(e)}")
        
        return views

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Set up BigQuery views for dashboards')
    parser.add_argument('--view', choices=['latest', 'daily', 'comparison', 'trends', 'all'], 
                      default='all', help='Which view to create')
    args = parser.parse_args()
    
    creator = ViewCreator()
    
    if args.view == 'latest' or args.view == 'all':
        try:
            creator.create_latest_weather_view()
            print("✅ Created latest_weather view")
        except Exception as e:
            print(f"❌ Error creating latest_weather view: {str(e)}")
    
    if args.view == 'daily' or args.view == 'all':
        try:
            creator.create_daily_summary_view()
            print("✅ Created daily_weather_summary view")
        except Exception as e:
            print(f"❌ Error creating daily_weather_summary view: {str(e)}")
    
    if args.view == 'comparison' or args.view == 'all':
        try:
            creator.create_city_comparison_view()
            print("✅ Created city_comparison view")
        except Exception as e:
            print(f"❌ Error creating city_comparison view: {str(e)}")
    
    if args.view == 'trends' or args.view == 'all':
        try:
            creator.create_weather_trends_view()
            print("✅ Created weather_trends view")
        except Exception as e:
            print(f"❌ Error creating weather_trends view: {str(e)}")
    
    print("\nView creation completed")

if __name__ == "__main__":
    main()
