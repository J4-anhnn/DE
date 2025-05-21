#!/usr/bin/env python3
"""
Script to check data availability and quality for dashboards
"""
import os
import sys
import argparse
from datetime import datetime, timedelta
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

class DataChecker:
    """Class to check data availability and quality for dashboards"""
    
    def __init__(self):
        """Initialize the data checker"""
        self.client = bigquery.Client()
        self.project_id = GCP_PROJECT_ID
        self.dataset_id = BIGQUERY_DATASET
        self.tables = BIGQUERY_TABLES
        
        logger.info(f"Data checker initialized for project {self.project_id}")
    
    def check_table_exists(self, table_key: str) -> bool:
        """
        Check if a table exists
        
        Args:
            table_key: Key of the table in BIGQUERY_TABLES
            
        Returns:
            True if the table exists, False otherwise
        """
        table_id = self.tables.get(table_key.upper())
        if not table_id:
            logger.warning(f"Unknown table key: {table_key}")
            return False
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} exists")
            return True
        except Exception as e:
            logger.warning(f"Table {table_ref} does not exist: {str(e)}")
            return False
    
    def check_data_availability(
        self, 
        table_key: str, 
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Check data availability for a table
        
        Args:
            table_key: Key of the table in BIGQUERY_TABLES
            days: Number of days to check
            
        Returns:
            Dictionary with availability information
        """
        table_id = self.tables.get(table_key.upper())
        if not table_id:
            raise ValueError(f"Unknown table key: {table_key}")
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        # Determine date field based on table
        date_field = "date" if table_key.upper() == 'ANALYTICS' else (
            "DATE(window_start)" if table_key.upper() == 'STREAMING_HOURLY' else
            "DATE(timestamp)" if table_key.upper() == 'STREAMING' else
            "processing_date"
        )
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Query to check data availability by date and city
        query = f"""
        SELECT
            {date_field} as date,
            city_name,
            COUNT(*) as record_count
        FROM
            `{table_ref}`
        WHERE
            {date_field} BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY
            date, city_name
        ORDER BY
            date DESC, city_name
        """
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Process results
            availability = {
                'table': table_id,
                'period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'days': days
                },
                'data_by_date': {},
                'data_by_city': {},
                'total_records': 0
            }
            
            # Initialize data structures
            for day_offset in range(days):
                date_str = (end_date - timedelta(days=day_offset)).isoformat()
                availability['data_by_date'][date_str] = {
                    'total': 0,
                    'cities': {}
                }
            
            for city in CITIES:
                availability['data_by_city'][city] = {
                    'total': 0,
                    'dates': {}
                }
            
            # Fill in data
            for row in results:
                date_str = row['date'].isoformat()
                city = row['city_name']
                count = row['record_count']
                
                # Update by date
                if date_str in availability['data_by_date']:
                    availability['data_by_date'][date_str]['total'] += count
                    availability['data_by_date'][date_str]['cities'][city] = count
                
                # Update by city
                if city in availability['data_by_city']:
                    availability['data_by_city'][city]['total'] += count
                    availability['data_by_city'][city]['dates'][date_str] = count
                
                # Update total
                availability['total_records'] += count
            
            # Check for missing data
            availability['missing_data'] = []
            
            for date_str, date_data in availability['data_by_date'].items():
                for city in CITIES:
                    if city not in date_data['cities']:
                        availability['missing_data'].append({
                            'date': date_str,
                            'city': city
                        })
            
            return availability
            
        except Exception as e:
            logger.error(f"Error checking data availability: {str(e)}")
            raise
    
    def check_data_quality(self, table_key: str) -> Dict[str, Any]:
        """
        Check data quality for a table
        
        Args:
            table_key: Key of the table in BIGQUERY_TABLES
            
        Returns:
            Dictionary with quality information
        """
        table_id = self.tables.get(table_key.upper())
        if not table_id:
            raise ValueError(f"Unknown table key: {table_key}")
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        # Determine fields to check based on table
        fields_to_check = []
        if table_key.upper() == 'ANALYTICS':
            fields_to_check = ['avg_value', 'min_value', 'max_value']
        elif table_key.upper() == 'STREAMING_HOURLY':
            fields_to_check = ['avg_temperature', 'avg_humidity', 'avg_pressure']
        elif table_key.upper() in ['STREAMING', 'BATCH']:
            fields_to_check = ['temperature', 'humidity', 'pressure']
        
        quality_results = {
            'table': table_id,
            'null_counts': {},
            'out_of_range_counts': {},
            'statistics': {}
        }
        
        # Check for null values
        for field in fields_to_check:
            query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNTIF({field} IS NULL) as null_count,
                COUNTIF({field} IS NOT NULL) as non_null_count
            FROM
                `{table_ref}`
            """
            
            try:
                query_job = self.client.query(query)
                results = query_job.result()
                
                for row in results:
                    quality_results['null_counts'][field] = {
                        'total': row['total_count'],
                        'null': row['null_count'],
                        'non_null': row['non_null_count'],
                        'null_percentage': (row['null_count'] / row['total_count'] * 100) if row['total_count'] > 0 else 0
                    }
            except Exception as e:
                logger.error(f"Error checking null values for {field}: {str(e)}")
        
        # Check for out-of-range values
        range_checks = {
            'temperature': {'min': -50, 'max': 60},
            'avg_temperature': {'min': -50, 'max': 60},
            'humidity': {'min': 0, 'max': 100},
            'avg_humidity': {'min': 0, 'max': 100},
            'pressure': {'min': 800, 'max': 1200},
            'avg_pressure': {'min': 800, 'max': 1200}
        }
        
        for field in fields_to_check:
            if field in range_checks:
                min_val = range_checks[field]['min']
                max_val = range_checks[field]['max']
                
                query = f"""
                SELECT
                    COUNT(*) as total_count,
                    COUNTIF({field} < {min_val} OR {field} > {max_val}) as out_of_range_count,
                    MIN({field}) as min_value,
                    MAX({field}) as max_value,
                    AVG({field}) as avg_value,
                    STDDEV({field}) as std_dev
                FROM
                    `{table_ref}`
                WHERE
                    {field} IS NOT NULL
                """
                
                try:
                    query_job = self.client.query(query)
                    results = query_job.result()
                    
                    for row in results:
                        quality_results['out_of_range_counts'][field] = {
                            'total': row['total_count'],
                            'out_of_range': row['out_of_range_count'],
                            'out_of_range_percentage': (row['out_of_range_count'] / row['total_count'] * 100) if row['total_count'] > 0 else 0,
                            'expected_range': {'min': min_val, 'max': max_val}
                        }
                        
                        quality_results['statistics'][field] = {
                            'min': row['min_value'],
                            'max': row['max_value'],
                            'avg': row['avg_value'],
                            'std_dev': row['std_dev']
                        }
                except Exception as e:
                    logger.error(f"Error checking range for {field}: {str(e)}")
        
        return quality_results
    
    def generate_report(self, days: int = 7) -> Dict[str, Any]:
        """
        Generate a comprehensive data quality report
        
        Args:
            days: Number of days to check
            
        Returns:
            Dictionary with report information
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'project_id': self.project_id,
            'dataset_id': self.dataset_id,
            'tables': {},
            'overall_status': 'PASS'
        }
        
        # Check each table
        for table_key in self.tables:
            if self.check_table_exists(table_key):
                try:
                    availability = self.check_data_availability(table_key, days)
                    quality = self.check_data_quality(table_key)
                    
                    # Determine table status
                    table_status = 'PASS'
                    
                    # Check for missing data
                    if availability['missing_data']:
                        if len(availability['missing_data']) > (days * len(CITIES) * 0.1):  # More than 10% missing
                            table_status = 'FAIL'
                        else:
                            table_status = 'WARNING'
                    
                    # Check for data quality issues
                    for field, stats in quality['null_counts'].items():
                        if stats['null_percentage'] > 10:  # More than 10% nulls
                            table_status = 'FAIL'
                        elif stats['null_percentage'] > 5:  # More than 5% nulls
                            table_status = 'WARNING' if table_status != 'FAIL' else table_status
                    
                    for field, stats in quality['out_of_range_counts'].items():
                        if stats['out_of_range_percentage'] > 5:  # More than 5% out of range
                            table_status = 'FAIL'
                        elif stats['out_of_range_percentage'] > 1:  # More than 1% out of range
                            table_status = 'WARNING' if table_status != 'FAIL' else table_status
                    
                    # Add to report
                    report['tables'][self.tables[table_key]] = {
                        'status': table_status,
                        'availability': availability,
                        'quality': quality
                    }
                    
                    # Update overall status
                    if table_status == 'FAIL' and report['overall_status'] != 'FAIL':
                        report['overall_status'] = 'FAIL'
                    elif table_status == 'WARNING' and report['overall_status'] == 'PASS':
                        report['overall_status'] = 'WARNING'
                    
                except Exception as e:
                    logger.error(f"Error checking table {table_key}: {str(e)}")
                    report['tables'][self.tables[table_key]] = {
                        'status': 'ERROR',
                        'error': str(e)
                    }
                    report['overall_status'] = 'FAIL'
            else:
                report['tables'][self.tables[table_key]] = {
                    'status': 'NOT_FOUND'
                }
        
        return report

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Check data availability and quality for dashboards')
    parser.add_argument('--days', type=int, default=7, help='Number of days to check')
    parser.add_argument('--output', help='Path to save report (JSON format)')
    args = parser.parse_args()
    
    checker = DataChecker()
    report = checker.generate_report(args.days)
    
    # Print summary
    print(f"Data Quality Report: {report['overall_status']}")
    print(f"Project: {report['project_id']}")
    print(f"Dataset: {report['dataset_id']}")
    print("\nTable Status:")
    
    for table, data in report['tables'].items():
        status = data.get('status', 'UNKNOWN')
        status_display = {
            'PASS': '✅ PASS',
            'WARNING': '⚠️ WARNING',
            'FAIL': '❌ FAIL',
            'ERROR': '🔥 ERROR',
            'NOT_FOUND': '❓ NOT FOUND'
        }.get(status, status)
        
        print(f"  {table}: {status_display}")
        
        if status == 'PASS' or status == 'WARNING':
            if 'availability' in data:
                print(f"    - Records: {data['availability']['total_records']}")
                missing = len(data['availability'].get('missing_data', []))
                if missing > 0:
                    print(f"    - Missing data points: {missing}")
    
    # Save report if output path is provided
    if args.output:
        import json
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\nDetailed report saved to {args.output}")

if __name__ == "__main__":
    main()
