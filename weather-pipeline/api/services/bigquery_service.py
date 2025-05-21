"""
BigQuery service for weather data API
"""
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Union, Tuple
import os
import logging
import sys
import json
from functools import lru_cache

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.settings import (
    GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLES, 
    WEATHER_ALERT_THRESHOLDS, setup_logging
)

logger = setup_logging(__name__)

class BigQueryService:
    """Service for querying weather data from BigQuery"""
    
    def __init__(self):
        """Initialize BigQuery client"""
        self.client = bigquery.Client()
        self.project_id = GCP_PROJECT_ID
        self.dataset_id = BIGQUERY_DATASET
        self.batch_table = BIGQUERY_TABLES.get('BATCH', 'weather_batch')
        self.streaming_table = BIGQUERY_TABLES.get('STREAMING', 'weather_streaming')
        self.streaming_hourly_table = BIGQUERY_TABLES.get('STREAMING_HOURLY', 'weather_streaming_hourly')
        self.analytics_table = BIGQUERY_TABLES.get('ANALYTICS', 'weather_analytics')
        
        # Kiểm tra các bảng có tồn tại không
        self.available_tables = self._check_available_tables()
        
        logger.info(f"Initialized BigQuery service for project {self.project_id}")
        logger.info(f"Available tables: {', '.join(self.available_tables)}")
    
    def _check_available_tables(self) -> List[str]:
        """Kiểm tra các bảng có sẵn trong dataset"""
        available = []
        tables_to_check = [
            self.batch_table,
            self.streaming_table,
            self.streaming_hourly_table,
            self.analytics_table
        ]
        
        for table in tables_to_check:
            try:
                self.client.get_table(f"{self.project_id}.{self.dataset_id}.{table}")
                available.append(table)
            except Exception:
                logger.warning(f"Table {table} not found in dataset {self.dataset_id}")
        
        return available
    
    def _get_best_table_for_data(self) -> Tuple[str, str]:
        """Chọn bảng tốt nhất để truy vấn dữ liệu thời tiết"""
        if self.batch_table in self.available_tables:
            return self.batch_table, "processing_date"
        elif self.streaming_table in self.available_tables:
            return self.streaming_table, "DATE(timestamp)"
        elif self.streaming_hourly_table in self.available_tables:
            return self.streaming_hourly_table, "DATE(window_start)"
        else:
            raise ValueError("No suitable table available for weather data")
    
    def _get_best_table_for_analytics(self) -> str:
        """Chọn bảng tốt nhất để truy vấn phân tích"""
        if self.analytics_table in self.available_tables:
            return self.analytics_table
        elif self.streaming_hourly_table in self.available_tables:
            return self.streaming_hourly_table
        elif self.streaming_table in self.available_tables:
            return self.streaming_table
        else:
            raise ValueError("No suitable table available for analytics")
    
    @lru_cache(maxsize=32)
    def get_weather_data(
        self, 
        city: Optional[str], 
        date_from: date, 
        date_to: date, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query weather data with optional filtering
        
        Args:
            city: Optional city name filter
            date_from: Start date for data
            date_to: End date for data
            limit: Maximum number of records to return
            
        Returns:
            List of weather data records
        """
        table_name, date_field = self._get_best_table_for_data()
        
        query = f"""
        SELECT
            city_name,
            {date_field} as measurement_date,
            TIMESTAMP_TRUNC(
                CASE 
                    WHEN '{table_name}' = '{self.batch_table}' THEN measurement_time
                    WHEN '{table_name}' = '{self.streaming_hourly_table}' THEN window_start
                    ELSE timestamp
                END, 
                SECOND
            ) as measurement_time,
            CASE
                WHEN '{table_name}' = '{self.streaming_hourly_table}' THEN avg_temperature
                ELSE temperature
            END as temperature,
            CASE
                WHEN '{table_name}' = '{self.streaming_hourly_table}' THEN avg_humidity
                ELSE humidity
            END as humidity,
            CASE
                WHEN '{table_name}' = '{self.streaming_hourly_table}' THEN avg_pressure
                ELSE pressure
            END as pressure,
            CASE
                WHEN '{table_name}' = '{self.batch_table}' THEN wind_speed
                WHEN '{table_name}' = '{self.streaming_table}' THEN wind_speed
                ELSE NULL
            END as wind_speed,
            CASE
                WHEN '{table_name}' = '{self.batch_table}' THEN weather_condition
                WHEN '{table_name}' = '{self.streaming_table}' THEN weather_condition
                ELSE NULL
            END as weather_condition
        FROM
            `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE
            {date_field} BETWEEN DATE('{date_from}') AND DATE('{date_to}')
        """
        
        if city:
            query += f" AND city_name = '{city}'"
        
        query += f"""
        ORDER BY measurement_time DESC
        LIMIT {limit}
        """
        
        logger.info(f"Executing query: {query}")
        query_job = self.client.query(query)
        results = query_job.result()
        
        return [dict(row) for row in results]
    
    @lru_cache(maxsize=32)
    def get_weather_stats(
        self, 
        city: Optional[str], 
        date_from: date, 
        date_to: date, 
        metric: str
    ) -> List[Dict[str, Any]]:
        """
        Query weather statistics
        
        Args:
            city: Optional city name filter
            date_from: Start date for data
            date_to: End date for data
            metric: Weather metric to analyze (temperature, humidity, pressure, wind_speed)
            
        Returns:
            List of weather statistics records
        """
        # Validate metric
        valid_metrics = ["temperature", "humidity", "pressure", "wind_speed"]
        if metric not in valid_metrics:
            raise ValueError(f"Invalid metric: {metric}. Must be one of {valid_metrics}")
        
        # Try to use analytics table first
        try:
            table_name = self._get_best_table_for_analytics()
            
            if table_name == self.analytics_table:
                # Use pre-computed analytics
                query = f"""
                SELECT
                    city_name,
                    date,
                    metric_type as metric,
                    avg_value,
                    min_value,
                    max_value,
                    std_dev
                FROM
                    `{self.project_id}.{self.dataset_id}.{self.analytics_table}`
                WHERE
                    date BETWEEN DATE('{date_from}') AND DATE('{date_to}')
                    AND metric_type = '{metric}'
                """
                
                if city:
                    query += f" AND city_name = '{city}'"
                
                query += """
                ORDER BY city_name, date
                """
            elif table_name == self.streaming_hourly_table:
                # Use hourly aggregated data
                value_field = f"avg_{metric}"
                date_field = "DATE(window_start)"
                
                query = f"""
                SELECT
                    city_name,
                    {date_field} as date,
                    '{metric}' as metric,
                    AVG({value_field}) as avg_value,
                    MIN({value_field}) as min_value,
                    MAX({value_field}) as max_value,
                    STDDEV({value_field}) as std_dev
                FROM
                    `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE
                    {date_field} BETWEEN DATE('{date_from}') AND DATE('{date_to}')
                """
                
                if city:
                    query += f" AND city_name = '{city}'"
                
                query += """
                GROUP BY city_name, date
                ORDER BY city_name, date
                """
            else:
                # Use raw data
                date_field = "DATE(timestamp)" if table_name == self.streaming_table else "processing_date"
                
                query = f"""
                SELECT
                    city_name,
                    {date_field} as date,
                    '{metric}' as metric,
                    AVG({metric}) as avg_value,
                    MIN({metric}) as min_value,
                    MAX({metric}) as max_value,
                    STDDEV({metric}) as std_dev
                FROM
                    `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE
                    {date_field} BETWEEN DATE('{date_from}') AND DATE('{date_to}')
                """
                
                if city:
                    query += f" AND city_name = '{city}'"
                
                query += """
                GROUP BY city_name, date
                ORDER BY city_name, date
                """
            
            logger.info(f"Executing query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error querying weather stats: {str(e)}")
            raise
    
    @lru_cache(maxsize=16)
    def get_weather_trend(
        self, 
        city: str, 
        days: int, 
        metric: str
    ) -> List[Dict[str, Any]]:
        """
        Query weather trend for a specific city
        
        Args:
            city: City name
            days: Number of days to analyze
            metric: Weather metric to analyze (temperature, humidity, pressure, wind_speed)
            
        Returns:
            List of trend data with data points
        """
        # Validate metric
        valid_metrics = ["temperature", "humidity", "pressure", "wind_speed"]
        if metric not in valid_metrics:
            raise ValueError(f"Invalid metric: {metric}. Must be one of {valid_metrics}")
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        try:
            # Choose best table for trend analysis
            table_name = self._get_best_table_for_analytics()
            
            if table_name == self.analytics_table:
                # Use pre-computed analytics
                query = f"""
                WITH daily_data AS (
                    SELECT
                        city_name,
                        date,
                        avg_value as value
                    FROM
                        `{self.project_id}.{self.dataset_id}.{self.analytics_table}`
                    WHERE
                        city_name = '{city}'
                        AND date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                        AND metric_type = '{metric}'
                    ORDER BY date
                )
                """
            elif table_name == self.streaming_hourly_table:
                # Use hourly aggregated data
                value_field = f"avg_{metric}"
                date_field = "DATE(window_start)"
                
                query = f"""
                WITH daily_data AS (
                    SELECT
                        city_name,
                        {date_field} as date,
                        AVG({value_field}) as value
                    FROM
                        `{self.project_id}.{self.dataset_id}.{table_name}`
                    WHERE
                        city_name = '{city}'
                        AND {date_field} BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                    GROUP BY city_name, {date_field}
                    ORDER BY date
                )
                """
            else:
                # Use raw data
                date_field = "DATE(timestamp)" if table_name == self.streaming_table else "processing_date"
                
                query = f"""
                WITH daily_data AS (
                    SELECT
                        city_name,
                        {date_field} as date,
                        AVG({metric}) as value
                    FROM
                        `{self.project_id}.{self.dataset_id}.{table_name}`
                    WHERE
                        city_name = '{city}'
                        AND {date_field} BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                    GROUP BY city_name, {date_field}
                    ORDER BY date
                )
                """
            
            # Add trend calculation
            query += """
            , trend_calc AS (
                SELECT
                    city_name,
                    CASE
                        WHEN REGR_SLOPE(value, UNIX_DATE(date)) > 0.1 THEN 'increasing'
                        WHEN REGR_SLOPE(value, UNIX_DATE(date)) < -0.1 THEN 'decreasing'
                        ELSE 'stable'
                    END as trend_direction,
                    REGR_SLOPE(value, UNIX_DATE(date)) as change_rate
                FROM daily_data
            )
            
            SELECT
                d.city_name,
                t.trend_direction,
                t.change_rate,
                STRUCT(d.date, d.value) as data_point
            FROM daily_data d
            CROSS JOIN trend_calc t
            ORDER BY d.date
            """
            
            logger.info(f"Executing query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Process results to group data points
            trends = {}
            for row in results:
                row_dict = dict(row)
                city_name = row_dict['city_name']
                
                if city_name not in trends:
                    trends[city_name] = {
                        'city_name': city_name,
                        'metric': metric,
                        'trend_direction': row_dict['trend_direction'],
                        'change_rate': row_dict['change_rate'],
                        'data_points': []
                    }
                
                trends[city_name]['data_points'].append({
                    'date': row_dict['data_point']['date'],
                    'value': row_dict['data_point']['value']
                })
            
            return list(trends.values())
            
        except Exception as e:
            logger.error(f"Error querying weather trend: {str(e)}")
            raise
    
    def get_weather_alerts(
        self, 
        city: Optional[str], 
        hours: int
    ) -> List[Dict[str, Any]]:
        """
        Query recent weather alerts
        
        Args:
            city: Optional city name filter
            hours: Number of hours to look back
            
        Returns:
            List of weather alerts
        """
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        try:
            # Choose best table for alerts
            table_name, date_field = self._get_best_table_for_data()
            
            # Define thresholds
            temp_high = WEATHER_ALERT_THRESHOLDS['temperature_high']
            temp_low = WEATHER_ALERT_THRESHOLDS['temperature_low']
            wind_high = WEATHER_ALERT_THRESHOLDS['wind_speed_high']
            humidity_high = WEATHER_ALERT_THRESHOLDS['humidity_high']
            
            # Define time field based on table
            if table_name == self.batch_table:
                time_field = "measurement_time"
                temp_field = "temperature"
                humidity_field = "humidity"
                wind_field = "wind_speed"
            elif table_name == self.streaming_hourly_table:
                time_field = "window_start"
                temp_field = "avg_temperature"
                humidity_field = "avg_humidity"
                wind_field = "NULL"  # Hourly data might not have wind speed
            else:
                time_field = "timestamp"
                temp_field = "temperature"
                humidity_field = "humidity"
                wind_field = "wind_speed"
            
            query = f"""
            SELECT
                city_name,
                CASE
                    WHEN {temp_field} > {temp_high} THEN 'HIGH_TEMPERATURE'
                    WHEN {temp_field} < {temp_low} THEN 'LOW_TEMPERATURE'
                    WHEN {wind_field} > {wind_high} THEN 'HIGH_WIND'
                    WHEN {humidity_field} > {humidity_high} THEN 'HIGH_HUMIDITY'
                    ELSE 'OTHER'
                END as alert_type,
                CASE
                    WHEN {temp_field} > {temp_high + 3} OR {temp_field} < {temp_low - 3} OR {wind_field} > {wind_high + 5} THEN 'critical'
                    WHEN {temp_field} > {temp_high} OR {temp_field} < {temp_low} OR {wind_field} > {wind_high} OR {humidity_field} > {humidity_high} THEN 'warning'
                    ELSE 'info'
                END as alert_level,
                CASE
                    WHEN {temp_field} > {temp_high} THEN CONCAT('High temperature: ', CAST({temp_field} AS STRING), '°C')
                    WHEN {temp_field} < {temp_low} THEN CONCAT('Low temperature: ', CAST({temp_field} AS STRING), '°C')
                    WHEN {wind_field} > {wind_high} THEN CONCAT('High wind speed: ', CAST({wind_field} AS STRING), ' m/s')
                    WHEN {humidity_field} > {humidity_high} THEN CONCAT('High humidity: ', CAST({humidity_field} AS STRING), '%')
                    ELSE 'Weather condition alert'
                END as message,
                CASE
                    WHEN {temp_field} > {temp_high} OR {temp_field} < {temp_low} THEN {temp_field}
                    WHEN {wind_field} > {wind_high} THEN {wind_field}
                    WHEN {humidity_field} > {humidity_high} THEN {humidity_field}
                    ELSE NULL
                END as value,
                CASE
                    WHEN {temp_field} > {temp_high} THEN {temp_high}
                    WHEN {temp_field} < {temp_low} THEN {temp_low}
                    WHEN {wind_field} > {wind_high} THEN {wind_high}
                    WHEN {humidity_field} > {humidity_high} THEN {humidity_high}
                    ELSE NULL
                END as threshold,
                {time_field} as timestamp
            FROM
                `{self.project_id}.{self.dataset_id}.{table_name}`
            WHERE
                {time_field} BETWEEN TIMESTAMP('{start_time.isoformat()}') AND TIMESTAMP('{end_time.isoformat()}')
                AND ({temp_field} > {temp_high} OR {temp_field} < {temp_low} OR {wind_field} > {wind_high} OR {humidity_field} > {humidity_high})
            """
            
            if city:
                query += f" AND city_name = '{city}'"
            
            query += """
            ORDER BY timestamp DESC
            """
            
            logger.info(f"Executing query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error querying weather alerts: {str(e)}")
            raise
    
    def get_weather_forecast(
        self, 
        city: str, 
        days: int
    ) -> List[Dict[str, Any]]:
        """
        Get weather forecast for a specific city
        
        Note: This is a simplified implementation that uses historical data
        to generate a "forecast". In a real system, you would integrate with
        a forecasting service or implement a proper forecasting model.
        
        Args:
            city: City name
            days: Number of days to forecast
            
        Returns:
            List of forecast data for each day
        """
        # Calculate date range for last year
        end_date = datetime.now().date()
        forecast_dates = [end_date + timedelta(days=i+1) for i in range(days)]
        last_year_dates = [(date - timedelta(days=365)).strftime("%Y-%m-%d") for date in forecast_dates]
        
        try:
            # Choose best table for forecast
            table_name, date_field = self._get_best_table_for_data()
            
            # Define fields based on table
            if table_name == self.batch_table:
                temp_field = "temperature"
                humidity_field = "humidity"
                condition_field = "weather_condition"
                date_field = "DATE(measurement_time)"
            elif table_name == self.streaming_hourly_table:
                temp_field = "avg_temperature"
                humidity_field = "avg_humidity"
                condition_field = "NULL"  # Hourly data might not have weather condition
                date_field = "DATE(window_start)"
            else:
                temp_field = "temperature"
                humidity_field = "humidity"
                condition_field = "weather_condition"
                date_field = "DATE(timestamp)"
            
            date_conditions = " OR ".join([f"{date_field} = '{date}'" for date in last_year_dates])
            
            query = f"""
            WITH historical_data AS (
                SELECT
                    city_name,
                    {date_field} as historical_date,
                    DATE_ADD({date_field}, INTERVAL 365 DAY) as forecast_date,
                    AVG({temp_field}) as avg_temp,
                    MIN({temp_field}) as min_temp,
                    MAX({temp_field}) as max_temp,
                    AVG({humidity_field}) as avg_humidity,
                    MODE({condition_field}) as common_condition,
                    COUNT(CASE WHEN {condition_field} LIKE '%rain%' OR {condition_field} LIKE '%shower%' THEN 1 END) / COUNT(*) * 100 as rain_probability
                FROM
                    `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE
                    city_name = '{city}'
                    AND ({date_conditions})
                GROUP BY city_name, {date_field}
            )
            
            SELECT
                city_name,
                forecast_date,
                min_temp as temperature_min,
                max_temp as temperature_max,
                avg_humidity as humidity,
                common_condition as weather_condition,
                rain_probability as precipitation_probability
            FROM historical_data
            ORDER BY forecast_date
            """
            
            logger.info(f"Executing query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            forecasts = [dict(row) for row in results]
            
            # If no historical data is available, generate some placeholder forecasts
            if not forecasts:
                logger.warning(f"No historical data available for {city}, generating placeholder forecasts")
                forecasts = []
                base_temp = 25  # Base temperature
                for i, forecast_date in enumerate(forecast_dates):
                    # Add some variation to the forecast
                    temp_variation = (i % 3) - 1  # -1, 0, or 1
                    forecasts.append({
                        "city_name": city,
                        "forecast_date": forecast_date,
                        "temperature_min": base_temp - 5 + temp_variation,
                        "temperature_max": base_temp + 5 + temp_variation,
                        "humidity": 70 + (i * 2) % 20,  # 70-90%
                        "weather_condition": ["Clear", "Clouds", "Rain"][i % 3],
                        "precipitation_probability": [10, 30, 60][i % 3]
                    })
            
            return forecasts
            
        except Exception as e:
            logger.error(f"Error generating weather forecast: {str(e)}")
            raise
