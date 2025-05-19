# Tiếp tục file bigquery_service.py
cat > api/services/bigquery_service.py << 'EOF'
"""
BigQuery service for weather data API
"""
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BigQueryService:
    """Service for querying weather data from BigQuery"""
    
    def __init__(self):
        """Initialize BigQuery client"""
        self.client = bigquery.Client()
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id")
        self.dataset_id = os.getenv("BIGQUERY_DATASET", "weather_data")
        self.batch_table = os.getenv("BIGQUERY_BATCH_TABLE", "weather_batch")
        self.realtime_table = os.getenv("BIGQUERY_REALTIME_TABLE", "weather_realtime")
        self.analytics_table = os.getenv("BIGQUERY_ANALYTICS_TABLE", "weather_analytics")
        
        logger.info(f"Initialized BigQuery service for project {self.project_id}")
    
    def get_weather_data(
        self, 
        city: Optional[str], 
        date_from: date, 
        date_to: date, 
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        Query weather data with optional filtering
        """
        query = f"""
        SELECT
            city_name,
            temperature,
            feels_like,
            humidity,
            pressure,
            wind_speed,
            wind_direction,
            cloudiness,
            weather_condition,
            weather_description,
            measurement_time
        FROM
            `{self.project_id}.{self.dataset_id}.{self.batch_table}`
        WHERE
            processing_date BETWEEN DATE('{date_from}') AND DATE('{date_to}')
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
    
    def get_weather_stats(
        self, 
        city: Optional[str], 
        date_from: date, 
        date_to: date, 
        metric: str
    ) -> List[Dict[str, Any]]:
        """
        Query weather statistics
        """
        # Validate metric
        valid_metrics = ["temperature", "humidity", "pressure", "wind_speed"]
        if metric not in valid_metrics:
            raise ValueError(f"Invalid metric: {metric}. Must be one of {valid_metrics}")
        
        query = f"""
        SELECT
            city_name,
            DATE(measurement_time) as date,
            '{metric}' as metric,
            AVG({metric}) as avg_value,
            MIN({metric}) as min_value,
            MAX({metric}) as max_value,
            STDDEV({metric}) as std_dev
        FROM
            `{self.project_id}.{self.dataset_id}.{self.batch_table}`
        WHERE
            processing_date BETWEEN DATE('{date_from}') AND DATE('{date_to}')
        """
        
        if city:
            query += f" AND city_name = '{city}'"
        
        query += """
        GROUP BY city_name, DATE(measurement_time)
        ORDER BY city_name, date
        """
        
        logger.info(f"Executing query: {query}")
        query_job = self.client.query(query)
        results = query_job.result()
        
        return [dict(row) for row in results]
    
    def get_weather_trend(
        self, 
        city: str, 
        days: int, 
        metric: str
    ) -> List[Dict[str, Any]]:
        """
        Query weather trend for a specific city
        """
        # Validate metric
        valid_metrics = ["temperature", "humidity", "pressure", "wind_speed"]
        if metric not in valid_metrics:
            raise ValueError(f"Invalid metric: {metric}. Must be one of {valid_metrics}")
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Query for daily averages
        query = f"""
        WITH daily_data AS (
            SELECT
                city_name,
                DATE(measurement_time) as date,
                AVG({metric}) as value
            FROM
                `{self.project_id}.{self.dataset_id}.{self.batch_table}`
            WHERE
                city_name = '{city}'
                AND DATE(measurement_time) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            GROUP BY city_name, DATE(measurement_time)
            ORDER BY date
        ),
        
        trend_calc AS (
            SELECT
                city_name,
                '{metric}' as metric,
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
            t.metric,
            t.trend_direction,
            t.change_rate,
            STRUCT(d.date, d.value) as data_point
        FROM daily_data d
        JOIN trend_calc t ON d.city_name = t.city_name
        ORDER BY d.date
        """
        
        logger.info(f"Executing query: {query}")
        query_job = self.client.query(query)
        results = query_job.result()
        
        # Process results to group data points
        trends = {}
        for row in results:
            row_dict = dict(row)
            city = row_dict['city_name']
            
            if city not in trends:
                trends[city] = {
                    'city_name': city,
                    'metric': row_dict['metric'],
                    'trend_direction': row_dict['trend_direction'],
                    'change_rate': row_dict['change_rate'],
                    'data_points': []
                }
            
            trends[city]['data_points'].append({
                'date': row_dict['data_point']['date'],
                'value': row_dict['data_point']['value']
            })
        
        return list(trends.values())
    
    def get_weather_alerts(
        self, 
        city: Optional[str], 
        hours: int
    ) -> List[Dict[str, Any]]:
        """
        Query recent weather alerts
        """
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        query = f"""
        SELECT
            city_name,
            CASE
                WHEN temperature > 35 THEN 'HIGH_TEMPERATURE'
                WHEN temperature < 5 THEN 'LOW_TEMPERATURE'
                WHEN wind_speed > 15 THEN 'HIGH_WIND'
                WHEN humidity > 90 THEN 'HIGH_HUMIDITY'
                ELSE 'OTHER'
            END as alert_type,
            CASE
                WHEN temperature > 38 OR temperature < 0 OR wind_speed > 20 THEN 'critical'
                WHEN temperature > 35 OR temperature < 5 OR wind_speed > 15 OR humidity > 90 THEN 'warning'
                ELSE 'info'
            END as alert_level,
            CASE
                WHEN temperature > 35 THEN 'High temperature detected'
                WHEN temperature < 5 THEN 'Low temperature detected'
                WHEN wind_speed > 15 THEN 'High wind speed detected'
                WHEN humidity > 90 THEN 'High humidity detected'
                ELSE 'Weather condition alert'
            END as message,
            CASE
                WHEN temperature > 35 OR temperature < 5 THEN temperature
                WHEN wind_speed > 15 THEN wind_speed
                WHEN humidity > 90 THEN humidity
                ELSE NULL
            END as value,
            CASE
                WHEN temperature > 35 THEN 35
                WHEN temperature < 5 THEN 5
                WHEN wind_speed > 15 THEN 15
                WHEN humidity > 90 THEN 90
                ELSE NULL
            END as threshold,
            measurement_time as timestamp
        FROM
            `{self.project_id}.{self.dataset_id}.{self.batch_table}`
        WHERE
            measurement_time BETWEEN TIMESTAMP('{start_time.isoformat()}') AND TIMESTAMP('{end_time.isoformat()}')
            AND (temperature > 35 OR temperature < 5 OR wind_speed > 15 OR humidity > 90)
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
        """
        # For demonstration purposes, we'll use historical data from the same days last year
        # as a simple "forecast"
        
        # Calculate date range for last year
        end_date = datetime.now().date()
        forecast_dates = [end_date + timedelta(days=i+1) for i in range(days)]
        last_year_dates = [(date - timedelta(days=365)).strftime("%Y-%m-%d") for date in forecast_dates]
        
        date_conditions = " OR ".join([f"DATE(measurement_time) = '{date}'" for date in last_year_dates])
        
        query = f"""
        WITH historical_data AS (
            SELECT
                city_name,
                DATE(measurement_time) as historical_date,
                DATE_ADD(DATE(measurement_time), INTERVAL 365 DAY) as forecast_date,
                AVG(temperature) as avg_temp,
                MIN(temperature) as min_temp,
                MAX(temperature) as max_temp,
                AVG(humidity) as avg_humidity,
                MODE(weather_condition) as common_condition,
                COUNT(CASE WHEN weather_condition LIKE '%rain%' OR weather_condition LIKE '%shower%' THEN 1 END) / COUNT(*) * 100 as rain_probability
            FROM
                `{self.project_id}.{self.dataset_id}.{self.batch_table}`
            WHERE
                city_name = '{city}'
                AND ({date_conditions})
            GROUP BY city_name, DATE(measurement_time)
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