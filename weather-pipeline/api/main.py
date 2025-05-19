from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from datetime import datetime, timedelta
from typing import List, Optional
import os

app = FastAPI(title="Weather API")

@app.get("/")
async def root():
    return {"message": "Weather API is running"}

@app.get("/weather/current/{city}")
async def get_current_weather(city: str):
    try:
        client = bigquery.Client()
        
        query = f"""
        SELECT *
        FROM `weather_data.weather_streaming`
        WHERE city_name = @city
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city", "STRING", city)
            ]
        )
        
        results = client.query(query, job_config=job_config).result()
        for row in results:
            return dict(row)
        
        # If no results
        raise HTTPException(status_code=404, detail=f"No weather data found for {city}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/history/{city}")
async def get_weather_history(
    city: str,
    days: Optional[int] = 7
):
    try:
        client = bigquery.Client()
        
        query = f"""
        SELECT *
        FROM `weather_data.weather_streaming`
        WHERE city_name = @city
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        ORDER BY timestamp DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city", "STRING", city),
                bigquery.ScalarQueryParameter("days", "INT64", days)
            ]
        )
        
        results = client.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
