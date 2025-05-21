"""
Weather Data API

API để truy vấn dữ liệu thời tiết từ BigQuery
"""
import os
import sys
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# Thêm đường dẫn gốc của dự án vào sys.path để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import API_HOST, API_PORT, API_WORKERS, API_RATE_LIMIT, CITIES, setup_logging
from api.services.bigquery_service import BigQueryService

logger = setup_logging(__name__)

# Khởi tạo FastAPI app
app = FastAPI(
    title="Weather Data API",
    description="API để truy vấn dữ liệu thời tiết từ BigQuery",
    version="1.0.0"
)

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Định nghĩa models
class WeatherData(BaseModel):
    city_name: str
    measurement_date: date
    measurement_time: datetime
    temperature: float
    humidity: float
    pressure: float
    wind_speed: Optional[float] = None
    weather_condition: Optional[str] = None

class WeatherStats(BaseModel):
    city_name: str
    date: date
    metric: str
    avg_value: float
    min_value: float
    max_value: float
    std_dev: float

class DataPoint(BaseModel):
    date: date
    value: float

class WeatherTrend(BaseModel):
    city_name: str
    metric: str
    trend_direction: str
    change_rate: float
    data_points: List[DataPoint]

class WeatherAlert(BaseModel):
    city_name: str
    alert_type: str
    alert_level: str
    message: str
    value: Optional[float] = None
    threshold: Optional[float] = None
    timestamp: datetime

class WeatherForecast(BaseModel):
    city_name: str
    forecast_date: date
    temperature_min: float
    temperature_max: float
    humidity: float
    weather_condition: Optional[str] = None
    precipitation_probability: Optional[float] = None

# Dependency để lấy BigQuery service
def get_bigquery_service():
    return BigQueryService()

@app.get("/")
def read_root():
    """API root endpoint"""
    return {
        "message": "Welcome to Weather Data API",
        "version": "1.0.0",
        "endpoints": [
            "/weather",
            "/weather/stats",
            "/weather/trend",
            "/weather/alerts",
            "/weather/forecast",
            "/cities"
        ]
    }

@app.get("/cities", response_model=List[str])
def get_cities():
    """Lấy danh sách các thành phố có dữ liệu"""
    return CITIES

@app.get("/weather", response_model=List[WeatherData])
def get_weather(
    city: Optional[str] = Query(None, description="Tên thành phố"),
    days: int = Query(7, description="Số ngày dữ liệu (tính từ hiện tại trở về trước)"),
    limit: int = Query(100, description="Số lượng bản ghi tối đa"),
    bq_service: BigQueryService = Depends(get_bigquery_service)
):
    """
    Lấy dữ liệu thời tiết với bộ lọc tùy chọn
    """
    try:
        # Tính toán khoảng thời gian
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Kiểm tra city nếu được cung cấp
        if city and city not in CITIES:
            raise HTTPException(status_code=400, detail=f"City '{city}' not found. Available cities: {', '.join(CITIES)}")
        
        # Truy vấn dữ liệu
        results = bq_service.get_weather_data(city, start_date, end_date, limit)
        
        return results
    except Exception as e:
        logger.error(f"Error in get_weather: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/stats", response_model=List[WeatherStats])
def get_weather_stats(
    metric: str = Query(..., description="Chỉ số thời tiết (temperature, humidity, pressure, wind_speed)"),
    city: Optional[str] = Query(None, description="Tên thành phố"),
    days: int = Query(30, description="Số ngày dữ liệu (tính từ hiện tại trở về trước)"),
    bq_service: BigQueryService = Depends(get_bigquery_service)
):
    """
    Lấy thống kê thời tiết theo chỉ số
    """
    try:
        # Kiểm tra metric
        valid_metrics = ["temperature", "humidity", "pressure", "wind_speed"]
        if metric not in valid_metrics:
            raise HTTPException(status_code=400, detail=f"Invalid metric: {metric}. Must be one of {valid_metrics}")
        
        # Kiểm tra city nếu được cung cấp
        if city and city not in CITIES:
            raise HTTPException(status_code=400, detail=f"City '{city}' not found. Available cities: {', '.join(CITIES)}")
        
        # Tính toán khoảng thời gian
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Truy vấn dữ liệu
        results = bq_service.get_weather_stats(city, start_date, end_date, metric)
        
        return results
    except Exception as e:
        logger.error(f"Error in get_weather_stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/trend", response_model=List[WeatherTrend])
def get_weather_trend(
    city: str = Query(..., description="Tên thành phố"),
    metric: str = Query(..., description="Chỉ số thời tiết (temperature, humidity, pressure, wind_speed)"),
    days: int = Query(30, description="Số ngày dữ liệu (tính từ hiện tại trở về trước)"),
    bq_service: BigQueryService = Depends(get_bigquery_service)
):
    """
    Lấy xu hướng thời tiết cho một thành phố
    """
    try:
        # Kiểm tra metric
        valid_metrics = ["temperature", "humidity", "pressure", "wind_speed"]
        if metric not in valid_metrics:
            raise HTTPException(status_code=400, detail=f"Invalid metric: {metric}. Must be one of {valid_metrics}")
        
        # Kiểm tra city
        if city not in CITIES:
            raise HTTPException(status_code=400, detail=f"City '{city}' not found. Available cities: {', '.join(CITIES)}")
        
        # Truy vấn dữ liệu
        results = bq_service.get_weather_trend(city, days, metric)
        
        return results
    except Exception as e:
        logger.error(f"Error in get_weather_trend: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/alerts", response_model=List[WeatherAlert])
def get_weather_alerts(
    city: Optional[str] = Query(None, description="Tên thành phố"),
    hours: int = Query(24, description="Số giờ dữ liệu (tính từ hiện tại trở về trước)"),
    bq_service: BigQueryService = Depends(get_bigquery_service)
):
    """
    Lấy cảnh báo thời tiết gần đây
    """
    try:
        # Kiểm tra city nếu được cung cấp
        if city and city not in CITIES:
            raise HTTPException(status_code=400, detail=f"City '{city}' not found. Available cities: {', '.join(CITIES)}")
        
        # Truy vấn dữ liệu
        results = bq_service.get_weather_alerts(city, hours)
        
        return results
    except Exception as e:
        logger.error(f"Error in get_weather_alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/forecast", response_model=List[WeatherForecast])
def get_weather_forecast(
    city: str = Query(..., description="Tên thành phố"),
    days: int = Query(7, description="Số ngày dự báo"),
    bq_service: BigQueryService = Depends(get_bigquery_service)
):
    """
    Lấy dự báo thời tiết cho một thành phố
    
    Lưu ý: Đây là triển khai đơn giản sử dụng dữ liệu lịch sử để tạo "dự báo".
    Trong hệ thống thực tế, bạn sẽ tích hợp với dịch vụ dự báo hoặc triển khai mô hình dự báo phù hợp.
    """
    try:
        # Kiểm tra city
        if city not in CITIES:
            raise HTTPException(status_code=400, detail=f"City '{city}' not found. Available cities: {', '.join(CITIES)}")
        
        # Truy vấn dữ liệu
        results = bq_service.get_weather_forecast(city, days)
        
        return results
    except Exception as e:
        logger.error(f"Error in get_weather_forecast: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Hàm chính để chạy API server"""
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        workers=API_WORKERS,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()
