
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import argparse
from datetime import datetime

def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName("WeatherBatchProcessing")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def process_weather_data(spark, input_path, output_table, processing_date):
    """Process weather data from GCS and load into BigQuery"""
    # Define schema for raw weather data
    schema = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType()),
            StructField("lat", DoubleType())
        ])),
        StructField("weather", StringType()),
        StructField("base", StringType()),
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("feels_like", DoubleType()),
            StructField("temp_min", DoubleType()),
            StructField("temp_max", DoubleType()),
            StructField("pressure", DoubleType()),
            StructField("humidity", DoubleType())
        ])),
        StructField("visibility", DoubleType()),
        StructField("wind", StructType([
            StructField("speed", DoubleType()),
            StructField("deg", DoubleType())
        ])),
        StructField("clouds", StructType([
            StructField("all", DoubleType())
        ])),
        StructField("dt", DoubleType()),
        StructField("sys", StructType([
            StructField("type", DoubleType()),
            StructField("id", DoubleType()),
            StructField("country", StringType()),
            StructField("sunrise", DoubleType()),
            StructField("sunset", DoubleType())
        ])),
        StructField("timezone", DoubleType()),
        StructField("id", DoubleType()),
        StructField("name", StringType()),
        StructField("cod", DoubleType()),
        StructField("city_name", StringType()),
        StructField("collected_at", StringType())
    ])
    
    # Read JSON files from GCS
    df = spark.read.schema(schema).json(input_path)
    
    # Extract and transform data
    processed_df = df.select(
        col("city_name"),
        col("name").alias("city_original_name"),
        col("coord.lon").alias("longitude"),
        col("coord.lat").alias("latitude"),
        col("main.temp").alias("temperature"),
        col("main.feels_like").alias("feels_like"),
        col("main.humidity").alias("humidity"),
        col("main.pressure").alias("pressure"),
        col("wind.speed").alias("wind_speed"),
        col("wind.deg").alias("wind_direction"),
        col("clouds.all").alias("cloudiness"),
        expr("explode(from_json(weather, 'array<struct<main:string,description:string>>'))")
            .alias("weather_data"),
        to_timestamp(col("dt")).alias("measurement_time"),
        to_timestamp(col("sys.sunrise")).alias("sunrise"),
        to_timestamp(col("sys.sunset")).alias("sunset"),
        current_timestamp().alias("processing_time"),
        lit(processing_date).cast("date").alias("processing_date")
    )
    
    # Extract weather condition and description
    final_df = processed_df.select(
        col("city_name"),
        col("city_original_name"),
        col("longitude"),
        col("latitude"),
        col("temperature"),
        col("feels_like"),
        col("humidity"),
        col("pressure"),
        col("wind_speed"),
        col("wind_direction"),
        col("cloudiness"),
        col("weather_data.main").alias("weather_condition"),
        col("weather_data.description").alias("weather_description"),
        col("measurement_time"),
        col("sunrise"),
        col("sunset"),
        col("processing_time"),
        col("processing_date"),
        expr("(unix_timestamp(sunset) - unix_timestamp(sunrise)) / 3600").alias("day_length_hours")
    )
    
    # Write to BigQuery
    final_df.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", "weather-data-lake-2024") \
        .mode("append") \
        .save()
    
    return final_df.count()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Process weather data from GCS to BigQuery")
    parser.add_argument("--input_path", required=True, help="GCS path to input data")
    parser.add_argument("--output_table", required=True, help="BigQuery output table (project.dataset.table)")
    parser.add_argument("--processing_date", required=True, help="Processing date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        record_count = process_weather_data(
            spark, 
            args.input_path, 
            args.output_table, 
            args.processing_date
        )
        print(f"Successfully processed {record_count} weather records")
    except Exception as e:
        print(f"Error processing weather data: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()