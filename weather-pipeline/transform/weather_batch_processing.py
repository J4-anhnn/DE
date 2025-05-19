"""
Spark batch job to process weather data from GCS and load into BigQuery
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import argparse
from datetime import datetime

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process weather data from GCS to BigQuery')
    parser.add_argument('--input_path', required=True, help='GCS path to input data')
    parser.add_argument('--output_table', required=True, help='BigQuery output table (project.dataset.table)')
    parser.add_argument('--processing_date', required=True, help='Processing date (YYYY-MM-DD)')
    parser.add_argument('--temp_gcs_bucket', required=False, help='Temporary GCS bucket for BigQuery loading')
    return parser.parse_args()

def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName("WeatherBatchProcessing")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .getOrCreate())

def process_weather_data(spark, input_path, output_table, processing_date, temp_bucket=None):
    """Process weather data from GCS and load into BigQuery"""
    # Read JSON files from GCS
    df = spark.read.json(input_path)
    
    # Extract and flatten nested fields
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
        col("weather")[0]["main"].alias("weather_condition"),
        col("weather")[0]["description"].alias("weather_description"),
        to_timestamp(col("sys.sunrise")).alias("sunrise"),
        to_timestamp(col("sys.sunset")).alias("sunset"),
        to_timestamp(col("dt")).alias("measurement_time"),
        current_timestamp().alias("processing_time"),
        to_timestamp(lit(processing_date)).cast("date").alias("processing_date")
    )
    
    # Add calculated fields
    processed_df = processed_df.withColumn(
        "day_length_hours", 
        expr("(unix_timestamp(sunset) - unix_timestamp(sunrise)) / 3600")
    )
    
    # Write to BigQuery
    write_options = {}
    if temp_bucket:
        write_options["temporaryGcsBucket"] = temp_bucket
        
    processed_df.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
    
    print(f"Successfully processed {processed_df.count()} weather records")
    return processed_df.count()

def main():
    """Main entry point"""
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        record_count = process_weather_data(
            spark, 
            args.input_path, 
            args.output_table, 
            args.processing_date,
            args.temp_gcs_bucket
        )
        print(f"Job completed successfully. Processed {record_count} records.")
    except Exception as e:
        print(f"Error processing weather data: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
