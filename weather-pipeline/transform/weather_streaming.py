"""
Spark Streaming job to process real-time weather data from Kafka
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, window, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

def create_spark_session():
    """Create and configure Spark session for streaming"""
    return (SparkSession.builder
            .appName("WeatherStreaming")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.google.cloud:spark-bigquery-with-dependencies_2.12:0.32.2")
            .getOrCreate())

def define_schema():
    """Define schema for the weather data from Kafka"""
    return StructType([
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

def process_streaming_data():
    """Process streaming weather data from Kafka"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define schema
    schema = define_schema()
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather-events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("processing_time", current_timestamp())
    
    # Process data - calculate metrics over windows
    windowed_df = parsed_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            col("city"),
            window(col("event_time"), "1 hour")
        ) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("pressure").alias("avg_pressure"),
            avg("wind_speed").alias("avg_wind_speed")
        )
    
    # Output to console for debugging
    console_query = windowed_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Output to BigQuery
    bq_table = os.environ.get("BIGQUERY_TABLE", "weather_project.weather_data.weather_realtime")
    
    bq_query = windowed_df \
        .writeStream \
        .outputMode("append") \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("checkpointLocation", "/tmp/checkpoint/weather") \
        .option("temporaryGcsBucket", os.environ.get("TEMP_GCS_BUCKET")) \
        .start()
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_streaming_data()
