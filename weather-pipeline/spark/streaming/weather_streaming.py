from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    
    # Define schema for weather data
    schema = StructType([
        StructField("city_name", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("timestamp", TimestampType())
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather-raw") \
        .load()

    # Parse JSON data
    weather_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Write to BigQuery
    query = weather_df.writeStream \
        .outputMode("append") \
        .format("bigquery") \
        .option("table", "weather_data.weather_streaming") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
