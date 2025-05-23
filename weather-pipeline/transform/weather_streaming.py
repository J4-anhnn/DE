"""
Spark_Streaming_job_to_process_real-time_weather_data_from_Kafka
"""
from_pyspark.sql_import_SparkSession
from_pyspark.sql.functions_import_col,_from_json,_to_timestamp,_current_timestamp,_window,_avg,_max,_min
from_pyspark.sql.types_import_StructType,_StructField,_StringType,_DoubleType,_TimestampType
import_os

def_create_spark_session():
____"""Create_and_configure_Spark_session_for_streaming"""
____return_(SparkSession.builder
____________.appName("WeatherStreaming")
____________.config("spark.jars.packages",_"org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.google.cloud:spark-bigquery-with-dependencies_2.12:0.32.2")
____________.getOrCreate())

def_define_schema():
____"""Define_schema_for_the_weather_data_from_Kafka"""
____return_StructType([
________StructField("city",_StringType(),_True),
________StructField("temperature",_DoubleType(),_True),
________StructField("humidity",_DoubleType(),_True),
________StructField("pressure",_DoubleType(),_True),
________StructField("wind_speed",_DoubleType(),_True),
________StructField("weather_condition",_StringType(),_True),
________StructField("timestamp",_DoubleType(),_True)
____])

def_process_streaming_data():
____"""Process_streaming_weather_data_from_Kafka"""
____spark_=_create_spark_session()
____spark.sparkContext.setLogLevel("WARN")
____
____#_Define_schema
____schema_=_define_schema()
____
____#_Read_from_Kafka
____kafka_df_=_spark.readStream_\
________.format("kafka")_\
________.option("kafka.bootstrap.servers",_"kafka:9092")_\
________.option("subscribe",_"weather-events")_\
________.option("startingOffsets",_"latest")_\
________.load()
____
____#_Parse_JSON_data
____parsed_df_=_kafka_df_\
________.selectExpr("CAST(value_AS_STRING)_as_json")_\
________.select(from_json(col("json"),_schema).alias("data"))_\
________.select("data.*")_\
________.withColumn("event_time",_to_timestamp(col("timestamp")))_\
________.withColumn("processing_time",_current_timestamp())
____
____#_Process_data_-_calculate_metrics_over_windows
____windowed_df_=_parsed_df_\
________.withWatermark("event_time",_"10_minutes")_\
________.groupBy(
____________col("city"),
____________window(col("event_time"),_"1_hour")
________)_\
________.agg(
____________avg("temperature").alias("avg_temperature"),
____________max("temperature").alias("max_temperature"),
____________min("temperature").alias("min_temperature"),
____________avg("humidity").alias("avg_humidity"),
____________avg("pressure").alias("avg_pressure"),
____________avg("wind_speed").alias("avg_wind_speed")
________)
____
____#_Output_to_console_for_debugging
____console_query_=_windowed_df_\
________.writeStream_\
________.outputMode("update")_\
________.format("console")_\
________.option("truncate",_False)_\
________.start()
____
____#_Output_to_BigQuery
____bq_table_=_os.environ.get("BIGQUERY_TABLE",_"weather_project.weather_data.weather_realtime")
____
____bq_query_=_windowed_df_\
________.writeStream_\
________.outputMode("append")_\
________.format("bigquery")_\
________.option("table",_bq_table)_\
________.option("checkpointLocation",_"/tmp/checkpoint/weather")_\
________.option("temporaryGcsBucket",_os.environ.get("TEMP_GCS_BUCKET"))_\
________.start()
____
____#_Wait_for_termination
____spark.streams.awaitAnyTermination()

if___name___==_"__main__":
____process_streaming_data()
