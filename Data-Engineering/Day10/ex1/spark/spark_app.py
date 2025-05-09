from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, FloatType, LongType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("sensor_id", IntegerType()) \
    .add("value", FloatType()) \
    .add("timestamp", LongType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-data") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

aggregated = parsed_df.groupBy("sensor_id").count()

query = aggregated.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
