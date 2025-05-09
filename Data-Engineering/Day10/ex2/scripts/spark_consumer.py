from pyspark.sql import SparkSession

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("SimpleSparkTest") \
    .getOrCreate()

print("=== SPARK SESSION ĐƯỢC KHỞI TẠO THÀNH CÔNG ===")
print(f"Spark version: {spark.version}")
print(f"Spark master: {spark.sparkContext.master}")

# Tạo dữ liệu đơn giản để kiểm tra
data = [(1, "test")]
df = spark.createDataFrame(data, ["id", "value"])

# In kết quả
print("=== SAMPLE DATA ===")
df.show()

# Thêm xử lý
df2 = df.withColumn("processed", df.id * 10)
print("=== PROCESSED DATA ===")
df2.show()

print("=== XỬ LÝ HOÀN TẤT ===")
spark.stop()
