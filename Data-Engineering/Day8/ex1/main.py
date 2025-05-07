from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    spark = SparkSession.builder \
        .appName("Exercise 1 - CSV Filter and Aggregation") \
        .getOrCreate()

    df = spark.read.csv("employees.csv", header=True, inferSchema=True)

    print("=== Dữ liệu gốc ===")
    df.show()

    filtered_df = df.filter(col("age") > 25)

    print("=== Dữ liệu sau khi lọc (age > 25) ===")
    filtered_df.show()

    result_df = filtered_df.groupBy("department") \
        .agg(avg("salary").alias("average_salary"))

    print("=== Kết quả trung bình lương theo phòng ban ===")
    result_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
