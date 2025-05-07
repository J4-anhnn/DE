from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("Exercise 2: Save and Load") \
        .getOrCreate()

    df = spark.read.csv("employees.csv", header=True, inferSchema=True)
    print("=== Schema ===")
    df.printSchema()

    filtered_df = df.filter(col("age") > 25)

    filtered_df.write.csv("output/filtered_employees", header=True, mode="overwrite")

    filtered_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/mydb") \
        .option("dbtable", "filtered_employees") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

if __name__ == "__main__":
    main()
