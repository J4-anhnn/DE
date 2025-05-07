from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

sales = spark.read.option("header", True).csv("data/sales.csv")
products = spark.read.option("header", True).csv("data/products.csv")
stores = spark.read.option("header", True).csv("data/stores.csv")

sales.createOrReplaceTempView("sales")
products.createOrReplaceTempView("products")
stores.createOrReplaceTempView("stores")

query = '''
SELECT s.region, p.product_name, SUM(s.quantity) as total_sold
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY s.region, p.product_name
ORDER BY s.region, total_sold DESC
'''

result = spark.sql(query)

result.write.mode("overwrite").parquet("output/top_10_products_by_region.parquet")

parquet_df = spark.read.parquet("output/top_10_products_by_region.parquet")

parquet_df.show()

spark.stop()
