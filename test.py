from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkPractice").getOrCreate()
sc = spark.sparkContext

print(sc)