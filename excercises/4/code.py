from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, slice

spark = SparkSession.builder.appName("SparkPractice").getOrCreate()

data = [
    (0, 0),
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 0),
    (6, 1),
    (7, 2),
    (8, 3),
    (9, 4),
    (10, 0),
    (11, 1),
    (12, 2),
    (13, 3),
    (14, 4),
    (15, 0),
    (16, 1),
    (17, 2),
    (18, 3),
    (19, 4)
]

df = spark.createDataFrame(data, ["id", "key"])

result = df.groupBy("key").agg(collect_set("id").alias("all"))

result = result.withColumn("only_first_three", slice(result.all, 1, 3))

result.show(truncate=False)