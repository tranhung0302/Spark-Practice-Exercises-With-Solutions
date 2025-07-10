from pyspark.sql import SparkSession
from pyspark.sql.functions import min

spark = SparkSession.builder.appName("SparkPractice").getOrCreate()

data = [
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")
]

df = spark.createDataFrame(data, ["id", "value"])

# initialize priorities_df as a DataFrame with an order of priorities
priorities = ["MV1", "MV2", "VPV", "Others"]
priorities_rdd = spark.sparkContext.parallelize(priorities)
priorities_df = priorities_rdd.zipWithIndex().toDF(["name", "rank"])

# find the minimum rank for each id
mins = df.join(priorities_df, df.value == priorities_df.name)\
    .groupBy("id")\
    .agg(min("rank").alias("min_rank"))

# join the minimum ranks with the priorities to take the names
result = mins.join(priorities_df, mins.min_rank == priorities_df.rank)\
    .select("id", "name")

result.show()
