from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, first

spark = SparkSession.builder.appName("SparkPractice").getOrCreate()

data = [
    Row(
        name="Manuel",
        movieRatings=[
            Row(movieName="Logan", rating=1.5),
            Row(movieName="Zoolander", rating=3.0),
            Row(movieName="John Wick", rating=2.5)
        ]
    ),
    Row(
        name="John",
        movieRatings=[
            Row(movieName="Logan", rating=2.0),
            Row(movieName="Zoolander", rating=3.5),
            Row(movieName="John Wick", rating=3.0)
        ]
    )
]

df = spark.createDataFrame(data)
df.show(truncate=False)

result = df.select(
    "name",
    explode("movieRatings").alias("rating")
).select(
    "name",
    "rating.movieName",
    "rating.rating"
)

result = result.groupBy("name").pivot("movieName").agg(first("rating"))

result.show(truncate=False)

