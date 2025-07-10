from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf, array_remove
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.appName("SparkPractice").getOrCreate()

data = [
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")
]
df = spark.createDataFrame(data, ["values", "delimiter"])
df.show(truncate=False)

# User defined function
def custom_split(values: str, delimiter: str):
    return values.split(delimiter)
udf_split = udf(custom_split, ArrayType(StringType()))

result = df.withColumn("split_values", udf_split("values", "delimiter"))
result.show(truncate=False)

extra = result.withColumn("extra", array_remove(result.split_values, ""))
# alternatively
# extra = result.withColumn("extra", expr("filter(split_values, x -> x != '')"))
extra.show(truncate=False)
