from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank
from pyspark.ml.feature import Bucketizer

spark = SparkSession.builder.appName("Segmentation_Practice").getOrCreate()

data = [
    ("Ravi", 12000),
    ("Anita", 8000),
    ("John", 3000),
    ("Meena", 15000),
    ("Arun", 6000),
    ("Kiran", 2000),
    ("Sita", 11000)
]

cols = ["name", "total_spend"]
df = spark.createDataFrame(data, cols)

print("original data")
df.show()

df_cond = df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

print("conditional segmentation")
df_cond.show()

segment_count = df_cond.groupBy("segment") \
    .agg(count("*").alias("customer_count"))

print("customers per segment")
segment_count.show()

quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

df_quantile = df.withColumn(
    "segment",
    when(col("total_spend") <= q1, "Bronze")
    .when((col("total_spend") > q1) & (col("total_spend") <= q2), "Silver")
    .otherwise("Gold")
)

print("quantile segmentation")
df_quantile.show()

splits = [-float("inf"), 5000, 10000, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

df_bucket = bucketizer.transform(df)

print("bucketizer output")
df_bucket.show()

window = Window.orderBy("total_spend")

df_rank = df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)

print("percent rank")
df_rank.show()

spark.stop()
