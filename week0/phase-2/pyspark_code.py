from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL to PySpark").getOrCreate()

# Customers Data
customers_data = [
    (1, "Alice", "New York"),
    (2, "Bob", "Los Angeles"),
    (3, "Charlie", "Chicago"),
    (4, "David", "New York")
]

customers = spark.createDataFrame(customers_data, ["customer_id", "name", "city"])

# Orders Data
orders_data = [
    (101, 1, 100),
    (102, 1, 200),
    (103, 2, 150),
    (104, 3, 300)
]

orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_amount"])

customers.show()
orders.show()
from pyspark.sql.functions import sum

orders.groupBy("customer_id") \
      .agg(sum("order_amount").alias("total_spend")) \
      .show()

orders.groupBy("customer_id") \
      .agg(sum("order_amount").alias("total_spend")) \
      .orderBy("total_spend", ascending=False) \
      .limit(3) \
      .show()

customers.join(orders, on="customer_id", how="left") \
         .filter(orders.customer_id.isNull()) \
         .select("customer_id") \
         .show()

customers.join(orders, on="customer_id") \
         .groupBy("city") \
         .agg(sum("order_amount").alias("total_revenue")) \
         .show()

from pyspark.sql.functions import avg

orders.groupBy("customer_id") \
      .agg(avg("order_amount").alias("avg_order")) \
      .show()

from pyspark.sql.functions import count

orders.groupBy("customer_id") \
      .agg(count("*").alias("order_count")) \
      .filter("order_count > 1") \
      .show()

orders.groupBy("customer_id") \
      .agg(sum("order_amount").alias("total_spend")) \
      .orderBy("total_spend", ascending=False) \
      .show()
