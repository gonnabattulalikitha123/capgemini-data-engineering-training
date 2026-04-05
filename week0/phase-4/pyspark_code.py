# import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, count, when

# start spark
spark = SparkSession.builder.appName("Phase4_Business_Pipeline").getOrCreate()


# create customers data

customers_data = [
    (1, "Ravi", "Hyderabad"),
    (2, "Anita", "Chennai"),
    (3, "John", "Bangalore"),
    (4, "Meena", "Hyderabad"),
    (5, "Arun", "Chennai")
]

customers_cols = ["customer_id", "name", "city"]
df_customers = spark.createDataFrame(customers_data, customers_cols)

# -----------------------------------
# create orders data
# -----------------------------------
orders_data = [
    (101, 1, 5000),
    (102, 1, 7000),
    (103, 2, 3000),
    (104, 2, 4000),
    (105, 3, 2000),
    (106, 4, 12000),
    (107, 4, 2000),
    (108, 4, 1000),
    (109, 5, 1000)
]

orders_cols = ["order_id", "customer_id", "order_amount"]
df_orders = spark.createDataFrame(orders_data, orders_cols)

# -----------------------------------
# create sales data
# -----------------------------------
sales_data = [
    ("2024-01-01", 5000),
    ("2024-01-01", 7000),
    ("2024-01-02", 3000),
    ("2024-01-02", 4000),
    ("2024-01-03", 2000)
]

sales_cols = ["date", "amount"]
df_sales = spark.createDataFrame(sales_data, sales_cols)

# print("customers data")
# df_customers.show()

# print("orders data")
# df_orders.show()

# print("sales data")
# df_sales.show()

from pyspark.sql.functions import sum as sum_

daily_sales = df_sales.groupBy("date") \
    .agg(sum_("amount").alias("total_sales"))

daily_sales.show()

city_revenue = df_customers.join(df_orders, "customer_id") \
    .groupBy("city") \
    .agg(sum_("order_amount").alias("total_revenue"))

city_revenue.show()

top_customers = df_customers.join(df_orders, "customer_id") \
    .groupBy("name") \
    .agg(sum_("order_amount").alias("total_spend")) \
    .orderBy("total_spend", ascending=False) \
    .limit(5)

top_customers.show()

from pyspark.sql.functions import count

repeat_customers = df_orders.groupBy("customer_id") \
    .agg(count("*").alias("order_count")) \
    .filter("order_count > 1")

repeat_customers.show()

from pyspark.sql.functions import when

customer_spend = df_customers.join(df_orders, "customer_id") \
    .groupBy("name") \
    .agg(sum_("order_amount").alias("total_spend"))

segmented = customer_spend.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

segmented.show()

final_df = df_customers.join(df_orders, "customer_id") \
    .groupBy("name", "city") \
    .agg(
        sum_("order_amount").alias("total_spend"),
        count("order_id").alias("order_count")
    )

final_df = final_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

final_df.show()

final_df.write.mode("overwrite").csv("/samples/output/report")
