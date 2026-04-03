# ============================================
# PySpark ETL Pipeline - Phase 3 
# ============================================

# Import Spark Session
from pyspark.sql import SparkSession

# Import functions for transformations
from pyspark.sql.functions import col, sum

# --------------------------------------------
# STEP 1: Start Spark Session
# --------------------------------------------
# This initializes PySpark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ETL_Pipeline_Phase3") \
    .getOrCreate()

# --------------------------------------------
# STEP 2: Extract (Create Raw Data)
# --------------------------------------------
# In real projects, data comes from files
# Here we simulate raw (dirty) data

data = [
    (1, "Amit", 25, "West", "100"),
    (2, "Riya", None, "East", "200"),
    (3, "John", -5, "West", "300"),
    (4, "Anita", 30, None, "400"),
    (5, "Rahul", 28, "North", "abc"),
    (6, None, 35, "South", "500")
]

columns = ["customer_id", "name", "age", "region", "amount"]

df = spark.createDataFrame(data, columns)

print("🔹 Raw Data:")
df.show()

# --------------------------------------------
# STEP 3: Transform (Data Cleaning)
# --------------------------------------------

# 3.1 Remove rows where customer_id is NULL
df = df.dropna(subset=["customer_id"])

# 3.2 Fill missing names with 'Unknown'
df = df.fillna({"name": "Unknown"})

# 3.3 Remove invalid ages (age should be > 0)
df = df.filter(df["age"] > 0)

# 3.4 Remove rows where region is NULL
df = df.dropna(subset=["region"])

# 3.5 Convert amount column to numeric (double)
df = df.withColumn("amount", col("amount").cast("double"))

# Remove rows where amount conversion failed (null values)
df = df.dropna(subset=["amount"])

print("🔹 Cleaned Data:")
df.show()

# --------------------------------------------
# STEP 4: Aggregation (Business Logic)
# --------------------------------------------
# Calculate total sales per region

result = df.groupBy("region") \
           .agg(sum("amount").alias("total_sales"))

print("🔹 Total Sales Per Region:")
result.show()



# --------------------------------------------
# END OF PIPELINE
# --------------------------------------------

print("🎯 ETL Pipeline Completed Successfully!")

# Stop Spark session
spark.stop()
