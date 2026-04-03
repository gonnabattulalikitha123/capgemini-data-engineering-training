# Phase 3: ETL Pipeline Practice using PySpark

## Project Overview
This project is a beginner-friendly ETL (Extract → Transform → Load) pipeline using PySpark.  
The goal is to learn how to handle **real-world messy data**, clean it, transform it, and perform aggregations using a structured pipeline.  

We work with multiple data sources: CSV, JSON, and Parquet files.

---

## Key Concepts Learned

# 1. Data is not clean by default
- Real data may have nulls, duplicates, or invalid values
- Important to identify issues before processing

# 2. Data Cleaning
- Drop rows with missing key columns using `dropna()`
- Fill missing values using `fillna()`
- Remove duplicates using `dropDuplicates()`
- Filter invalid records (like negative age) using `filter()`

# 3. Schema Awareness
- Inspect column types using `printSchema()`
- Ensure data types are correct before transformations

# 4. Pipeline Thinking
- ETL is a structured sequence of steps:
  1. Extract data from files
  2. Clean and validate data
  3. Transform and join datasets
  4. Aggregate results
  5. Load or save final output

# 5. SQL → PySpark Mapping
- Practice writing SQL queries first, then convert to PySpark transformations:
  - `SELECT` → `select()`
  - `WHERE` → `filter()`
  - `GROUP BY` → `groupBy()`
  - `JOIN` → `join()`
  - Aggregations → `agg(sum(), count(), avg())`

# 6. Validation
- Compare row counts before and after cleaning
- Ensure no nulls in key columns
- Check duplicates are removed
- Filter invalid values properly

---

## Project Steps

# STEP 1: Read CSV file
- Loaded `customers.csv` using PySpark
- Displayed raw data using `show()` and schema using `printSchema()`

# STEP 2: Identify Missing Values
- Checked null counts for each column
- Understood which columns need cleaning

# STEP 3: Clean Data
- Removed rows with null keys (`customer_id`, `name`, `city`)
- Filled optional missing values (`age`) with 0
- Removed duplicates
- Filtered invalid ages (`age > 0`)

# STEP 4: Read JSON and Parquet
- Loaded `orders.json` and `sales.parquet` using PySpark
- Displayed sample rows and schema

# STEP 5: Business Pipeline Exercises
1. Calculate **daily sales** from sales data
2. Calculate **city-wise revenue** by joining customers and orders
3. Identify **repeat customers** with more than 2 orders
4. Find **highest spending customer per city**
5. Build a **final reporting table** with customer, city, total spend, and order count

---

## Key Learnings and Takeaways
- Real-world data is messy and requires cleaning
- Cleaning and filtering should be done **before aggregation**
- Pipelines are a sequence of structured steps, not random queries
- Validating data at each step is crucial
- SQL knowledge helps understand PySpark transformations
- Gained confidence in handling multiple data formats (CSV, JSON, Parquet)

---

## How to Run
1. Open any PySpark environment (local or online compiler)
2. Place sample files (`customers.csv`, `orders.json`, `sales.parquet`) in `/samples/`
3. Copy the PySpark ETL code (`etl_pipeline_student.py`)
4. Run the script
5. Observe cleaned data, aggregation results, and reporting table

---

## Summary
This project demonstrates **Phase 3 learning objectives**:
- Extract → Clean → Transform → Aggregate → Load
- Pipeline thinking
- Data validation
- Working with real-world messy data
- Mapping SQL to PySpark transformations

By completing this project, i can confidently take raw data and process it into meaningful outputs using PySpark.
