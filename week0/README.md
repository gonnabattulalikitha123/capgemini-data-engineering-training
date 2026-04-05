
# Phase 4: Business Pipeline & Analytics using PySpark

## 🎯 Objective

The goal of this project is to build an end-to-end ETL pipeline and generate business insights from raw data.

In this phase, the focus is on moving from simple queries to a structured data pipeline.

## 🔁 What is an ETL Pipeline

ETL stands for:

* Extract → Read data
* Transform → Clean and process data
* Load → Save or output results
## 📊 Dataset

In this project, sample data is created using PySpark DataFrames:

* Customers data (customer_id, name, city)
* Orders data (order_id, customer_id, order_amount)
* Sales data (date, amount)

This simulates real-world datasets.

## 🧹 Data Cleaning

Steps performed:

* Removed rows with null keys (customer_id)
* Removed duplicate records
* Filtered invalid values (amount > 0)
* Ensured clean data before processing

---

## ⚙️ Pipeline Steps

1. Read data (customers, orders, sales)
2. Clean data
3. Filter invalid records
4. Join datasets using customer_id
5. Perform aggregations
6. Apply business logic (segmentation)
7. Generate final report
8. Save output

## 📈 Business Tasks

### Task 1: Daily Sales

* Calculated total sales per date

### Task 2: City-wise Revenue

* Calculated total revenue for each city

### Task 3: Top 5 Customers

* Identified customers with highest total spend

### Task 4: Repeat Customers

* Customers with more than 1 order

### Task 5: Customer Segmentation

Business logic:

* total_spend > 10000 → Gold
* 5000–10000 → Silver
* <5000 → Bronze

### Task 6: Final Reporting Table

Columns included:

* customer_name
* city
* total_spend
* order_count
* segment

### Task 7: Save Output


final_df.write.mode("overwrite").csv("/samples/output/report")

## 🔄 Pipeline Flow

Raw Data
↓
Cleaning
↓
Filtering
↓
Join
↓
Aggregation
↓
Segmentation
↓
Final Report

---

## 🧠 Key Learnings

* Built a complete end-to-end ETL pipeline
* Understood importance of cleaning before joins
* Learned how to generate business insights
* Applied SQL concepts in PySpark
* Practiced pipeline thinking instead of isolated queries

---

## Reflection

### Why is cleaning done before joining tables?

Cleaning ensures only valid data is used. Joining dirty data can lead to incorrect results.

### What happens if null keys are not removed?

Joins may fail or produce incorrect matches.

### How did you decide join order?

Data was cleaned first, then joined using customer_id.

### Which step was difficult?

Maintaining correct pipeline flow and combining multiple steps.

### How is SQL similar to PySpark?

Both use similar logic like SELECT, JOIN, GROUP BY.

### Challenges with large data?

* Performance issues
* Memory usage
* Need for optimization

### Pipeline in simple steps

1. Read data
2. Clean data
3. Filter invalid records
4. Join datasets
5. Aggregate results
6. Apply business logic
7. Save output



##  Conclusion

This project demonstrates how raw data can be transformed into meaningful business insights using a structured ETL pipeline in PySpark.
