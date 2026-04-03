# PySpark ETL Pipeline – Phase-3
## 🎯 Objective

This project demonstrates a simple ETL (Extract → Transform → Load) pipeline using PySpark.
The goal is to understand how raw data is cleaned and transformed step-by-step into meaningful output.

---

## 🔁 What is ETL?

* **Extract** → Get raw data
* **Transform** → Clean and process data
* **Load** → Store or output the cleaned data

---

## 📊 Dataset Description

The dataset used in this project is **intentionally dirty** to simulate real-world scenarios.

### Issues in raw data:

* Missing values (nulls)
* Invalid ages (negative values)
* Missing names
* Incorrect data types (amount as string)
* Invalid values (non-numeric amount)

---

## ⚙️ ETL Pipeline Steps

### 🔹 1. Extract

* Created raw dataset using PySpark DataFrame
* Simulates real-world unclean data

---

### 🔹 2. Transform (Data Cleaning)

The following cleaning steps were performed:

* Removed rows with null `customer_id`
* Filled missing names with `"Unknown"`
* Filtered invalid ages (age > 0)
* Removed rows with missing `region`
* Converted `amount` column from string to numeric
* Removed invalid amount values

---

### 🔹 3. Load

* Attempted to save cleaned data to output folder
* (Note: May not work in some online environments)

---

## 📈 Business Logic (Aggregation)

* Calculated **total sales per region** using:

  * `groupBy()`
  * `sum()`

---

## 🧠 Key Learnings

* Real-world data is messy and requires cleaning
* Data validation is important before processing
* Order of operations matters in ETL pipelines
* Learned PySpark DataFrame operations:

  * `dropna()`
  * `fillna()`
  * `filter()`
  * `withColumn()`
  * `groupBy()`
* Understood pipeline thinking:

  * Extract → Clean → Transform → Aggregate

---

## 🔄 Pipeline Flow

Raw Data → Cleaning → Filtering → Transformation → Aggregation → Output

---

## 🛠️ Technologies Used

* Python
* PySpark


## 🏁 Conclusion

This phase shows how to take raw, messy data and transform it into clean, structured data using a step-by-step ETL pipeline in PySpark.
