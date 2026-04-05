# Phase 4A: Bucketing & Segmentation using PySpark

## 🎯 Objective

The goal of this exercise is to understand how continuous data (like total spend) can be converted into categories such as Gold, Silver, and Bronze.

This process is called **segmentation or bucketing**, and it is widely used in business analytics.


## 📊 Dataset

Sample data is created using PySpark:

* name → customer name
* total_spend → amount spent by customer

This simulates real-world customer spending data.

---

## 🧠 Core Concept

Segmentation helps in:

* Simplifying analysis
* Grouping customers based on behavior
* Supporting business decisions

Example:

* High spenders → Gold
* Medium spenders → Silver
* Low spenders → Bronze

---

## ⚙️ Methods Implemented

### 1. Conditional Logic (Most Common)

* Used `when()` function
* Based on fixed business rules

Example:

* > 10000 → Gold
* 5000–10000 → Silver
* <5000 → Bronze

---

### 2. Grouping by Segment

* Counted number of customers in each segment
* Helps in understanding distribution

---

### 3. Quantile-based Segmentation

* Used `approxQuantile()`
* Divides data into equal-sized groups
* Dynamic segmentation based on data distribution

---

### 4. Bucketizer (MLlib)

* Used PySpark ML feature `Bucketizer`
* Converts values into numeric buckets

---

### 5. Window-based Ranking

* Used `percent_rank()`
* Assigns rank based on spending
* Useful for advanced analytics

---

## 🔄 Workflow

1. Create sample data
2. Apply segmentation using different methods
3. Group and analyze segments
4. Compare results

---

## 📈 Key Learnings

* Learned multiple ways to perform segmentation
* Understood difference between fixed and dynamic grouping
* Practiced PySpark transformations and functions
* Learned how segmentation supports business decisions

---

##  Reflection

### Why convert continuous values into categories?

To simplify analysis and make decision-making easier.

---

### Difference between business segmentation and technical bucketing?

* Business segmentation → based on business rules (Gold, Silver, Bronze)
* Technical bucketing → numeric grouping for processing

---

### When do fixed thresholds fail?

When data distribution changes over time.

---

### How is quantile segmentation different?

* Fixed rules → same thresholds
* Quantile → adjusts based on data

---

### Which method is most useful?

* Conditional logic → best for business use
* Quantile → best for dynamic datasets

---

## 🚀 How to Run

```bash
pip install pyspark
python segmentation_practice.py
```

---

## 🏁 Conclusion

This exercise demonstrates how to convert raw numerical data into meaningful categories using different segmentation techniques in PySpark.
