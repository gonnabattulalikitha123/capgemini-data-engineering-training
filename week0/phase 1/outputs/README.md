Phase 2 – Customer Orders Analysis
🔹 Objective

This phase is about analyzing customer and order data to generate meaningful insights. The goal is to practice data cleaning, joining tables, calculating key metrics, and creating reports using both SQL and PySpark.

🔹 Problem Statement 

The main tasks for this phase include:

Clean the datasets by removing missing or invalid customer_id values
Join customers and orders tables to combine data
Calculate important metrics like total spend, average order amount, and order counts
Apply transformations such as aggregation, filtering, and sorting
Identify special cases like customers with no orders or multiple orders
Generate final reporting outputs for analysis
🔹 Dataset Used
Dataset: Sample Customer Orders Dataset
Source: Created manually for practice purposes
Tables used: customers, orders
🔹 Approach

Step-by-step process followed:

Loaded the customers and orders datasets into PySpark DataFrames
Cleaned the data by removing rows with missing customer_id
Registered DataFrames as temporary SQL tables to run SQL queries
Calculated metrics such as total spend, average order, and number of orders per customer
Joined the customers and orders tables to combine relevant information
Applied aggregations, filtering, and sorting to prepare reports
Compared SQL query results with PySpark transformations to make sure they match
🔹 Key Transformations
groupBy with sum(), avg(), count() for aggregations
Joins: inner join and left join to combine tables
Filtering data using filter() in PySpark or WHERE in SQL
Sorting results using orderBy() or ORDER BY
Conditional logic using when / otherwise in PySpark
🔹 Output / Results
Total order amount for each customer
Top 3 customers by total spend
Customers who have not placed any orders
City-wise total revenue
Average order amount per customer
Customers with more than one order
Final table sorted by total spend

🔹 Challenges Faced
Handling missing values in the customer_id column
Understanding how to join tables properly
Translating SQL queries into PySpark code accurately
🔹 Learnings
Learned the importance of cleaning and validating data before analysis
Understood how to join multiple tables and check referential integrity
Learned to use aggregations and window functions for reporting
Practiced comparing SQL and PySpark outputs
Built a small end-to-end data analysis workflow
