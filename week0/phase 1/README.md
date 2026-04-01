## Phase 1 – SQL to PySpark Simple Foundation

 Objective
In this phase, I focused on understanding the basics of PySpark and how it is similar to SQL. The main goal was to learn how to perform simple operations like displaying data, filtering rows, selecting columns, and doing basic aggregation using PySpark DataFrames.
Problem Statement 
• Create a sample customers dataset using SQL and PySpark
• Display all the records from the dataset
• Filter customers based on conditions like city and age
• Select only required columns from the dataset
• Perform grouping to count customers city-wise
• Understand how SQL queries can be converted into PySpark code

 Dataset Used
• Dataset: Sample Customers Data
• Source: Self-created dataset for practice
• Tables used: customers

The dataset contains basic customer details such as customer_id, customer_name, city, and age.

Approach

1. First, I created a customers table using SQL with columns like id, name, city, and age.
2. Then I inserted some sample records into the table.
3. After that, I created the same dataset in PySpark using createDataFrame().
4. I used show() method to display all the data in the DataFrame.
5. I applied filter() to get specific data such as customers from Chennai and customers with age greater than 25.
6. I used select() to display only selected columns like customer_name and city.
7. I used groupBy() and count() to find how many customers are there in each city.
8. I compared each SQL query with the equivalent PySpark code to understand the difference and similarity.
   Key Learnings
• I learned how to create and work with DataFrames in PySpark
• I understood how show() is used to display data
• I learned how to filter data using filter()
• I understood how to select specific columns using select()
• I learned how to group data using groupBy() and perform aggregation
• I understood that PySpark DataFrame works similar to SQL tables
• I gained confidence in converting simple SQL queries into PySpark code
• I got basic understanding of how data processing works in PySpark
 Conclusion
In this phase, I built a strong foundation in PySpark basics. I understood how to perform simple data operations and how PySpark is similar to SQL. This phase helped me gain confidence to move to more advanced topics like joins and complex transformations in the next phases.

