Assignment Description

This assignment focuses on building a structured data processing pipeline using PySpark for analyzing car sales data. The objective is to understand how raw data can be cleaned, validated, transformed, and analyzed to generate meaningful insights. The assignment emphasizes data quality checks, correct handling of inconsistencies, and performing aggregations to ensure accurate results.

The workflow follows a step-by-step approach, starting from identifying issues in the dataset, applying necessary cleaning operations, validating the processed data, and finally performing analytical computations. The output is then stored for further use and reporting purposes.

📌 Steps Performed
🔹 1. Data Cleaning

In this step, the dataset was examined for inconsistencies and errors. Missing values in the name column of the customers dataset were identified and handled appropriately by replacing them with a default value. String fields were cleaned using trimming functions to remove unnecessary spaces and ensure uniform formatting.

Negative values present in the price column of the cars dataset were identified and removed, as they are not logically valid and could affect revenue calculations. Additionally, invalid foreign key references were detected in the sales dataset, where certain customer_id or car_id values did not match with the respective master tables. These invalid records were removed to maintain data integrity.

🔹 2. Validation

After cleaning the data, validation checks were performed to ensure correctness. The total number of records before and after cleaning was compared to understand the impact of cleaning operations. Counts of invalid records such as null values, negative prices, and foreign key mismatches were calculated.

This step ensures that only incorrect data is removed and that the remaining dataset is consistent and reliable. Validation helps in verifying that the cleaning process has been applied correctly without affecting valid records.

🔹 3. Transformation

In the transformation phase, new derived columns were created to enable further analysis. A revenue column was calculated using the formula:

Revenue = Price × Quantity

Using this derived column, multiple aggregations were performed. Customer-wise revenue was calculated to understand the contribution of each customer. Brand-wise aggregation was performed to identify which car brands generate higher sales. City-wise revenue analysis was also carried out to understand geographical trends in the data.

🔹 4. Analytics

Further analysis was performed using both PySpark and SQL queries. Customers with multiple purchases were identified as repeat customers. Monthly sales trends were analyzed to observe how revenue changes over time.

Additionally, ranking functions were used to identify the top three customers in each city based on revenue. These analytical queries help in extracting deeper insights from the dataset and demonstrate the use of advanced data processing techniques.

🔹 5. Output

Finally, the processed results were saved as output files or tables. These outputs include customer revenue, brand-wise sales, and city-wise revenue. Saving the results ensures that the processed data can be reused for reporting or further analysis.
