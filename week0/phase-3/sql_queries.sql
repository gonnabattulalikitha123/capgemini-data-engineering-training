-- 1. Daily Sales: Calculate total sales per day
SELECT 
    date,
    SUM(amount) AS daily_sales
FROM sales
GROUP BY date
ORDER BY date;


-- 2. City-wise Revenue: Revenue per city
SELECT 
    c.city,
    SUM(o.order_amount) AS city_revenue
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.city
ORDER BY city_revenue DESC;


-- 3. Repeat Customers: Customers with more than 2 orders
SELECT 
    customer_id,
    COUNT(order_id) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(order_id) > 2
ORDER BY order_count DESC;


-- 4. Highest Spending Customer per City
WITH customer_total AS (
    SELECT 
        c.city,
        o.customer_id,
        SUM(o.order_amount) AS total_spend
    FROM customers c
    JOIN orders o
    ON c.customer_id = o.customer_id
    GROUP BY c.city, o.customer_id
)
SELECT 
    city,
    customer_id,
    total_spend
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_spend DESC) AS rn
    FROM customer_total
) t
WHERE rn = 1;


-- 5. Final Reporting Table: Customer, city, total spend, order count
SELECT 
    c.customer_id,
    c.city,
    SUM(o.order_amount) AS total_spend,
    COUNT(o.order_id) AS order_count
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.city
ORDER BY total_spend DESC;
