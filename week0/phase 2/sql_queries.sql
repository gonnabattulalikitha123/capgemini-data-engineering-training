
CREATE TABLE customers (
    customer_id INT,
    name VARCHAR(50),
    city VARCHAR(50)
);

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_amount INT
);


INSERT INTO customers VALUES
(1, 'Alice', 'New York'),
(2, 'Bob', 'Los Angeles'),
(3, 'Charlie', 'Chicago'),
(4, 'David', 'New York');

INSERT INTO orders VALUES
(101, 1, 100),
(102, 1, 200),
(103, 2, 150),
(104, 3, 300);

SELECT customer_id, SUM(order_amount) AS total_spend
FROM orders
GROUP BY customer_id;

SELECT customer_id, SUM(order_amount) AS total_spend
FROM orders
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 3;


SELECT c.customer_id
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;


SELECT c.city, SUM(o.order_amount) AS total_revenue
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.city;

SELECT customer_id, AVG(order_amount) AS avg_order
FROM orders
GROUP BY customer_id;

SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT customer_id, SUM(order_amount) AS total_spend
FROM orders
GROUP BY customer_id
ORDER BY total_spend DESC;
