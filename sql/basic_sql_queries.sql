use database PRACTICE;
use schema PUBLIC;

--Create Customers Table
Create table Customers(
Customer_id integer,
name varchar
);

--Inserting Values in Customers Table 
insert into Customers(Customer_id, name)
values
 (1,'Asha')  
,(2,'Rahul')  
,(3,'Priya');

--Truncate table (if need be)
truncate table customers;

--Create Orders Table
Create or replace table Orders(
order_id integer,
customer_id integer,
amount integer,
order_date datetime
);

--Inserting Values in CustomOrderser Table 
insert into Orders(order_id, Customer_id, amount, order_date)
values
 (10, 1, 200, '2024-01-01')  
,(11, 1, 350, '2024-02-01')  
,(12, 3, 100, '2024-01-15');

--Truncate table (if need be)
truncate table orders;
--JOINS
--Inner
select c.name as "customer name", o.order_id, o.amount
from customers as c
inner join orders as o
on c.customer_id = o.customer_id;

--Left
select c.name as "customer name", o.order_id, o.amount
from customers as c
left join orders as o
on c.customer_id = o.customer_id;

--Full
select c.name as "customer name", o.order_id, o.amount
from customers as c
full join orders as o
on c.customer_id = o.customer_id;

--Window Functions
--ROW_NUMBER()
SELECT
   customer_id,
   order_id,
   amount,
   order_date,
   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
FROM orders;

--LAG()
SELECT
   customer_id,
   order_id,
   amount,
   order_date,
   LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_amount
FROM orders
Order by customer_id;

--GROUP_BY
SELECT customer_id, COUNT(*) AS total_orders
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;

--CASE statement
SELECT
    order_id,
    amount,
    CASE
        WHEN amount >= 300 THEN 'HIGH'
        WHEN amount >= 150 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS order_size
FROM orders;

--Coomon Table Expressions (CTE)
WITH categorized_orders AS(
select order_id,customer_id, amount,
case
    WHEN amount >= 300 THEN 'HIGH'
    WHEN amount >= 150 THEN 'MEDIUM'
    ELSE'LOW'
END AS order_size
FROM orders
)
select order_size, count(*) as "total_orders"
from categorized_orders
group by order_size;

--View data
select * from customers;
select * from orders;

--Customer who has not placed an order
select c.name
from customers as c
left join orders o
on c.customer_id = o.customer_id
where o.order_id is null;

--Customer and their first order amounts
with r_orders as (
select row_number() OVER (PARTITION BY customer_id ORDER BY order_date) as "r_n", customer_id, amount
from orders )
select customer_id, amount as first_order_amount from r_orders 
where "r_n" = 1

--Customer and their max order amounts
select c.name, max(o.amount) as max_order_amount
from customers as c
left join orders o
on c.customer_id = o.customer_id
group by c.name;

--Customer and their total order amounts
select c.name, sum(o.amount) as total_spent
from customers as c
left join orders o
on c.customer_id = o.customer_id
group by c.customer_id, c.name
having sum(o.amount) > 500;

--Customer and their max order amounts (improved query)
SELECT 
    c.customer_id, 
    MAX(o.amount) AS max_order_amount
FROM customers AS c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;

--Customers Whose First Order Was Also Their Highest Order (Method 1 with 2 CTEs)
WITH first_order AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS r_n,
        customer_id,
        amount AS first_amount
    FROM orders
),
max_order AS (
    SELECT 
        customer_id,
        MAX(amount) AS max_amount
    FROM orders
    GROUP BY customer_id
)
SELECT 
    c.name
FROM first_order f
JOIN max_order m
    ON f.customer_id = m.customer_id
JOIN customers c
    ON c.customer_id = f.customer_id
WHERE 
    f.r_n = 1              -- keep only first order
    AND f.first_amount = m.max_amount;   -- first = max

--Customers Whose First Order Was Also Their Highest Order(Method 2 more compact)
WITH orders_ranked AS (
    SELECT
        o.customer_id,
        c.name,
        o.amount,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id 
            ORDER BY o.order_date
        ) AS rn,
        MAX(o.amount) OVER (
            PARTITION BY o.customer_id
        ) AS max_amount
    FROM orders o
    JOIN customers c
        ON c.customer_id = o.customer_id
)
SELECT name
FROM orders_ranked
WHERE rn = 1
  AND amount = max_amount;







