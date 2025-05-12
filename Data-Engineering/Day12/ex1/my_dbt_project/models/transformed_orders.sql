{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.name AS customer_name,
        o.order_id,
        o.order_date,
        o.total_amount
    FROM {{ source('raw_data', 'customers') }} c
    JOIN {{ source('raw_data', 'orders') }} o ON c.customer_id = o.customer_id
)

SELECT
    customer_id,
    customer_name,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_spent,
    AVG(total_amount) AS avg_order_value
FROM customer_orders
GROUP BY customer_id, customer_name
