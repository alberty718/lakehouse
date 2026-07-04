SELECT
    o.order_id,
    c.name,
    c.country,
    o.amount,
    o.order_date
FROM iceberg.lakehouse.orders o
JOIN iceberg.lakehouse.customers c
    ON o.customer_id = c.id
ORDER BY o.order_id;