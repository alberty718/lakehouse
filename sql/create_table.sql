CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse
WITH (location = 's3://lakehouse/warehouse/lakehouse');

CREATE TABLE IF NOT EXISTS iceberg.lakehouse.customers (
    id INTEGER,
    name VARCHAR,
    country VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.lakehouse.orders (
    order_id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    order_date DATE
)
WITH (format = 'PARQUET');