INSERT INTO iceberg.lakehouse.customers VALUES
(1, 'Alice', 'DE'),
(2, 'Bob', 'NL'),
(3, 'Carlos', 'ES');

INSERT INTO iceberg.lakehouse.orders VALUES
(101, 1, 250.50, DATE '2026-06-01'),
(102, 2, 89.90, DATE '2026-06-03'),
(103, 1, 430.00, DATE '2026-06-10'),
(104, 3, 12.75, DATE '2026-06-15');

INSERT INTO iceberg.lakehouse.customers VALUES
(4, 'Diana', 'FR');