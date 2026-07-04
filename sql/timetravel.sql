SELECT * FROM iceberg.lakehouse.customers;

SELECT * FROM iceberg.lakehouse."customers$snapshots";

SELECT * FROM iceberg.lakehouse.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-07-04 17:10:00.000 UTC';