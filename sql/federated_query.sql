SELECT
    c.name,
    c.country,
    r.region
FROM iceberg.lakehouse.customers c
JOIN postgresql.public.country_region r
    ON c.country = r.country;
