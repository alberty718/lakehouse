"""Idempotent Silver-to-Gold mart build for the Retail Lakehouse."""

import logging
import os
from datetime import datetime, timedelta

import trino
from airflow import DAG
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)


def get_connection():
    """Return a Trino connection configured for the Iceberg catalog."""
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "airflow"),
        catalog="iceberg",
        schema="gold",
    )


def create_gold_tables():
    """Create the Gold schema and its marts when they do not yet exist."""
    statements = [
        "CREATE SCHEMA IF NOT EXISTS iceberg.gold",
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.fact_sales (
            transaction_id VARCHAR,
            ts TIMESTAMP,
            store_id VARCHAR,
            customer_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            city VARCHAR,
            segment VARCHAR,
            product_id VARCHAR,
            product_name VARCHAR,
            category VARCHAR,
            brand VARCHAR,
            quantity INTEGER,
            unit_price DOUBLE,
            line_amount DOUBLE,
            payment_method VARCHAR
        ) WITH (format = 'PARQUET')
        """,
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.sales_daily (
            sale_date DATE,
            transactions_count BIGINT,
            items_sold BIGINT,
            revenue DOUBLE,
            avg_check DOUBLE
        ) WITH (format = 'PARQUET')
        """,
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.product_sales (
            product_id VARCHAR,
            product_name VARCHAR,
            category VARCHAR,
            brand VARCHAR,
            units_sold BIGINT,
            revenue DOUBLE
        ) WITH (format = 'PARQUET')
        """,
    ]
    with get_connection() as connection:
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)


def rebuild_fact_sales():
    """Rebuild the line-level sales fact from the current Silver snapshot."""
    with get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM iceberg.gold.fact_sales")
        cursor.execute(
            """
            INSERT INTO iceberg.gold.fact_sales (
                transaction_id, ts, store_id, customer_id, first_name, last_name,
                city, segment, product_id, product_name, category, brand,
                quantity, unit_price, line_amount, payment_method
            )
            SELECT
                t.transaction_id,
                t.ts,
                t.store_id,
                c.customer_id,
                c.first_name,
                c.last_name,
                c.city,
                c.segment,
                p.product_id,
                p.name AS product_name,
                p.category,
                p.brand,
                ti.quantity,
                ti.unit_price,
                CAST(ti.quantity * ti.unit_price AS DOUBLE) AS line_amount,
                t.payment_method
            FROM iceberg.silver.pos_transactions AS t
            INNER JOIN iceberg.silver.customers AS c
                ON t.customer_id = c.customer_id
            INNER JOIN iceberg.silver.transaction_items AS ti
                ON t.transaction_id = ti.transaction_id
            INNER JOIN iceberg.silver.products AS p
                ON ti.product_id = p.product_id
            """
        )


def build_aggregates():
    """Build daily and product-level marts from fact_sales."""
    with get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM iceberg.gold.sales_daily")
        cursor.execute(
            """
            INSERT INTO iceberg.gold.sales_daily (
                sale_date, transactions_count, items_sold, revenue, avg_check
            )
            SELECT
                CAST(ts AS DATE) AS sale_date,
                COUNT(DISTINCT transaction_id) AS transactions_count,
                CAST(SUM(quantity) AS BIGINT) AS items_sold,
                SUM(line_amount) AS revenue,
                SUM(line_amount) / COUNT(DISTINCT transaction_id) AS avg_check
            FROM iceberg.gold.fact_sales
            GROUP BY CAST(ts AS DATE)
            """
        )
        cursor.execute("DELETE FROM iceberg.gold.product_sales")
        cursor.execute(
            """
            INSERT INTO iceberg.gold.product_sales (
                product_id, product_name, category, brand, units_sold, revenue
            )
            SELECT
                product_id,
                product_name,
                category,
                brand,
                CAST(SUM(quantity) AS BIGINT) AS units_sold,
                SUM(line_amount) AS revenue
            FROM iceberg.gold.fact_sales
            GROUP BY product_id, product_name, category, brand
            """
        )


def validate_gold_layer():
    """Fail the DAG early when source data exists but joins yield no Gold facts."""
    checks = {
        "silver_transactions": "SELECT COUNT(*) FROM iceberg.silver.pos_transactions",
        "silver_items": "SELECT COUNT(*) FROM iceberg.silver.transaction_items",
        "fact_sales": "SELECT COUNT(*) FROM iceberg.gold.fact_sales",
        "sales_daily": "SELECT COUNT(*) FROM iceberg.gold.sales_daily",
        "product_sales": "SELECT COUNT(*) FROM iceberg.gold.product_sales",
    }
    results = {}
    with get_connection() as connection:
        cursor = connection.cursor()
        for name, query in checks.items():
            cursor.execute(query)
            results[name] = cursor.fetchone()[0]

    LOGGER.info("Gold layer validation counts: %s", results)
    if results["silver_transactions"] and not results["fact_sales"]:
        raise ValueError(
            "Silver contains transactions, but fact_sales is empty. "
            "Check customer_id/product_id consistency before rebuilding Gold."
        )
    if results["fact_sales"] and (
        not results["sales_daily"] or not results["product_sales"]
    ):
        raise ValueError("fact_sales has data, but one or more aggregate marts are empty.")


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_silver_to_gold",
    description="Idempotent build of Iceberg Gold marts from Silver tables",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["retail", "iceberg", "gold"],
) as dag:
    ensure_tables = PythonOperator(
        task_id="create_gold_tables",
        python_callable=create_gold_tables,
    )
    load_fact_sales = PythonOperator(
        task_id="rebuild_fact_sales",
        python_callable=rebuild_fact_sales,
    )
    load_aggregates = PythonOperator(
        task_id="build_aggregates",
        python_callable=build_aggregates,
    )
    validate = PythonOperator(
        task_id="validate_gold_layer",
        python_callable=validate_gold_layer,
    )

    ensure_tables >> load_fact_sales >> load_aggregates >> validate
