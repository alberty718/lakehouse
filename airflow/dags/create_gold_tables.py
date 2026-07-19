from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import trino


def create_tables():

    conn = trino.dbapi.connect(
        host="trino",
        port=8080,
        user="airflow",
        catalog="iceberg",
        schema="gold",
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS iceberg.gold
    """)

    cursor.execute("""
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

    )
    WITH (
        format='PARQUET'
    )
    """)

    conn.close()


with DAG(

    dag_id="create_gold_tables",

    start_date=datetime(2025, 1, 1),

    catchup=False,

    schedule=None

) as dag:

    PythonOperator(

        task_id="create",

        python_callable=create_tables

    )