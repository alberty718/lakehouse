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
        schema="silver",
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS iceberg.silver
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.customers (

        customer_id VARCHAR,
        first_name VARCHAR,
        last_name VARCHAR,
        email VARCHAR,
        city VARCHAR,
        segment VARCHAR,
        registration_date DATE,
        ingestion_batch_date DATE

    )
    WITH (
        format='PARQUET'
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.products (

        product_id VARCHAR,
        name VARCHAR,
        category VARCHAR,
        brand VARCHAR,
        price DOUBLE,
        currency VARCHAR

    )
    WITH (
        format='PARQUET'
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.pos_transactions (

        transaction_id VARCHAR,
        ts TIMESTAMP,
        store_id VARCHAR,
        customer_id VARCHAR,
        total_amount DOUBLE,
        payment_method VARCHAR

    )
    WITH (
        format='PARQUET'
    )
    """)

    conn.close()


with DAG(

    dag_id="create_silver_tables",

    start_date=datetime(2025,1,1),

    catchup=False,

    schedule=None

) as dag:

    PythonOperator(

        task_id="create",

        python_callable=create_tables

    )