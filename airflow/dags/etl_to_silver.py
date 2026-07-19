# airflow/dags/etl_to_silver.py
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_raw_to_silver_iceberg',
    default_args=default_args,
    description='ETL: Перенос данных из Raw JSON в Silver Iceberg через Trino',
    schedule_interval='@hourly',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['silver', 'iceberg', 'trino'],
) as dag:

    # --- 1. Таблица Клиентов (CRM) ---
    create_customers = TrinoOperator(
        task_id='create_silver_customers',
        sql="""
            CREATE TABLE IF NOT EXISTS iceberg.silver.customers (
                customer_id VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                city VARCHAR,
                segment VARCHAR,
                registration_date DATE
            ) WITH (format = 'PARQUET')
        """,
        trino_conn_id='trino_default'
    )

    insert_customers = TrinoOperator(
        task_id='insert_silver_customers',
        sql="""
            INSERT INTO iceberg.silver.customers
            SELECT 
                CAST(json_extract_scalar(json_data, '$.customer_id') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.first_name') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.last_name') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.email') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.city') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.segment') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.registration_date') AS DATE)
            FROM minio.raw.crm 
            WHERE _file LIKE '%crm_%.json'
        """,
        trino_conn_id='trino_default'
    )

    # --- 2. Таблица Товаров (Products) ---
    create_products = TrinoOperator(
        task_id='create_silver_products',
        sql="""
            CREATE TABLE IF NOT EXISTS iceberg.silver.products (
                product_id VARCHAR,
                name VARCHAR,
                category VARCHAR,
                brand VARCHAR,
                price DOUBLE
            ) WITH (format = 'PARQUET')
        """,
        trino_conn_id='trino_default'
    )

    insert_products = TrinoOperator(
        task_id='insert_silver_products',
        sql="""
            INSERT INTO iceberg.silver.products
            SELECT 
                CAST(json_extract_scalar(json_data, '$.product_id') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.name') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.category') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.brand') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.price') AS DOUBLE)
            FROM minio.raw.products
            WHERE _file LIKE '%products.json'
        """,
        trino_conn_id='trino_default'
    )

    # --- 3. Таблица Транзакций (POS) ---
    # Тут сложнее, так как items - это массив. Для MVP упростим: возьмем общую сумму.
    create_pos = TrinoOperator(
        task_id='create_silver_pos',
        sql="""
            CREATE TABLE IF NOT EXISTS iceberg.silver.pos_transactions (
                transaction_id VARCHAR,
                timestamp TIMESTAMP,
                store_id VARCHAR,
                customer_id VARCHAR,
                total_amount DOUBLE,
                payment_method VARCHAR
            ) WITH (format = 'PARQUET', partitioning = ARRAY['store_id'])
        """,
        trino_conn_id='trino_default'
    )

    insert_pos = TrinoOperator(
        task_id='insert_silver_pos',
        sql="""
            INSERT INTO iceberg.silver.pos_transactions
            SELECT 
                CAST(json_extract_scalar(json_data, '$.transaction_id') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.timestamp') AS TIMESTAMP),
                CAST(json_extract_scalar(json_data, '$.store_id') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.customer_id') AS VARCHAR),
                CAST(json_extract_scalar(json_data, '$.total_amount') AS DOUBLE),
                CAST(json_extract_scalar(json_data, '$.payment_method') AS VARCHAR)
            FROM minio.raw.pos
            WHERE _file LIKE '%pos_%.json'
        """,
        trino_conn_id='trino_default'
    )

    # Порядок выполнения
    [create_customers, create_products, create_pos] >> [insert_customers, insert_products, insert_pos]