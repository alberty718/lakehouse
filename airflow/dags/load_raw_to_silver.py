from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from etl.minio_loader import MinioIcebergLoader


def load():

    loader = MinioIcebergLoader()

    loader.load_customers()
    loader.load_products()
    loader.load_transactions()

    loader.close()


with DAG(
    dag_id="load_raw_to_silver",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    PythonOperator(
        task_id="load_raw",
        python_callable=load
    )