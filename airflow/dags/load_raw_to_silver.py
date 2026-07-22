from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import re

from etl.minio_loader import MinioIcebergLoader


def load(**context):
    """Refresh Silver from the current CRM/products snapshots and POS batch."""
    consumed_batch = context["ti"].xcom_pull(task_ids="consume_kafka_to_raw")
    if not consumed_batch:
        raise ValueError("No Raw POS batch metadata found in XCom.")

    loader = MinioIcebergLoader()
    try:
        loader.clear_silver_tables()
        loader.load_customers()
        loader.load_products()
        safe_batch_id = re.sub(r"[^A-Za-z0-9_.-]", "_", consumed_batch["batch_id"])
        loader.load_transactions(prefix=f"raw/pos/{safe_batch_id}.json")
    finally:
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
