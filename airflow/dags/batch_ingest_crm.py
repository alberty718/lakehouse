# dags/batch_ingest_crm.py
import json
import os
from datetime import datetime, timedelta
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
from minio import Minio

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "lakehouse"

fake = Faker("ru_RU")


def generate_and_upload_crm(**kwargs):
    execution_date = kwargs["ds"]
    
    customers = []
    for _ in range(1000):
        customers.append({
            "customer_id": str(fake.uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "segment": fake.random_element(["premium", "standard", "basic"]),
            "registration_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "ingestion_batch_date": execution_date
        })

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    data_bytes = json.dumps(customers, ensure_ascii=False).encode("utf-8")
    object_name = "raw/crm/customers.json"
    
    client.put_object(
        BUCKET_NAME,
        object_name,
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json"
    )
    
    print(f"Uploaded {len(customers)} customers to s3://{BUCKET_NAME}/{object_name}")


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_ingest_crm",
    default_args=default_args,
    description="Ежечасная загрузка CRM данных в Raw/Bronze слой (ФТ-3)",
    schedule_interval="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["retail", "batch", "crm", "bronze"],
) as dag:

    ingest_task = PythonOperator(
        task_id="generate_and_upload_crm",
        python_callable=generate_and_upload_crm,
        provide_context=True
    )