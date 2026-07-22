import json
import os
import random
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


def generate_and_upload_products():

    categories = [
        "Электроника",
        "Одежда",
        "Продукты",
        "Дом и сад",
        "Спорт"
    ]

    products = []

    for i in range(200):

        products.append({
            "product_id": f"PROD-{1000+i}",
            "name": fake.catch_phrase(),
            "category": random.choice(categories),
            "brand": fake.company(),
            "price": round(random.uniform(500, 75000), 2),
            "currency": "RUB"
        })

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    data = json.dumps(
        products,
        ensure_ascii=False
    ).encode("utf-8")

    client.put_object(
        BUCKET_NAME,
        "raw/products/products.json",
        BytesIO(data),
        len(data),
        content_type="application/json"
    )

    print(f"Uploaded {len(products)} products")


default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_ingest_products",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    PythonOperator(
        task_id="generate_products",
        python_callable=generate_and_upload_products,
    )
