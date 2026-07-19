import json
import os
from datetime import datetime
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_USER = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_PASS = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
BUCKET_NAME = 'lakehouse'
RAW_PREFIX = 'raw/products'

def upload_products_to_minio(**kwargs):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    file_path = '/opt/airflow/scripts/output/products.json' 
    
    try:
        with open(file_path, 'rb') as f:
            data = f.read()
            
        object_name = f"{RAW_PREFIX}/products.json"
        client.put_object(
            BUCKET_NAME, 
            object_name, 
            BytesIO(data), 
            length=len(data),
            content_type='application/json'
        )
        print(f"Файл products.json успешно загружен в s3://{BUCKET_NAME}/{object_name}")
        
    except FileNotFoundError:
        print(f"Файл не найден по пути: {file_path}. Убедитесь, что вы запустили генератор.")

with DAG(
    dag_id='ingest_products_catalog',
    description='Загрузка справочника товаров в Raw слой',
    start_date=datetime(2026, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['retail', 'static', 'bronze'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_products_json',
        python_callable=upload_products_to_minio,
        provide_context=True
    )