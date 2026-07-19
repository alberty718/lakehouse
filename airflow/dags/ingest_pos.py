import json
import os
from datetime import datetime, timedelta
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from minio import Minio

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'raw_pos_transactions')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_USER = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_PASS = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
BUCKET_NAME = 'lakehouse'
RAW_PREFIX = 'raw/pos'

def consume_and_upload_to_minio(**kwargs):
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        group_id='airflow-pos-consumer',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    messages_batch = []
    start_time = datetime.now()
    
    for message in consumer:
        print(message.value["customer_id"])
        messages_batch.append(message.value)

        if len(messages_batch) >= 100 or (datetime.now() - start_time).seconds > 50:
            break
            
    consumer.close()
    
    if not messages_batch:
        return "No new data"

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    execution_date = kwargs['ds']
    timestamp = datetime.now().strftime('%H%M%S')
    object_name = f"{RAW_PREFIX}/pos_{execution_date}_{timestamp}.json"
    
    data_bytes = json.dumps(messages_batch, ensure_ascii=False).encode('utf-8')
    
    client.put_object(
        BUCKET_NAME,
        object_name,
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type='application/json'
    )
    
    return f"Uploaded {len(messages_batch)} records"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ingest_pos_from_kafka',
    default_args=default_args,
    description='Загрузка POS-транзакций из Kafka в Raw слой MinIO',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['retail', 'kafka', 'bronze', 'ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='consume_kafka_and_save_to_minio',
        python_callable=consume_and_upload_to_minio,
        provide_context=True
    )