import json
import os
import re
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
    """Consume exactly the POS batch emitted by the upstream producer task."""
    produced_batch = kwargs["ti"].xcom_pull(task_ids="produce_pos_events")
    if not produced_batch:
        raise ValueError("No producer metadata found in XCom.")

    batch_id = produced_batch["batch_id"]
    expected_count = produced_batch["event_count"]
    safe_batch_id = re.sub(r"[^A-Za-z0-9_.-]", "_", batch_id)
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id=f'airflow-pos-consumer-{safe_batch_id}',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000,
    )
    
    messages_batch = []
    start_time = datetime.now()
    
    try:
        for message in consumer:
            if message.value.get("batch_id") != batch_id:
                continue
            messages_batch.append(message.value)
            if len(messages_batch) == expected_count:
                break
            if (datetime.now() - start_time).seconds > 120:
                break
    finally:
        consumer.close()

    if len(messages_batch) != expected_count:
        raise ValueError(
            f"Expected {expected_count} POS events for batch {batch_id}, "
            f"but received {len(messages_batch)}."
        )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    object_name = f"{RAW_PREFIX}/{safe_batch_id}.json"
    
    data_bytes = json.dumps(messages_batch, ensure_ascii=False).encode('utf-8')
    
    client.put_object(
        BUCKET_NAME,
        object_name,
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type='application/json'
    )
    
    return {"batch_id": batch_id, "object_name": object_name, "records": len(messages_batch)}

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
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['retail', 'kafka', 'bronze', 'ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='consume_kafka_and_save_to_minio',
        python_callable=consume_and_upload_to_minio,
        provide_context=True
    )
