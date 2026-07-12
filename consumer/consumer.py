import json
import os
import io
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'raw_pos_transactions')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
BUCKET_NAME = os.getenv('MINIO_BUCKET', 'raw-pos')
BATCH_SIZE = int(os.getenv('CONSUMER_BATCH_SIZE', '50'))

def init_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket created: {BUCKET_NAME}")
    return client

def main():
    logger.info(f"Launch POS Consumer | Topic: {TOPIC_NAME} | Batch: {BATCH_SIZE}")
    
    minio_client = init_minio()
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest',
        group_id='pos-to-minio-consumer',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True
    )
    
    buffer = []
    try:
        for message in consumer:
            buffer.append(message.value)
            
            if len(buffer) >= BATCH_SIZE:
                timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
                filename = f"pos_batch_{timestamp}.json"
                
                data_bytes = json.dumps(buffer, ensure_ascii=False).encode('utf-8')
                data_stream = io.BytesIO(data_bytes)
                
                minio_client.put_object(
                    BUCKET_NAME,
                    filename,
                    data=data_stream,
                    length=len(data_bytes),
                    content_type='application/json'
                )
                
                logger.info(f"Batch saved: {filename} ({len(buffer)} записей)")
                buffer.clear()
                
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()