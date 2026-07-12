import json
import time
import os
import logging
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker('ru_RU')

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'raw_pos_transactions')
INTERVAL = int(os.getenv('PRODUCE_INTERVAL_SEC', '2'))

def generate_transaction():
    items_count = fake.pyint(min_value=1, max_value=5)
    items = []
    total = 0
    
    for _ in range(items_count):
        price = round(fake.pyfloat(min_value=100, max_value=15000), 2)
        qty = fake.pyint(min_value=1, max_value=3)
        items.append({
            "product_id": f"PROD-{fake.pyint(min_value=1000, max_value=9999)}",
            "quantity": qty,
            "unit_price": price
        })
        total += price * qty
    
    return {
        "transaction_id": f"TXN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{fake.pyint(min_value=1000, max_value=9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": f"STORE-{fake.city()[:3].upper()}-{fake.pyint(min_value=1, max_value=20):02d}",
        "customer_id": str(fake.uuid4()),
        "items": items,
        "total_amount": round(total, 2),
        "payment_method": fake.random_element(["card", "cash", "sbp"])
    }

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                request_timeout_ms=10000
            )
            producer.bootstrap_connected()
            logger.info("Successful connect to Kafka")
            return producer
        except (NoBrokersAvailable, KafkaTimeoutError, Exception) as e:
            logger.warning(f"Waiting Kafka... ({type(e).__name__}: {e})")
            time.sleep(5)

def main():
    logger.info(f"Launch POS Producer | Topic: {TOPIC_NAME} | Interval: {INTERVAL}s")
    
    producer = create_producer()
    
    try:
        while True:
            msg = generate_transaction()
            future = producer.send(TOPIC_NAME, value=msg)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Sent: {msg['transaction_id']} | Total: {msg['total_amount']}₽ | Partition: {record_metadata.partition}")
            time.sleep(INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    main()