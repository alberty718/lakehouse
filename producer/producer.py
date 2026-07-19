import json
import time
import os
import logging
import random
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker("ru_RU")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "raw_pos_transactions")
INTERVAL = int(os.getenv("PRODUCE_INTERVAL_SEC", "2"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "lakehouse"


def load_reference_data():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False,
    )

    crm_object = client.get_object(
        BUCKET_NAME,
        "raw/crm/customers.json"
    )
    customers = json.loads(crm_object.read().decode("utf-8"))

    products_object = client.get_object(
        BUCKET_NAME,
        "raw/products/products.json",
    )
    products = json.loads(products_object.read().decode("utf-8"))

    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]

    logger.info(f"Loaded {len(customer_ids)} customers")
    logger.info(f"Loaded {len(product_ids)} products")
    logger.info(f"First 5 customer_ids: {customer_ids[:5]}")
    logger.info(f"First 5 product_ids: {product_ids[:5]}")

    return customer_ids, product_ids


def generate_transaction(customer_ids, product_ids):
    items_count = fake.pyint(min_value=1, max_value=5)

    items = []
    total = 0

    for _ in range(items_count):
        price = round(fake.pyfloat(min_value=100, max_value=15000), 2)
        qty = fake.pyint(min_value=1, max_value=3)

        items.append({
            "product_id": random.choice(product_ids),
            "quantity": qty,
            "unit_price": price,
        })

        total += price * qty

    customer_id = random.choice(customer_ids)

    logger.info(f"Generated customer_id: {customer_id}")

    return {
        "transaction_id": f"TXN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{fake.pyint(min_value=1000, max_value=9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": f"STORE-{fake.city()[:3].upper()}-{fake.pyint(min_value=1, max_value=20):02d}",
        "customer_id": customer_id,
        "items": items,
        "total_amount": round(total, 2),
        "payment_method": fake.random_element(["card", "cash", "sbp"]),
    }


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(
                    v,
                    ensure_ascii=False,
                ).encode("utf-8"),
                acks="all",
                retries=3,
                retry_backoff_ms=1000,
                request_timeout_ms=10000,
            )

            producer.bootstrap_connected()

            logger.info("Connected to Kafka")

            return producer

        except (NoBrokersAvailable, KafkaTimeoutError, Exception) as e:
            logger.warning(f"Waiting for Kafka... ({type(e).__name__}: {e})")
            time.sleep(5)


def main():
    logger.info(
        f"Launch POS Producer | Topic: {TOPIC_NAME} | Interval: {INTERVAL}s"
    )

    while True:
        try:
            customer_ids, product_ids = load_reference_data()
            break
        except Exception as e:
            logger.info(f"Waiting for reference data... ({e})")
            time.sleep(5)

    producer = create_producer()

    try:
        while True:
            msg = generate_transaction(customer_ids, product_ids)

            future = producer.send(
                TOPIC_NAME,
                value=msg,
            )

            record_metadata = future.get(timeout=10)

            logger.info(
                f"Sent: {msg['transaction_id']} | "
                f"Customer: {msg['customer_id']} | "
                f"Items: {len(msg['items'])} | "
                f"Total: {msg['total_amount']}₽ | "
                f"Partition: {record_metadata.partition}"
            )

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)

    finally:
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    main()