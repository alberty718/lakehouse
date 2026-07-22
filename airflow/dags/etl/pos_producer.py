"""Finite Kafka producer used by the orchestrated retail pipeline."""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from minio import Minio

LOGGER = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw_pos_transactions")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "lakehouse"


def _load_reference_ids():
    """Load the reference snapshots created by upstream tasks in this DAG run."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False,
    )
    objects = {
        "customers": "raw/crm/customers.json",
        "products": "raw/products/products.json",
    }
    result = {}
    for name, object_name in objects.items():
        response = client.get_object(BUCKET_NAME, object_name)
        try:
            result[name] = json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

    customer_ids = [row["customer_id"] for row in result["customers"]]
    product_ids = [row["product_id"] for row in result["products"]]
    if not customer_ids or not product_ids:
        raise ValueError("CRM or product reference snapshot is empty.")
    return customer_ids, product_ids


def _make_event(customer_ids, product_ids, batch_id, fake):
    items = []
    total_amount = 0.0
    for _ in range(random.randint(1, 5)):
        quantity = random.randint(1, 3)
        unit_price = round(random.uniform(100, 15000), 2)
        items.append(
            {
                "product_id": random.choice(product_ids),
                "quantity": quantity,
                "unit_price": unit_price,
            }
        )
        total_amount += quantity * unit_price

    return {
        "batch_id": batch_id,
        "transaction_id": f"TXN-{uuid.uuid4()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": f"STORE-{fake.city()[:3].upper()}-{random.randint(1, 20):02d}",
        "customer_id": random.choice(customer_ids),
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": random.choice(["card", "cash", "sbp"]),
    }


def produce_pos_events(**context):
    """Publish a finite, traceable POS batch and return its metadata through XCom."""
    event_count = int(os.getenv("POS_EVENTS_PER_RUN", "100"))
    batch_id = context["run_id"]
    customer_ids, product_ids = _load_reference_ids()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=1000,
        request_timeout_ms=10000,
    )
    try:
        fake = Faker("ru_RU")
        futures = []
        for _ in range(event_count):
            futures.append(producer.send(TOPIC_NAME, _make_event(customer_ids, product_ids, batch_id, fake)))
        for future in futures:
            future.get(timeout=30)
        producer.flush(timeout=30)
    finally:
        producer.close()

    LOGGER.info("Published %s POS events for batch %s", event_count, batch_id)
    return {"batch_id": batch_id, "event_count": event_count}
