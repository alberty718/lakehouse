"""Finite Kafka producer used by the orchestrated retail pipeline."""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

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

# Сколько дней истории генерировать за один запуск DAG (для холодного старта форкаста)
BACKFILL_DAYS = int(os.getenv("POS_BACKFILL_DAYS", "14"))


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


def _make_event(customer_ids, product_ids, batch_id, fake, event_ts):
    items = []
    total_amount = 0.0
    for _ in range(random.randint(1, 5)):
        quantity = random.randint(1, 3)
        unit_price = round(random.lognormvariate(6.5, 0.8), 2)
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
        "timestamp": event_ts.isoformat(),
        "store_id": f"STORE-{fake.city()[:3].upper()}-{random.randint(1, 20):02d}",
        "customer_id": random.choice(customer_ids),
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": random.choice(["card", "cash", "sbp"]),
    }


def produce_pos_events(**context):
    """Publish a finite, traceable POS batch spanning BACKFILL_DAYS days, returned via XCom."""
    events_per_day = int(os.getenv("POS_EVENTS_PER_RUN", "100"))
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
        now = datetime.now(timezone.utc)
        futures = []
        total_events = 0

        # Генерируем BACKFILL_DAYS дней истории: от (BACKFILL_DAYS-1) дней назад до сегодня.
        for day_offset in range(BACKFILL_DAYS - 1, -1, -1):
            day_ts = now - timedelta(days=day_offset)
            daily_events = max(40, int(random.gauss(events_per_day, events_per_day * 0.2)))
            for _ in range(daily_events):
                # разбрасываем время событий в течение дня, а не все в одну секунду
                event_ts = day_ts.replace(
                    hour = random.choices(
                        [8,9,10,11,12,13,14,15,16,17,18,19,20,21],
                        weights=[2,3,5,6,8,10,10,8,7,9,14,15,12,5]
                    )[0],
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                    microsecond=0,
                )
                futures.append(
                    producer.send(
                        TOPIC_NAME,
                        _make_event(customer_ids, product_ids, batch_id, fake, event_ts),
                    )
                )
                total_events += 1

        for future in futures:
            future.get(timeout=30)
        producer.flush(timeout=30)
    finally:
        producer.close()

    LOGGER.info(
        "Published %s POS events across %s days for batch %s",
        total_events, BACKFILL_DAYS, batch_id,
    )
    return {"batch_id": batch_id, "event_count": total_events}