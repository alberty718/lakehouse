"""The single scheduled pipeline that keeps reference and POS IDs in sync."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from batch_ingest_crm import generate_and_upload_crm
from batch_ingest_products import generate_and_upload_products
from create_silver_tables import create_tables as create_silver_tables
from etl.pos_producer import produce_pos_events
from etl.superset_provisioner import provision_dashboards
from ingest_pos import consume_and_upload_to_minio
from load_raw_to_silver import load as load_raw_to_silver
from load_silver_to_gold import (
    build_aggregates,
    create_gold_tables,
    rebuild_fact_sales,
    validate_gold_layer,
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_lakehouse_pipeline",
    description="Synchronized CRM, products, POS, Silver and Gold pipeline",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["retail", "kafka", "iceberg", "orchestrated"],
) as dag:
    ensure_silver_tables = PythonOperator(
        task_id="create_silver_tables", python_callable=create_silver_tables
    )
    generate_customers = PythonOperator(
        task_id="generate_customers", python_callable=generate_and_upload_crm
    )
    generate_products = PythonOperator(
        task_id="generate_products", python_callable=generate_and_upload_products
    )
    produce_events = PythonOperator(
        task_id="produce_pos_events", python_callable=produce_pos_events
    )
    consume_events = PythonOperator(
        task_id="consume_kafka_to_raw", python_callable=consume_and_upload_to_minio
    )
    load_silver = PythonOperator(
        task_id="load_raw_to_silver", python_callable=load_raw_to_silver
    )
    ensure_gold_tables = PythonOperator(
        task_id="create_gold_tables", python_callable=create_gold_tables
    )
    load_fact = PythonOperator(
        task_id="rebuild_fact_sales", python_callable=rebuild_fact_sales
    )
    load_marts = PythonOperator(
        task_id="build_aggregates", python_callable=build_aggregates
    )
    validate_gold = PythonOperator(
        task_id="validate_gold_layer", python_callable=validate_gold_layer
    )
    provision_superset = PythonOperator(
        task_id="provision_superset_dashboards", python_callable=provision_dashboards
    )

    ensure_silver_tables >> generate_customers >> generate_products >> produce_events
    produce_events >> consume_events >> load_silver >> ensure_gold_tables
    ensure_gold_tables >> load_fact >> load_marts >> validate_gold >> provision_superset
