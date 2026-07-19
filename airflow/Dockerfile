FROM apache/airflow:2.9.1-python3.11

RUN pip install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-trino==1.8.0 \
    minio==7.2.7 \
    kafka-python-ng==2.2.2 \
    faker==25.0.0 \
    apache-airflow-providers-amazon==8.26.0

ENV DBT_LOG_PATH=/tmp/dbt_logs
ENV DBT_TARGET_PATH=/tmp/dbt_target