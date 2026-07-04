# Mini-Lakehouse: Trino + Iceberg + MinIO + PostgreSQL

Локальный lakehouse-стенд в Docker. Trino выполняет SQL-запросы к таблицам
в формате Iceberg. Метаданные таблиц (схемы, снапшоты) хранятся в PostgreSQL
через Iceberg JDBC-каталог. Сами данные (parquet-файлы) лежат в MinIO —
S3-совместимом хранилище.

## Сервисы и порты

- **Trino** — SQL-движок, порт 8080 (Web UI и запросы)
- **MinIO** — S3-хранилище, порт 9000 (API) и 9001 (веб-консоль)
- **PostgreSQL** — метастор Iceberg, порт 5432

## Запуск

    cp .env.example .env      # заполнить своими значениями
    docker compose up -d
    docker ps                 # все три сервиса должны быть healthy

## Создание бакета

Зайти в MinIO UI (http://localhost:9001), логин/пароль — из `.env`
(MINIO_ROOT_USER / MINIO_ROOT_PASSWORD), создать бакет `lakehouse`.

## Инициализация таблиц

    docker exec -i trino trino --catalog iceberg < sql/create_table.sql
    docker exec -i trino trino --catalog iceberg < sql/insert.sql

## Проверка

    docker exec -i trino trino --catalog iceberg < sql/select.sql      # JOIN
    docker exec -i trino trino --catalog iceberg < sql/timetravel.sql  # Time Travel

## Health-check

    ./healthcheck/check_postgres.sh
    ./healthcheck/check_minio.sh
    ./healthcheck/check_trino.sh

## Структура репозитория

- `docker-compose.yml` — описание сервисов
- `.env.example` — шаблон переменных окружения
- `trino/catalog/iceberg.properties` — конфиг Iceberg-каталога
- `sql/` — SQL-скрипты (create_table, insert, select, timetravel)
- `healthcheck/` — скрипты проверки живости сервисов

## Скриншоты

![MinIO bucket](screenshots/minio.jpg)
![Trino query](screenshots/trino.jpg)