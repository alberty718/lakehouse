# OrangeData

OrangeData - это дата платформа для розничной торговли, собранная как lakehouse на открытых технологиях. Она обрабатывает данные о клиентах, товарах и продажах через Kafka, MinIO и Iceberg, а на выходе отдаёт готовые витрины и дашборд в Superset.

---

## Стек

Apache Airflow, Apache Kafka, Apache Iceberg, Project Nessie, Trino, Apache Superset, MinIO, PostgreSQL, Docker Compose.

---

## Архитектура

```
CRM Generator
      │
Products Generator
      │
Kafka Producer
      │
      ▼
 Apache Kafka
      │
      ▼
 Airflow Consumer
      │
      ├────────► Raw (MinIO)
      │
      ▼
 Silver (Iceberg)
      │
      ▼
 Gold (Iceberg)
      │
      ▼
 ML: прогноз выручки
      │
      ▼
 Apache Superset
```

Всё это выполняется одним DAG в Airflow: генерация клиентов и товаров → генерация транзакций и публикация в Kafka → чтение из Kafka → запись Raw в MinIO → очистка в Silver → витрины в Gold → обучение ML-модели и прогноз выручки → создание датасетов и дашборда в Superset.

---

## Слои данных

**Raw** — сырые JSON-сообщения из Kafka, без изменений, лежат в MinIO на случай переобработки.

**Silver** — очищенные и нормализованные таблицы: `customers`, `products`, `pos_transactions`, `transaction_items`.

**Gold** — витрины для аналитики: `fact_sales`, `sales_daily`, `product_sales`, `revenue_forecast` (прогноз выручки от ML-модели, см. раздел ниже).

---

## ML-модель прогнозирования выручки

Как она работает:

- Источник данных — витрина `gold.sales_daily` (дневная выручка).
- Признаки: день недели, флаг выходного, лаги выручки, скользящие средние.
- На каждом запуске обучаются две модели — `LinearRegression` и `RandomForestRegressor` — на исторической части данных (80/20 split), по MAE на отложенной выборке выбирается лучшая.
- Результат записывается в таблицу `gold.revenue_forecast`.
- Прогноз публикуется в Superset отдельным чартом "Прогноз выручки".
- Если в `gold.sales_daily` меньше 14 дней истории, задача просто пропускает обучение.

---

## Dashboard

После запуска DAG-ов в Superset UI появляются dashboards и charts. При желании можно собрать собственный дашборд из готовых charts.

---

## Запуск

**1. Заполнить `.env`.**

Fernet key для Airflow генерируется так:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Вписать в `AIRFLOW__CORE__FERNET_KEY`. Остальные переменные (`POSTGRES_*`, `MINIO_*`, `SUPERSET_*`) — под своё окружение.

**2. Поднять контейнеры.**

```bash
docker compose up -d --build
```

**3. Зайти в Airflow** — http://localhost:8080, логин `admin`, пароль:

```bash
docker compose logs airflow | grep password
```

**4. Запустить DAG** `retail_lakehouse_pipeline`. Он сам всё сделает — от генерации данных до готового дашборда.

---

## Сервисы

| Сервис | Адрес |
|--------|--------|
| Airflow | http://localhost:8080 |
| Superset | http://localhost:8088 |
| Trino | http://localhost:8081 |
| MinIO Console | http://localhost:9001 |

---

## Структура проекта

```
.
├── airflow/
│   ├── dags/
│   ├── plugins/
│   └── requirements/
├── kafka/
├── minio/
├── postgres/
├── superset/
├── trino/
├── docker-compose.yml
├── .env
└── README.md
```

---

## Контакты для связи

Telegram: @thealberty
