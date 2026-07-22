# Superset dashboards

After `docker compose up -d --build`, open `http://localhost:8088`.

Default credentials: `admin` / `admin`. Change them in `.env` with
`SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD`, `SUPERSET_ADMIN_EMAIL`, and
set a unique `SUPERSET_SECRET_KEY` before using the stack outside local development.

The image extends the official `apache/superset:latest` image only with the Trino SQLAlchemy
driver. The Airflow task `provision_superset_dashboards` runs after the Gold validation and
idempotently creates the Trino connection plus the three dashboards.

To verify the driver and network before running the Airflow task, run:

```bash
docker compose exec superset python -c "from sqlalchemy import create_engine, text; e = create_engine('trino://airflow@trino:8080/iceberg/gold'); print(e.connect().execute(text('SELECT 1')).scalar())"
```

The command must print `1`.
