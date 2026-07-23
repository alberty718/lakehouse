"""Minimal baseline forecast for daily revenue, built on top of gold.sales_daily.

No new infrastructure: reads/writes through the same Trino/Iceberg gold layer the rest
of the pipeline already uses. Trains on every run (cheap — dataset is small), so there's
no separate model-artifact storage to manage.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import trino
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

LOGGER = logging.getLogger(__name__)

MIN_ROWS_REQUIRED = 14  # need at least ~2 weeks of history for lag_7 + a meaningful test split
FORECAST_DAYS = int(os.getenv("REVENUE_FORECAST_DAYS", "7"))

FEATURE_COLS = [
    "dow", "is_weekend",
    "revenue_lag_1", "revenue_lag_2", "revenue_lag_7",
    "revenue_rolling_3", "revenue_rolling_7",
]


def get_connection(schema="gold"):
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "airflow"),
        catalog="iceberg",
        schema=schema,
    )


def ensure_forecast_table():
    """Create the forecast table once; no-op if it already exists."""
    with get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS iceberg.gold.revenue_forecast (
                sale_date DATE,
                revenue_forecast DOUBLE,
                model_name VARCHAR,
                generated_at TIMESTAMP
            ) WITH (format = 'PARQUET')
            """
        )


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["dow"] = data["sale_date"].dt.dayofweek
    data["is_weekend"] = data["dow"].isin([5, 6]).astype(int)

    for lag in [1, 2, 7]:
        data[f"revenue_lag_{lag}"] = data["revenue"].shift(lag)

    data["revenue_rolling_3"] = data["revenue"].shift(1).rolling(3).mean()
    data["revenue_rolling_7"] = data["revenue"].shift(1).rolling(7).mean()

    return data.dropna().reset_index(drop=True)


def _pick_best_model(data: pd.DataFrame):
    split_idx = max(int(len(data) * 0.8), len(data) - 1)
    train, test = data.iloc[:split_idx], data.iloc[split_idx:]

    if test.empty:
        # Not enough rows for a held-out split — train on everything, skip comparison.
        model = RandomForestRegressor(n_estimators=200, random_state=42)
        model.fit(data[FEATURE_COLS], data["revenue"])
        return model, "RandomForest"

    candidates = {
        "LinearRegression": LinearRegression(),
        "RandomForest": RandomForestRegressor(n_estimators=200, random_state=42),
    }
    scored = {}
    for name, model in candidates.items():
        model.fit(train[FEATURE_COLS], train["revenue"])
        preds = model.predict(test[FEATURE_COLS])
        scored[name] = mean_absolute_error(test["revenue"], preds)
        LOGGER.info("Model %s MAE on holdout: %.2f", name, scored[name])

    best_name = min(scored, key=scored.get)
    best_model = candidates[best_name]
    # Refit the winner on the full dataset before forecasting forward.
    best_model.fit(data[FEATURE_COLS], data["revenue"])
    return best_model, best_name


def _forecast_forward(history: pd.DataFrame, model, n_days: int) -> pd.DataFrame:
    hist = history[["sale_date", "revenue"]].copy()
    rows = []

    for _ in range(n_days):
        next_date = hist["sale_date"].max() + pd.Timedelta(days=1)
        dow = next_date.dayofweek
        row = pd.DataFrame([{
            "dow": dow,
            "is_weekend": int(dow in [5, 6]),
            "revenue_lag_1": hist["revenue"].iloc[-1],
            "revenue_lag_2": hist["revenue"].iloc[-2],
            "revenue_lag_7": hist["revenue"].iloc[-7],
            "revenue_rolling_3": hist["revenue"].iloc[-3:].mean(),
            "revenue_rolling_7": hist["revenue"].iloc[-7:].mean(),
        }])[FEATURE_COLS]

        pred = float(model.predict(row)[0])
        rows.append({"sale_date": next_date, "revenue_forecast": pred})
        hist = pd.concat(
            [hist, pd.DataFrame([{"sale_date": next_date, "revenue": pred}])],
            ignore_index=True,
        )

    return pd.DataFrame(rows)


def train_and_forecast(**context):
    """Read gold.sales_daily, train a baseline model, write forward forecast to gold.revenue_forecast."""
    with get_connection() as connection:
        df = pd.read_sql(
            "SELECT sale_date, revenue FROM iceberg.gold.sales_daily ORDER BY sale_date",
            connection,
        )

    if len(df) < MIN_ROWS_REQUIRED:
        LOGGER.warning(
            "Only %s rows in gold.sales_daily (need >= %s) — skipping forecast this run.",
            len(df), MIN_ROWS_REQUIRED,
        )
        return {"skipped": True, "rows_available": len(df)}

    df["sale_date"] = pd.to_datetime(df["sale_date"])
    data = _build_features(df)

    if len(data) < 3:
        LOGGER.warning("Not enough rows after feature engineering — skipping forecast this run.")
        return {"skipped": True, "rows_available": len(data)}

    model, model_name = _pick_best_model(data)
    forecast_df = _forecast_forward(df, model, FORECAST_DAYS)
    forecast_df["model_name"] = model_name
    generated_at = datetime.now(timezone.utc)

    with get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM iceberg.gold.revenue_forecast")
        values = ",".join(
            f"(DATE '{row.sale_date.date()}', {row.revenue_forecast}, "
            f"'{model_name}', TIMESTAMP '{generated_at.strftime('%Y-%m-%d %H:%M:%S')}')"
            for row in forecast_df.itertuples()
        )
        cursor.execute(
            f"""
            INSERT INTO iceberg.gold.revenue_forecast
                (sale_date, revenue_forecast, model_name, generated_at)
            VALUES {values}
            """
        )

    LOGGER.info(
        "Wrote %s-day forecast (model=%s) to gold.revenue_forecast", FORECAST_DAYS, model_name
    )
    return {"skipped": False, "model_name": model_name, "forecast_days": FORECAST_DAYS}