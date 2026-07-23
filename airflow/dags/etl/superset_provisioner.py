"""Create Superset datasets, charts and dashboards through its public REST API."""

import json
import logging
import os
from time import sleep

import requests

LOGGER = logging.getLogger(__name__)
BASE_URL = os.getenv("SUPERSET_URL", "http://superset:8088").rstrip("/")
USERNAME = os.getenv("SUPERSET_ADMIN_USERNAME", "admin")
PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
DATABASE_NAME = "Trino Iceberg Gold"
# The same Trino user is already used successfully by the Airflow Gold tasks.
DATABASE_URI = "trino://airflow@trino:8080/iceberg/gold"


def _api(session, method, path, **kwargs):
    response = session.request(method, f"{BASE_URL}{path}", timeout=30, **kwargs)
    if not response.ok:
        raise RuntimeError(f"Superset {method} {path}: {response.status_code} {response.text}")
    return response.json() if response.content else {}


def _session():
    session = requests.Session()
    for attempt in range(20):
        try:
            login = _api(session, "POST", "/api/v1/security/login", json={
                "username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True,
            })
            session.headers["Authorization"] = f"Bearer {login['access_token']}"
            session.headers["X-CSRFToken"] = _api(session, "GET", "/api/v1/security/csrf_token/")["result"]
            return session
        except (requests.RequestException, RuntimeError) as error:
            if attempt == 19:
                raise RuntimeError("Superset is not available for dashboard provisioning.") from error
            LOGGER.info("Waiting for Superset (%s/20): %s", attempt + 1, error)
            sleep(5)


def _items(session, resource):
    return _api(session, "GET", f"/api/v1/{resource}/?q=(page:0,page_size:100)").get("result", [])


def _named(session, resource, field, name):
    return next((item for item in _items(session, resource) if item.get(field) == name), None)


def _database(session):
    payload = {"database_name": DATABASE_NAME, "sqlalchemy_uri": DATABASE_URI,
               "expose_in_sqllab": True, "allow_ctas": False, "allow_cvas": False, "allow_dml": False}
    current = _named(session, "database", "database_name", DATABASE_NAME)
    method = "PUT" if current else "POST"
    path = f"/api/v1/database/{current['id']}" if current else "/api/v1/database/"
    for attempt in range(12):
        try:
            response = _api(session, method, path, json=payload)
            return current["id"] if current else response["id"]
        except RuntimeError as error:
            if "422" not in str(error) or attempt == 11:
                raise
            LOGGER.info("Trino connection is not ready in Superset (%s/12): %s", attempt + 1, error)
            sleep(10)


def _dataset(session, database_id, table):
    for item in _items(session, "dataset"):
        database = item.get("database") or {}
        current_database_id = database.get("id") if isinstance(database, dict) else database
        if item.get("table_name") == table and item.get("schema") == "gold" and current_database_id == database_id:
            return item["id"]
    return _api(session, "POST", "/api/v1/dataset/", json={
        "database": database_id, "schema": "gold", "table_name": table,
    })["id"]


def _dashboard(session, title, slug):
    current = _named(session, "dashboard", "dashboard_title", title)
    payload = {"dashboard_title": title, "slug": slug, "published": True, "position_json": "{}"}
    if current:
        _api(session, "PUT", f"/api/v1/dashboard/{current['id']}", json=payload)
        return current["id"]
    return _api(session, "POST", "/api/v1/dashboard/", json=payload)["id"]


def _chart(session, name, dataset_id, viz_type, params, dashboard_id):
    payload = {
        "slice_name": name, "viz_type": viz_type, "datasource_id": dataset_id,
        "datasource_type": "table", "dashboards": [dashboard_id],
        "params": json.dumps({"datasource": f"{dataset_id}__table", "viz_type": viz_type, **params}),
    }
    current = _named(session, "chart", "slice_name", name)
    if current:
        _api(session, "PUT", f"/api/v1/chart/{current['id']}", json=payload)
        chart_id = current["id"]
    else:
        chart_id = _api(session, "POST", "/api/v1/chart/", json=payload)["id"]
    # A dashboard tile links to its chart by UUID, not just by numeric id - see
    # https://github.com/apache/superset/issues/32966 and
    # https://github.com/apache/superset/issues/15456. Charts created through the plain
    # chart API don't automatically get wired into a hand-built position_json unless we
    # embed that UUID in the tile's meta ourselves, so we fetch it explicitly here.
    chart_uuid = _api(session, "GET", f"/api/v1/chart/{chart_id}").get("result", {}).get("uuid")
    return chart_id, chart_uuid


def _layout(session, dashboard_id, title, charts):
    rows = [f"ROW-{index}" for index in range(len(charts))]
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": rows, "parents": ["ROOT_ID"]},
        # A missing HEADER_ID node makes the dashboard front end throw on load
        # ("Unexpected error") regardless of whether the charts themselves are fine -
        # this was the actual cause of every dashboard failing to open.
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }
    for index, (chart_title, (chart_id, chart_uuid)) in enumerate(charts.items()):
        row, node = rows[index], f"CHART-{chart_id}"
        position[row] = {
            "id": row, "type": "ROW", "children": [node],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        meta = {"chartId": chart_id, "sliceName": chart_title, "width": 12, "height": 32}
        if chart_uuid:
            meta["uuid"] = chart_uuid
        position[node] = {"id": node, "type": "CHART", "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                          "meta": meta}
    _api(session, "PUT", f"/api/v1/dashboard/{dashboard_id}", json={"position_json": json.dumps(position), "published": True})


def provision_dashboards():
    """Airflow task: run after Gold validation, when all tables already contain data."""
    session = _session()
    database_id = _database(session)
    daily, products = _dataset(session, database_id, "sales_daily"), _dataset(session, database_id, "product_sales")
    forecast = _dataset(session, database_id, "revenue_forecast")

    overview_title = "Retail — Executive Overview"
    overview_id = _dashboard(session, overview_title, "retail-executive-overview")
    overview = {
        "Выручка": _chart(session, "KPI — Выручка", daily, "big_number_total", {"metric": {"expressionType": "SQL", "sqlExpression": "SUM(revenue)", "label": "Выручка"}}, overview_id),
        "Транзакции": _chart(session, "KPI — Транзакции", daily, "big_number_total", {"metric": {"expressionType": "SQL", "sqlExpression": "SUM(transactions_count)", "label": "Транзакции"}}, overview_id),
        "Средний чек": _chart(session, "KPI — Средний чек", daily, "big_number_total", {"metric": {"expressionType": "SQL", "sqlExpression": "AVG(avg_check)", "label": "Средний чек"}}, overview_id),
    }
    _layout(session, overview_id, overview_title, overview)

    trend_title = "Retail — Sales Trend"
    trend_id = _dashboard(session, trend_title, "retail-sales-trend")
    # NOTE: since Superset 3.x, "Generic Chart Axes" (GENERIC_CHART_AXES, on by default,
    # including in 4.1.2) is used for every echarts_* viz type. These charts no longer read
    # the temporal column from "granularity_sqla" (that field is now legacy/unused for
    # rendering) — they require an explicit "x_axis" param telling Superset which column
    # to plot on the X axis. Without it, the query has no temporal dimension to group by,
    # so the chart silently returns/renders empty. We also add an explicit TEMPORAL_RANGE
    # adhoc_filter on that column, which is how Superset scopes the "No filter" time range
    # to a specific column when generic chart axes are in play.
    trend_params = {
        "x_axis": "sale_date",
        "time_grain_sqla": "P1D",
        "granularity_sqla": "sale_date",  # kept for backwards compat / SQL Lab preview only
        "time_range": "No filter",
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "subject": "sale_date",
                "operator": "TEMPORAL_RANGE",
                "comparator": "No filter",
                "expressionType": "SIMPLE",
            }
        ],
        "x_axis_sort_asc": True,
        "row_limit": 10000,
    }
    trend = {
        "Динамика выручки": _chart(session, "Динамика выручки", daily, "echarts_timeseries_line", {**trend_params, "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(revenue)", "label": "Выручка"}]}, trend_id),
        "Динамика транзакций": _chart(session, "Динамика транзакций", daily, "echarts_timeseries_line", {**trend_params, "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(transactions_count)", "label": "Транзакции"}]}, trend_id),
        "Прогноз выручки": _chart(session, "Прогноз выручки", forecast, "echarts_timeseries_line", {**trend_params, "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(revenue_forecast)", "label": "Forecast"}]}, trend_id),
    }
    _layout(session, trend_id, trend_title, trend)

    product_title = "Retail — Product Performance"
    product_dashboard_id = _dashboard(session, product_title, "retail-product-performance")
    product = {
        "Топ-10 товаров": _chart(
            session,
            "Топ-10 товаров по выручке",
            products,
            "dist_bar",
            {
                "groupby": ["product_name"],
                "metrics": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "SUM(revenue)",
                        "label": "Выручка",
                    }
                ],
                "row_limit": 10,
                "order_desc": True,
            },
            product_dashboard_id,
        ),
    }
    _layout(session, product_dashboard_id, product_title, product)
    LOGGER.info("Three Superset dashboards were provisioned.")