"""
Automated Superset Dashboard Setup
====================================
Uses the Superset REST API to programmatically create:
1. Database connection  → Gold PostgreSQL DB
2. Datasets            → mart_class_daily_performance, fact_attendance, fact_assessments
3. Charts              → attendance rate trend, avg score by class/subject, mart table
4. Dashboard           → "Class Performance Dashboard"

Run after the Gold layer is populated:
    make setup-superset
  or:
    docker compose exec superset python /app/docker/dashboard/setup_dashboard.py
"""
import os
import sys
import time
import json
import logging
import requests
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER   = os.environ.get("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS   = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

GOLD_DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
GOLD_DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
GOLD_DB_NAME = os.environ.get("GOLD_DB_NAME", "gold_db")
GOLD_DB_USER = os.environ.get("GOLD_DB_USER", "gold")
GOLD_DB_PASS = os.environ.get("GOLD_DB_PASSWORD", "gold")


# ─── Auth ─────────────────────────────────────────────────────────────────────

def get_token() -> str:
    resp = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
        "provider": "db",
        "refresh": True,
    }, timeout=30)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    logger.info("Authenticated as %s", ADMIN_USER)
    return token


def headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_csrf_token(token: str) -> str:
    resp = requests.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/",
                        headers=headers(token), timeout=15)
    resp.raise_for_status()
    return resp.json()["result"]


# ─── Database ─────────────────────────────────────────────────────────────────

def create_or_get_database(token: str) -> int:
    """Create the Gold PostgreSQL connection if it doesn't exist."""
    h = headers(token)
    sqlalchemy_uri = (
        f"postgresql+psycopg2://{GOLD_DB_USER}:{GOLD_DB_PASS}"
        f"@{GOLD_DB_HOST}:{GOLD_DB_PORT}/{GOLD_DB_NAME}"
    )

    # Check if already exists
    resp = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=h, timeout=15)
    for db in resp.json().get("result", []):
        if db["database_name"] == "Gold DB":
            logger.info("Database 'Gold DB' already exists (id=%s)", db["id"])
            return db["id"]

    # Create
    payload = {
        "database_name": "Gold DB",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
    }
    resp = requests.post(f"{SUPERSET_URL}/api/v1/database/",
                         headers=h, json=payload, timeout=30)
    resp.raise_for_status()
    db_id = resp.json()["id"]
    logger.info("Created database 'Gold DB' (id=%d)", db_id)
    return db_id


# ─── Datasets ─────────────────────────────────────────────────────────────────

def create_or_get_dataset(token: str, db_id: int, table_name: str, schema: str = "gold") -> int:
    h = headers(token)

    resp = requests.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=h, timeout=15)
    for ds in resp.json().get("result", []):
        if ds["table_name"] == table_name and ds.get("schema") == schema:
            logger.info("Dataset '%s.%s' already exists (id=%s)", schema, table_name, ds["id"])
            return ds["id"]

    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table_name,
    }
    resp = requests.post(f"{SUPERSET_URL}/api/v1/dataset/",
                         headers=h, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        logger.warning("Could not create dataset %s: %s", table_name, resp.text)
        return -1
    ds_id = resp.json()["id"]
    logger.info("Created dataset '%s.%s' (id=%d)", schema, table_name, ds_id)
    return ds_id


# ─── Dataset helpers ─────────────────────────────────────────────────────────

def set_dataset_temporal_col(token: str, ds_id: int, col_name: str) -> None:
    """
    Tell Superset which column is the time axis for a dataset.
    Without this, any time-series chart will error with
    'Datetime column not provided as part of table configuration'.
    """
    resp = requests.put(
        f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
        headers=headers(token),
        json={"main_dttm_col": col_name},
        timeout=15,
    )
    if resp.status_code in (200, 201):
        logger.info("Set main_dttm_col='%s' on dataset id=%d", col_name, ds_id)
    else:
        logger.warning("Could not set temporal col: %s", resp.text)


def _m(column: str, aggregate: str, label: str = None) -> dict:
    """
    Build a Superset adhoc metric object.

    Using bare strings like "metrics": ["attendance_rate"] only works for
    metrics pre-saved on the dataset. For columns queried ad-hoc (our case),
    Superset requires the full expressionType object.
    """
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label or f"{aggregate}({column})",
    }


# ─── Charts ───────────────────────────────────────────────────────────────────

def create_chart(token: str, payload: dict) -> Optional[int]:
    """
    Create or update a chart.
    If a chart with the same name already exists, update its params via PUT
    so that fixes to chart configuration are always applied on re-run.
    """
    h = headers(token)

    resp = requests.get(f"{SUPERSET_URL}/api/v1/chart/", headers=h, timeout=15)
    for ch in resp.json().get("result", []):
        if ch["slice_name"] == payload["slice_name"]:
            chart_id = ch["id"]
            # Always update params so config changes take effect on re-run
            update = requests.put(
                f"{SUPERSET_URL}/api/v1/chart/{chart_id}",
                headers=h, json=payload, timeout=30,
            )
            if update.status_code in (200, 201):
                logger.info("Updated chart '%s' (id=%d)", payload["slice_name"], chart_id)
            else:
                logger.warning("Could not update chart '%s': %s", payload["slice_name"], update.text)
            return chart_id

    resp = requests.post(f"{SUPERSET_URL}/api/v1/chart/",
                         headers=h, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        logger.warning("Could not create chart %s: %s", payload["slice_name"], resp.text)
        return None
    chart_id = resp.json()["id"]
    logger.info("Created chart '%s' (id=%d)", payload["slice_name"], chart_id)
    return chart_id


# ─── Dashboard ────────────────────────────────────────────────────────────────

def _build_positions(chart_ids: list) -> dict:
    """
    Build a Superset v2 dashboard positions dict that places each chart
    in its own full-width row in a single GRID column.

    Superset 3.x dropped the `slices` field from the dashboard API.
    Charts must be linked via `json_metadata.positions` instead.
    Each CHART entry references the chart by its numeric `chartId`.
    """
    grid_id = "GRID_ID"
    root_id = "ROOT_ID"

    positions: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        root_id: {
            "children": [grid_id],
            "id": root_id,
            "type": "ROOT",
        },
        grid_id: {
            "children": [],
            "id": grid_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": [root_id],
            "type": "GRID",
        },
    }

    for i, chart_id in enumerate(chart_ids):
        row_key   = f"ROW-{i}"
        chart_key = f"CHART-{chart_id}"

        positions[row_key] = {
            "children": [chart_key],
            "id": row_key,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": [grid_id],
            "type": "ROW",
        }
        positions[chart_key] = {
            "children": [],
            "id": chart_key,
            "meta": {
                "chartId": chart_id,
                "sliceId": chart_id,
                "width": 12,      # full width
                "height": 50,
            },
            "parents": [row_key],
            "type": "CHART",
        }
        positions[grid_id]["children"].append(row_key)

    return positions


def create_dashboard(token: str, title: str, chart_ids: list) -> int:
    h = headers(token)

    # Check if already exists
    resp = requests.get(f"{SUPERSET_URL}/api/v1/dashboard/", headers=h, timeout=15)
    for db in resp.json().get("result", []):
        if db["dashboard_title"] == title:
            logger.info("Dashboard '%s' already exists (id=%s)", title, db["id"])
            return db["id"]

    # Step 1: create the dashboard (no slices — Superset 3.x dropped that field)
    resp = requests.post(
        f"{SUPERSET_URL}/api/v1/dashboard/",
        headers=h,
        json={"dashboard_title": title, "published": True},
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        logger.warning("Could not create dashboard: %s", resp.text)
        return -1
    dash_id = resp.json()["id"]
    logger.info("Created dashboard '%s' (id=%d)", title, dash_id)

    # Step 2: link charts by updating json_metadata with positions layout
    if chart_ids:
        positions = _build_positions(chart_ids)
        update_resp = requests.put(
            f"{SUPERSET_URL}/api/v1/dashboard/{dash_id}",
            headers=h,
            json={"json_metadata": json.dumps({"positions": positions})},
            timeout=30,
        )
        if update_resp.status_code not in (200, 201):
            logger.warning("Could not link charts to dashboard: %s", update_resp.text)
        else:
            logger.info("Linked %d charts to dashboard (id=%d)", len(chart_ids), dash_id)

    return dash_id


# ─── Main ─────────────────────────────────────────────────────────────────────

def wait_for_superset(max_retries: int = 20) -> None:
    for i in range(max_retries):
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.status_code == 200:
                logger.info("Superset is ready")
                return
        except requests.exceptions.ConnectionError:
            pass
        logger.info("Waiting for Superset... (%d/%d)", i + 1, max_retries)
        time.sleep(5)
    raise RuntimeError("Superset did not become ready in time")


def main():
    wait_for_superset()
    token = get_token()
    db_id = create_or_get_database(token)

    # Create datasets
    mart_ds_id = create_or_get_dataset(token, db_id, "mart_class_daily_performance")
    att_ds_id  = create_or_get_dataset(token, db_id, "fact_attendance")
    asm_ds_id  = create_or_get_dataset(token, db_id, "fact_assessments")

    # Tell Superset which column is the time axis on the mart dataset.
    # Without this, any time-series chart errors with "Datetime column not provided".
    if mart_ds_id > 0:
        set_dataset_temporal_col(token, mart_ds_id, "date")
    if att_ds_id > 0:
        set_dataset_temporal_col(token, att_ds_id, "attendance_date")
    if asm_ds_id > 0:
        set_dataset_temporal_col(token, asm_ds_id, "assessment_date")

    chart_ids = []

    # ── Chart 1: Attendance Rate Trend (line, one series per class) ───────────
    # Uses granularity_sqla + time_grain_sqla so Superset knows the time axis.
    # Uses _m() adhoc metric — NOT a bare string which only works for saved metrics.
    if mart_ds_id > 0:
        c1 = create_chart(token, {
            "slice_name": "Attendance Rate by Class Over Time",
            "viz_type": "line",
            "datasource_id": mart_ds_id,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type": "line",
                "granularity_sqla": "date",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "metrics": [_m("attendance_rate", "AVG", "Avg Attendance Rate")],
                "groupby": ["class_id"],
                "row_limit": 10000,
                "show_legend": True,
            }),
        })
        if c1:
            chart_ids.append(c1)

    # ── Chart 2: Average Score by Class (dist_bar = categorical bar) ──────────
    # Must use dist_bar, NOT bar.
    # In Superset legacy: bar = time-series bar (requires time axis config).
    #                     dist_bar = categorical bar (groups by arbitrary dimensions).
    # After setting main_dttm_col="date" on the mart dataset, the plain "bar" chart
    # tries to use a time axis and fails when groupby contains a non-temporal column.
    if mart_ds_id > 0:
        c2 = create_chart(token, {
            "slice_name": "Average Score by Class",
            "viz_type": "dist_bar",
            "datasource_id": mart_ds_id,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type": "dist_bar",
                "metrics": [_m("avg_score", "AVG", "Avg Score")],
                "groupby": ["class_id"],
                "row_limit": 50,
            }),
        })
        if c2:
            chart_ids.append(c2)

    # ── Chart 3: Present vs Absent by Class (stacked bar) ──────────────────
    if mart_ds_id > 0:
        c3 = create_chart(token, {
            "slice_name": "Present vs Absent by Class",
            "viz_type": "dist_bar",
            "datasource_id": mart_ds_id,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type": "dist_bar",
                "metrics": [
                    _m("present_count", "SUM", "Present"),
                    _m("absent_count",  "SUM", "Absent"),
                ],
                "groupby": ["class_id"],
                "row_limit": 50,
            }),
        })
        if c3:
            chart_ids.append(c3)

    # ── Chart 4: Average Score by Subject (pie) ──────────────────────────
    # Pie charts use "metric" (singular), not "metrics" (list).
    if asm_ds_id > 0:
        c4 = create_chart(token, {
            "slice_name": "Average Score by Subject",
            "viz_type": "pie",
            "datasource_id": asm_ds_id,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type": "pie",
                "metric": _m("score", "AVG", "Avg Score"),
                "groupby": ["subject"],
                "row_limit": 10,
            }),
        })
        if c4:
            chart_ids.append(c4)

    # ── Chart 5: Class Daily Performance (table — shows raw mart columns) ────
    if mart_ds_id > 0:
        c5 = create_chart(token, {
            "slice_name": "Class Daily Performance Table",
            "viz_type": "table",
            "datasource_id": mart_ds_id,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type": "table",
                "all_columns": [
                    "class_id", "date", "total_students",
                    "students_with_attendance", "present_count", "absent_count",
                    "attendance_rate", "students_with_assessment",
                    "assessment_count", "avg_score",
                ],
                "row_limit": 500,
            }),
        })
        if c5:
            chart_ids.append(c5)

    # ── Create dashboard ──────────────────────────────────────────────────────
    if chart_ids:
        dash_id = create_dashboard(token, "Class Performance Dashboard", chart_ids)
        dash_url = f"{SUPERSET_URL}/superset/dashboard/{dash_id}/"
        logger.info("=" * 60)
        logger.info("Dashboard ready: %s", dash_url)
        logger.info("=" * 60)
    else:
        logger.warning("No charts created — skipping dashboard creation")


if __name__ == "__main__":
    main()
