"""
Ingestion layer: Source files → MinIO Bronze.
Bronze is intentionally immutable and append-only
Any duplicates in the source are preserved here deduplication is Silver's job.
"""
import json
import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd
import pyarrow as pa

from scripts.utils.minio_client import get_minio_client, upload_parquet

logger = logging.getLogger(__name__)

SOURCE_DATA_PATH = os.environ.get("SOURCE_DATA_PATH", "/opt/airflow/data")
BRONZE_BUCKET    = os.environ.get("MINIO_BRONZE_BUCKET", "bronze")


# ─── Shared helpers ───────────────────────────────────────────────────────────

def _read_csv_as_strings(filename: str) -> pa.Table:
    """
    Read a CSV preserving ALL values as strings.
    We intentionally don't infer types at Bronze, types are assigned in Silver.
    pandas dtype=str + keep_default_na=False ensures no implicit None or float coercion.
    """
    path = os.path.join(SOURCE_DATA_PATH, filename)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return pa.Table.from_pandas(df, preserve_index=False)


def _read_json_as_strings(filename: str) -> pa.Table:
    """Read a JSON array file coercing all values to strings for raw Bronze."""
    path = os.path.join(SOURCE_DATA_PATH, filename)
    with open(path, encoding="utf-8") as f:
        records = json.load(f)
    # Convert each value to str — consistent with CSV bronze (no typing yet)
    data: Dict[str, list] = {k: [str(r[k]) for r in records] for k in records[0]}
    return pa.table(data)


def _with_metadata(table: pa.Table, source_file: str) -> pa.Table:
    """Append the two Bronze audit columns to any table."""
    n = table.num_rows
    return (
        table
        .append_column("_raw_ingested_at", pa.array([datetime.utcnow().isoformat()] * n))
        .append_column("_source_file",     pa.array([source_file] * n))
    )


def _upload_bronze(entity: str, run_date: str, table: pa.Table) -> str:
    """Upload a table to the Bronze bucket; return the MinIO object key."""
    key = f"{entity}/run_date={run_date}/{entity}.parquet"
    upload_parquet(get_minio_client(), BRONZE_BUCKET, key, table)
    logger.info("Bronze %-15s %d rows → s3://%s/%s", entity, table.num_rows, BRONZE_BUCKET, key)
    return key


# ─── Public ingestion functions ───────────────────────────────────────────────

def ingest_students(run_date: str) -> str:
    """
    Reads students.csv → bronze/students/run_date={run_date}/students.parquet
    """
    return _upload_bronze("students", run_date, _with_metadata(_read_csv_as_strings("students.csv"), "students.csv"))


def ingest_attendance(run_date: str) -> str:
    """
    Reads attendance.csv → bronze/attendance/run_date={run_date}/attendance.parquet
    """
    return _upload_bronze("attendance", run_date, _with_metadata(_read_csv_as_strings("attendance.csv"), "attendance.csv"))


def ingest_assessments(run_date: str) -> str:
    """
    Reads assessments.json → bronze/assessments/run_date={run_date}/assessments.parquet
    """
    return _upload_bronze("assessments", run_date, _with_metadata(_read_json_as_strings("assessments.json"), "assessments.json"))
