"""
Bronze → Silver transformation layer.

Each source goes through the same 5-step pipeline:
  1. Download bronze Parquet from MinIO
  2. Validate with Great Expectations (non-blocking)
  3. If validation fails → write copy to MinIO rejected/ for audit, continue anyway
  4. DuckDB SQL transform: cast types, trim strings, dedup, compute derived fields
  5. Upload clean Parquet to MinIO silver/

All three public functions are single-line delegations to _run_bronze_to_silver,
which encapsulates the shared pipeline. SQL queries live as module-level constants
so they're readable and testable without instantiating a DuckDB connection.
"""
import io
import logging
import os
from typing import Any, Callable, Dict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.utils.minio_client import get_minio_client, upload_parquet
from scripts.validation.ge_validations import (
    validate_students_bronze,
    validate_attendance_bronze,
    validate_assessments_bronze,
)

logger = logging.getLogger(__name__)

BRONZE_BUCKET   = os.environ.get("MINIO_BRONZE_BUCKET",   "bronze")
SILVER_BUCKET   = os.environ.get("MINIO_SILVER_BUCKET",   "silver")
REJECTED_BUCKET = os.environ.get("MINIO_REJECTED_BUCKET", "rejected")


# ─── SQL transforms (module-level constants for readability + testability) ─────

_SQL_STUDENTS = """
    SELECT
        TRIM(student_id)               AS student_id,
        TRIM(student_name)             AS student_name,
        TRIM(class_id)                 AS class_id,
        CAST(grade_level AS INTEGER)   AS grade_level,
        UPPER(TRIM(enrollment_status)) AS enrollment_status,
        CAST(updated_at AS TIMESTAMP)  AS updated_at,
        CAST(NOW() AS TIMESTAMP)       AS _ingested_at,
        'students.csv'                 AS _source,
        CAST(? AS BOOLEAN)             AS _ge_passed
    FROM raw
    WHERE student_id IS NOT NULL AND TRIM(student_id) <> ''
"""

# Dedup strategy: PRESENT > ABSENT (any positive signal wins), then latest created_at.
# This removes the 171 duplicate (student_id, attendance_date) pairs found in EDA.
_SQL_ATTENDANCE = """
    WITH ranked AS (
        SELECT
            attendance_id,
            TRIM(student_id)              AS student_id,
            CAST(attendance_date AS DATE) AS attendance_date,
            UPPER(TRIM(status))           AS status,
            CAST(created_at AS TIMESTAMP) AS created_at,
            ROW_NUMBER() OVER (
                PARTITION BY student_id, attendance_date
                ORDER BY
                    CASE WHEN UPPER(TRIM(status)) = 'PRESENT' THEN 1 ELSE 2 END ASC,
                    CAST(created_at AS TIMESTAMP) DESC
            ) AS _rn
        FROM raw
        WHERE student_id IS NOT NULL
          AND attendance_date IS NOT NULL
          AND UPPER(TRIM(status)) IN ('PRESENT', 'ABSENT')
    )
    SELECT
        attendance_id, student_id, attendance_date, status, created_at,
        CAST(NOW() AS TIMESTAMP) AS _ingested_at,
        'attendance.csv'         AS _source,
        CAST(? AS BOOLEAN)       AS _ge_passed
    FROM ranked WHERE _rn = 1
"""

# Dedup strategy: latest created_at wins.
# Adds normalized_score = score / max_score * 100.
# Filters score > max_score rows (data integrity guard).
# Removes the 15 duplicate (student_id, subject, assessment_date) triples found in EDA.
_SQL_ASSESSMENTS = """
    WITH ranked AS (
        SELECT
            assessment_id,
            TRIM(student_id)                AS student_id,
            TRIM(subject)                   AS subject,
            CAST(score AS DOUBLE)           AS score,
            CAST(max_score AS DOUBLE)       AS max_score,
            ROUND(
                CAST(score AS DOUBLE) / CAST(max_score AS DOUBLE) * 100.0, 2
            )                               AS normalized_score,
            CAST(assessment_date AS DATE)   AS assessment_date,
            CAST(created_at AS TIMESTAMP)   AS created_at,
            ROW_NUMBER() OVER (
                PARTITION BY student_id, subject, assessment_date
                ORDER BY CAST(created_at AS TIMESTAMP) DESC
            ) AS _rn
        FROM raw
        WHERE student_id IS NOT NULL
          AND assessment_date IS NOT NULL
          AND CAST(score AS DOUBLE) >= 0
          AND CAST(score AS DOUBLE) <= CAST(max_score AS DOUBLE)
    )
    SELECT
        assessment_id, student_id, subject, score, max_score, normalized_score,
        assessment_date, created_at,
        CAST(NOW() AS TIMESTAMP) AS _ingested_at,
        'assessments.json'       AS _source,
        CAST(? AS BOOLEAN)       AS _ge_passed
    FROM ranked WHERE _rn = 1
"""


# ─── Shared pipeline ────────────────────────────────────────────────────────

def _run_bronze_to_silver(
    entity: str,
    run_date: str,
    validate_fn: Callable,
    transform_sql: str,
) -> Dict[str, Any]:
    """
    Generic Bronze → Silver pipeline. Steps:
    1. Download bronze Parquet from MinIO.
    2. GE validate (non-blocking). If failed: also upload to rejected/ for audit.
    3. Register PyArrow table in DuckDB, run transform_sql, fetch result.
    4. Upload silver Parquet to MinIO.
    5. Return a uniform result dict (records_raw, records_silver, deduplicated, ...).

    The last parameter in transform_sql must be '?' (boolean ge_passed flag).
    This is automatically appended from the validation result.
    """
    client = get_minio_client()

    raw       = pq.read_table(io.BytesIO(
        client.get_object(Bucket=BRONZE_BUCKET, Key=f"{entity}/run_date={run_date}/{entity}.parquet")["Body"].read()
    ))
    validation = validate_fn(raw.to_pandas())

    if not validation["success"]:
        logger.warning("[%s] GE validation FAILED: %s", entity, validation["failed_expectations"])
        upload_parquet(client, REJECTED_BUCKET, f"{entity}/run_date={run_date}/rejected.parquet", raw)

    conn = duckdb.connect()
    conn.register("raw", raw)
    pre_count = conn.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    silver    = conn.execute(transform_sql, [validation["success"]]).fetch_arrow_table()

    upload_parquet(client, SILVER_BUCKET, f"{entity}/run_date={run_date}/{entity}.parquet", silver)

    result = {
        "records_raw":    pre_count,
        "records_silver": silver.num_rows,
        "deduplicated":   pre_count - silver.num_rows,
        "key":            f"{entity}/run_date={run_date}/{entity}.parquet",
        "validation":     validation,
    }
    logger.info("[%s] Silver: %d raw → %d clean (%d removed)",
                entity, pre_count, silver.num_rows, pre_count - silver.num_rows)
    return result


# ─── Public transform functions ──────────────────────────────────────────────

def transform_students(run_date: str) -> Dict[str, Any]:
    """Type cast + trim. No deduplication (student_id is already unique at source)."""
    return _run_bronze_to_silver("students",   run_date, validate_students_bronze,  _SQL_STUDENTS)


def transform_attendance(run_date: str) -> Dict[str, Any]:
    """Type cast + DEDUP on (student_id, date): PRESENT > ABSENT, then latest created_at."""
    return _run_bronze_to_silver("attendance", run_date, validate_attendance_bronze, _SQL_ATTENDANCE)


def transform_assessments(run_date: str) -> Dict[str, Any]:
    """Type cast + DEDUP on (student_id, subject, date): latest created_at. Adds normalized_score."""
    return _run_bronze_to_silver("assessments",run_date, validate_assessments_bronze, _SQL_ASSESSMENTS)
