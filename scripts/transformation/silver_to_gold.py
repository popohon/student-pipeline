"""
  - total_students = ACTIVE students per class from dim_students (constant per class)
  - attendance_rate = present_count / total_students  (not / students_with_attendance)
  - avg_score = mean raw score across all assessments on that date for that class
  - A (class_id, date) row exists if there is ANY attendance OR assessment activity that day
"""
import logging
import os
from datetime import datetime
from typing import Dict, Any

import duckdb

from scripts.utils.minio_client import get_minio_client, download_parquet
from scripts.utils.postgres_client import get_gold_connection, truncate_and_insert

logger = logging.getLogger(__name__)

SILVER_BUCKET = os.environ.get("MINIO_SILVER_BUCKET", "silver")


def _read_silver(client, entity: str, run_date: str):
    """Download a silver Parquet from MinIO using the shared download_parquet utility."""
    return download_parquet(client, SILVER_BUCKET, f"{entity}/run_date={run_date}/{entity}.parquet")



def load_dim_students(run_date: str) -> Dict[str, Any]:
    """Load silver students into gold.dim_students."""
    client = get_minio_client()
    students = _read_silver(client, "students", run_date)

    conn = duckdb.connect()
    conn.register("silver_students", students)

    df = conn.execute("""
        SELECT
            student_id,
            student_name,
            class_id,
            grade_level,
            enrollment_status,
            updated_at,
            _ingested_at
        FROM silver_students
        ORDER BY student_id
    """).fetchdf()

    gold_conn = get_gold_connection()
    rows = truncate_and_insert(gold_conn, "gold", "dim_students", df)
    gold_conn.close()

    logger.info("dim_students loaded: %d rows", rows)
    return {"table": "dim_students", "rows": rows}



def load_fact_attendance(run_date: str) -> Dict[str, Any]:
    """
    Load silver attendance into gold.fact_attendance.
    Enriches with class_id via JOIN on silver students.
    """
    client = get_minio_client()
    attendance = _read_silver(client, "attendance", run_date)
    students   = _read_silver(client, "students",   run_date)

    conn = duckdb.connect()
    conn.register("silver_att", attendance)
    conn.register("silver_stu", students)

    df = conn.execute("""
        SELECT
            a.attendance_id,
            a.student_id,
            s.class_id,
            a.attendance_date,
            a.status,
            a.created_at,
            a._ingested_at
        FROM silver_att  a
        LEFT JOIN silver_stu s ON a.student_id = s.student_id
        ORDER BY a.attendance_date, a.student_id
    """).fetchdf()

    gold_conn = get_gold_connection()
    rows = truncate_and_insert(gold_conn, "gold", "fact_attendance", df)
    gold_conn.close()

    logger.info("fact_attendance loaded: %d rows", rows)
    return {"table": "fact_attendance", "rows": rows}


def load_fact_assessments(run_date: str) -> Dict[str, Any]:
    """
    Load silver assessments into gold.fact_assessments.
    Enriches with class_id via JOIN on silver students.
    """
    client = get_minio_client()
    assessments = _read_silver(client, "assessments", run_date)
    students    = _read_silver(client, "students",    run_date)

    conn = duckdb.connect()
    conn.register("silver_asm", assessments)
    conn.register("silver_stu", students)

    df = conn.execute("""
        SELECT
            a.assessment_id,
            a.student_id,
            s.class_id,
            a.subject,
            ROUND(a.score::DOUBLE, 2)             AS score,
            ROUND(a.max_score::DOUBLE, 2)         AS max_score,
            ROUND(a.normalized_score::DOUBLE, 2)  AS normalized_score,
            a.assessment_date,
            a.created_at,
            a._ingested_at
        FROM silver_asm a
        LEFT JOIN silver_stu s ON a.student_id = s.student_id
        ORDER BY a.assessment_date, a.student_id
    """).fetchdf()

    gold_conn = get_gold_connection()
    rows = truncate_and_insert(gold_conn, "gold", "fact_assessments", df)
    gold_conn.close()

    logger.info("fact_assessments loaded: %d rows", rows)
    return {"table": "fact_assessments", "rows": rows}


def compute_class_daily_performance() -> Dict[str, Any]:
    """
    Compute the class daily performance mart from Gold fact tables.

    SQL logic:
    1. class_students: ACTIVE student count per class (total_students)
    2. attendance_agg: per (class_id, date) attendance stats
    3. assessment_agg: per (class_id, date) assessment stats
    4. all_dates: UNION of all active (class_id, date) combinations
    5. Final SELECT: LEFT JOIN all CTEs, compute attendance_rate

    attendance_rate = present_count / total_students
    (students with no attendance record on a given day count against the rate)
    """
    gold_conn = get_gold_connection()
    computed_at = datetime.utcnow()

    mart_sql = """
        WITH
        class_students AS (
            SELECT
                class_id,
                COUNT(DISTINCT student_id) AS total_students
            FROM gold.dim_students
            WHERE enrollment_status = 'ACTIVE'
            GROUP BY class_id
        ),
        attendance_agg AS (
            SELECT
                class_id,
                attendance_date                                             AS date,
                COUNT(DISTINCT student_id)                                  AS students_with_attendance,
                SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END)        AS present_count,
                SUM(CASE WHEN status = 'ABSENT'  THEN 1 ELSE 0 END)        AS absent_count
            FROM gold.fact_attendance
            GROUP BY class_id, attendance_date
        ),
        assessment_agg AS (
            SELECT
                class_id,
                assessment_date                                             AS date,
                COUNT(DISTINCT student_id)                                  AS students_with_assessment,
                COUNT(*)                                                    AS assessment_count,
                ROUND(AVG(score)::NUMERIC, 2)                              AS avg_score
            FROM gold.fact_assessments
            GROUP BY class_id, assessment_date
        ),
        all_dates AS (
            SELECT class_id, date FROM attendance_agg
            UNION
            SELECT class_id, date FROM assessment_agg
        )
        INSERT INTO gold.mart_class_daily_performance (
            class_id, date,
            total_students,
            students_with_attendance, present_count, absent_count, attendance_rate,
            students_with_assessment, assessment_count, avg_score,
            _computed_at
        )
        SELECT
            cd.class_id,
            cd.date,
            cs.total_students,
            COALESCE(aa.students_with_attendance, 0),
            COALESCE(aa.present_count,            0),
            COALESCE(aa.absent_count,             0),
            ROUND(
                CAST(COALESCE(aa.present_count, 0) AS NUMERIC)
                / NULLIF(cs.total_students, 0),
                4
            )                                                               AS attendance_rate,
            COALESCE(asm.students_with_assessment, 0),
            COALESCE(asm.assessment_count,         0),
            COALESCE(asm.avg_score,                0.00),
            %(computed_at)s
        FROM all_dates cd
        JOIN  class_students cs  ON cd.class_id = cs.class_id
        LEFT JOIN attendance_agg  aa  ON cd.class_id = aa.class_id  AND cd.date = aa.date
        LEFT JOIN assessment_agg  asm ON cd.class_id = asm.class_id AND cd.date = asm.date
        ON CONFLICT (class_id, date) DO UPDATE SET
            total_students            = EXCLUDED.total_students,
            students_with_attendance  = EXCLUDED.students_with_attendance,
            present_count             = EXCLUDED.present_count,
            absent_count              = EXCLUDED.absent_count,
            attendance_rate           = EXCLUDED.attendance_rate,
            students_with_assessment  = EXCLUDED.students_with_assessment,
            assessment_count          = EXCLUDED.assessment_count,
            avg_score                 = EXCLUDED.avg_score,
            _computed_at              = EXCLUDED._computed_at
    """

    with gold_conn.cursor() as cur:
        cur.execute(mart_sql, {"computed_at": computed_at})
        rows = cur.rowcount
    gold_conn.commit()
    gold_conn.close()

    logger.info("mart_class_daily_performance upserted: %d rows", rows)
    return {"table": "mart_class_daily_performance", "rows": rows}
