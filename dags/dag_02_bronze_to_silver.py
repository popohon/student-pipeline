"""
DAG 02 — Bronze → Silver
==========================
Reads Bronze Parquet from MinIO, validates with Great Expectations,
cleans/dedups with DuckDB SQL, and writes to MinIO Silver.

Triggered by: dag_01_ingestion (via TriggerDagRunOperator)
Schedule: None (event-driven)

Task graph:
  transform_students ─┐
  transform_attendance ─┼──► log_silver_summary ──► trigger_silver_to_gold
  transform_assessments ─┘

Key transformations per source:
  - students:    type cast, trim, normalize enrollment_status
  - attendance:  type cast + DEDUP (student_id, date) → PRESENT > ABSENT > latest
  - assessments: type cast + DEDUP (student_id, subject, date) → latest + normalized_score
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/opt/airflow")

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)


@dag(
    dag_id="dag_02_bronze_to_silver",
    description="Validate, clean, and deduplicate Bronze → Silver in MinIO",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transformation", "silver", "layer-2"],
    doc_md=__doc__,
)
def bronze_to_silver_dag():

    @task(task_id="transform_students")
    def transform_students(**context) -> dict:
        from scripts.transformation.bronze_to_silver import transform_students as _fn
        return _fn(context["ds"])

    @task(task_id="transform_attendance")
    def transform_attendance(**context) -> dict:
        from scripts.transformation.bronze_to_silver import transform_attendance as _fn
        return _fn(context["ds"])

    @task(task_id="transform_assessments")
    def transform_assessments(**context) -> dict:
        from scripts.transformation.bronze_to_silver import transform_assessments as _fn
        return _fn(context["ds"])

    @task(task_id="log_silver_summary")
    def log_silver_summary(students: dict, attendance: dict, assessments: dict) -> None:
        """Log deduplication stats and GE validation results for all three sources."""
        logger.info("=" * 60)
        logger.info("SILVER LAYER SUMMARY")
        logger.info("=" * 60)
        # All three sources now return uniform keys: records_raw, records_silver, deduplicated
        for name, result in [("students", students), ("attendance", attendance), ("assessments", assessments)]:
            logger.info(
                "%-15s %d raw → %d clean  (%d removed)  GE: %s [%s]",
                name,
                result["records_raw"],
                result["records_silver"],
                result["deduplicated"],
                "✓" if result["validation"]["success"] else "✗",
                result["validation"].get("engine", "?"),
            )

        # Warn if GE found issues (non-blocking — rejected copies already in MinIO rejected/)
        for name, result in [("students", students), ("attendance", attendance),
                              ("assessments", assessments)]:
            if not result["validation"]["success"]:
                logger.warning("GE FAILURES in %s: %s",
                               name, result["validation"]["failed_expectations"])

        logger.info("=" * 60)

    trigger = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="dag_03_silver_to_gold",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    s   = transform_students()
    a   = transform_attendance()
    asm = transform_assessments()

    log_silver_summary(s, a, asm) >> trigger


bronze_to_silver_dag()
