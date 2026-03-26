"""
DAG 03 — Silver → Gold
=======================
Loads Silver Parquet from MinIO into PostgreSQL Gold schema.

Triggered by: dag_02_bronze_to_silver (via TriggerDagRunOperator)
Schedule: None (event-driven)

Task graph:
  load_dim_students ─┐
  load_fact_attendance ─┼──► compute_class_daily_performance ──► log_gold_summary
  load_fact_assessments ─┘

Load strategy: TRUNCATE + INSERT (idempotent full reload, safe for daily batch).
The mart (class_daily_performance) uses INSERT … ON CONFLICT DO UPDATE.

Tables populated:
  gold.dim_students
  gold.fact_attendance           (enriched with class_id)
  gold.fact_assessments          (enriched with class_id + normalized_score)
  gold.mart_class_daily_performance
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/opt/airflow")

import logging
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="dag_03_silver_to_gold",
    description="Load Silver Parquet → PostgreSQL Gold (dims, facts, mart)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gold", "serving", "layer-3"],
    doc_md=__doc__,
)
def silver_to_gold_dag():

    @task(task_id="load_dim_students")
    def load_dim_students(**context) -> dict:
        from scripts.transformation.silver_to_gold import load_dim_students as _fn
        return _fn(context["ds"])

    @task(task_id="load_fact_attendance")
    def load_fact_attendance(**context) -> dict:
        from scripts.transformation.silver_to_gold import load_fact_attendance as _fn
        return _fn(context["ds"])

    @task(task_id="load_fact_assessments")
    def load_fact_assessments(**context) -> dict:
        from scripts.transformation.silver_to_gold import load_fact_assessments as _fn
        return _fn(context["ds"])

    @task(task_id="compute_class_daily_performance")
    def compute_class_daily_performance(
        dim_result: dict, att_result: dict, asm_result: dict
    ) -> dict:
        from scripts.transformation.silver_to_gold import compute_class_daily_performance as _fn
        return _fn()

    @task(task_id="log_gold_summary")
    def log_gold_summary(
        dim_result: dict, att_result: dict, asm_result: dict, mart_result: dict
    ) -> None:
        logger.info("=" * 60)
        logger.info("GOLD LAYER SUMMARY")
        logger.info("=" * 60)
        logger.info("dim_students:                 %d rows", dim_result["rows"])
        logger.info("fact_attendance:              %d rows", att_result["rows"])
        logger.info("fact_assessments:             %d rows", asm_result["rows"])
        logger.info("mart_class_daily_performance: %d rows", mart_result["rows"])
        logger.info("=" * 60)
        logger.info("Pipeline complete. Dashboard available at http://localhost:8088")

    dim = load_dim_students()
    att = load_fact_attendance()
    asm = load_fact_assessments()

    # Mart depends on all three fact/dim tables being loaded first
    mart = compute_class_daily_performance(dim, att, asm)

    log_gold_summary(dim, att, asm, mart)


silver_to_gold_dag()
