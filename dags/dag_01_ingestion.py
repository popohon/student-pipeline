"""
DAG 01 — Ingestion (Source → Bronze)
=====================================
Reads raw source files from /opt/airflow/data and uploads them as Parquet
to the MinIO Bronze layer, partitioned by Airflow logical date (ds).

Schedule: @daily  (paused by default — trigger manually or via make trigger-pipeline)

Task graph:
  ingest_students ─┐
  ingest_attendance ─┼──► trigger_bronze_to_silver
  ingest_assessments ─┘

All three ingest tasks run in parallel, then trigger DAG 02.
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
    dag_id="dag_01_ingestion",
    description="Ingest raw source files (CSV/JSON) to MinIO Bronze as Parquet",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze", "layer-1"],
    doc_md=__doc__,
)
def ingestion_dag():

    @task(task_id="ingest_students")
    def ingest_students(**context) -> str:
        from scripts.ingestion.ingest_sources import ingest_students as _fn
        key = _fn(context["ds"])
        logger.info("Ingested students → %s", key)
        return key

    @task(task_id="ingest_attendance")
    def ingest_attendance(**context) -> str:
        from scripts.ingestion.ingest_sources import ingest_attendance as _fn
        key = _fn(context["ds"])
        logger.info("Ingested attendance → %s", key)
        return key

    @task(task_id="ingest_assessments")
    def ingest_assessments(**context) -> str:
        from scripts.ingestion.ingest_sources import ingest_assessments as _fn
        key = _fn(context["ds"])
        logger.info("Ingested assessments → %s", key)
        return key

    trigger = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="dag_02_bronze_to_silver",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # All three ingest tasks are independent — run in parallel
    [ingest_students(), ingest_attendance(), ingest_assessments()] >> trigger


ingestion_dag()
