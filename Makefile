.PHONY: up down reset logs trigger-pipeline setup-superset psql-gold minio-console status

# ─────────────────────────────────────────────────────────────
# Service Management
# ─────────────────────────────────────────────────────────────

## Start all services (build images if needed)
up:
	docker compose up -d --build
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Services starting. Wait ~60s, then visit:"
	@echo "  Airflow   → http://localhost:8080  (admin/admin)"
	@echo "  Superset  → http://localhost:8088  (admin/admin)"
	@echo "  MinIO     → http://localhost:9001  (minioadmin/minioadmin123)"
	@echo "  (EDA notebook runs locally — see README.md for instructions)"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

## Stop all services (preserve volumes)
down:
	docker compose down

## Full reset: stop services and delete all volumes (fresh start)
reset:
	docker compose down -v --remove-orphans
	@echo "All volumes removed. Run 'make up' to start fresh."

## Show running container status
status:
	docker compose ps

# ─────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────

## Trigger the full pipeline (ingestion → silver → gold)
trigger-pipeline:
	docker compose exec airflow-webserver airflow dags trigger dag_01_ingestion
	@echo "Pipeline triggered. Monitor at http://localhost:8080"

## Trigger only ingestion DAG
trigger-ingestion:
	docker compose exec airflow-webserver airflow dags trigger dag_01_ingestion

## Trigger only bronze→silver DAG
trigger-silver:
	docker compose exec airflow-webserver airflow dags trigger dag_02_bronze_to_silver

## Trigger only silver→gold DAG
trigger-gold:
	docker compose exec airflow-webserver airflow dags trigger dag_03_silver_to_gold

## Unpause all DAGs
unpause-dags:
	docker compose exec airflow-webserver airflow dags unpause dag_01_ingestion
	docker compose exec airflow-webserver airflow dags unpause dag_02_bronze_to_silver
	docker compose exec airflow-webserver airflow dags unpause dag_03_silver_to_gold

# ─────────────────────────────────────────────────────────────
# Superset Dashboard
# ─────────────────────────────────────────────────────────────

## Run the automated dashboard setup (after pipeline completes)
setup-superset:
	docker compose exec superset python /app/docker/dashboard/setup_dashboard.py
	@echo "Dashboard ready at http://localhost:8088"

# ─────────────────────────────────────────────────────────────
# Debugging & Inspection
# ─────────────────────────────────────────────────────────────

## Open psql session on the Gold DB
psql-gold:
	docker compose exec postgres psql -U gold -d gold_db

## Open psql session on Airflow DB
psql-airflow:
	docker compose exec postgres psql -U airflow -d airflow_db

## Tail Airflow scheduler logs
logs-scheduler:
	docker compose logs -f airflow-scheduler

## Tail Airflow webserver logs
logs-webserver:
	docker compose logs -f airflow-webserver

## Tail all logs
logs:
	docker compose logs -f

## List MinIO buckets and objects
minio-ls:
	docker compose exec minio mc ls local/bronze
	docker compose exec minio mc ls local/silver
	docker compose exec minio mc ls local/rejected

## Query the Gold mart table
query-mart:
	docker compose exec postgres psql -U gold -d gold_db \
	  -c "SELECT * FROM gold.mart_class_daily_performance ORDER BY class_id, date LIMIT 20;"
