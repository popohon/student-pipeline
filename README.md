# Student Data Pipeline

End-to-end data pipeline ingesting student, attendance, and assessment data through a **Bronze → Silver → Gold** medallion architecture, fully containerised with Docker.

---
## Architecture Summary and Thought Process
For the thought process please use this [link on notion](https://www.notion.so/THOUGHT-PROCESS-32f70af3cc858027bc68e48e7bec2cc4?source=copy_link)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Source Layer                              │
│    students.csv    │  assessments.json  │  attendance.csv       │
└──────────────────────────────┬──────────────────────────────────┘
                               │  DAG 01 — Ingestion
                               │  (Full load → Parquet, no type casting)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  MinIO — Bronze Layer                           │
│  bronze/students/run_date=YYYY-MM-DD/students.parquet           │
│  bronze/attendance/run_date=YYYY-MM-DD/attendance.parquet       │
│  bronze/assessments/run_date=YYYY-MM-DD/assessments.parquet     │
└──────────────────────────────┬──────────────────────────────────┘
                               │  DAG 02 — Bronze → Silver
                               │  (GE validate → DuckDB SQL clean/dedup)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  MinIO — Silver Layer                           │
│  Typed, deduplicated, validated Parquet                         │
│  silver/students/ │ silver/attendance/ │ silver/assessments/    │
└──────────────────────────────┬──────────────────────────────────┘
                               │  DAG 03 — Silver → Gold
                               │  (Join, aggregate → PostgreSQL)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              PostgreSQL — Gold Layer (schema: gold)             │
│  gold.dim_students                                              │
│  gold.fact_attendance          (enriched with class_id)         │
│  gold.fact_assessments         (enriched + normalized_score)    │
│  gold.mart_class_daily_performance                              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Apache Superset                               │
│             Class Performance Dashboard                         │
└─────────────────────────────────────────────────────────────────┘
```

### Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| PostgreSQL | localhost:5432 | gold / gold (gold_db) |

> **Jupyter / EDA notebook** runs locally outside Docker — see the [EDA Notebook](#eda-notebook) section below.

### Transformation Details

| Layer | Source | Records In | Records Out | Key Operations |
|-------|--------|-----------|-------------|----------------|
| Bronze | students.csv | 1,200 | 1,200 | Raw → Parquet, add metadata |
| Bronze | attendance.csv | 5,000 | 5,000 | Raw → Parquet (dupes preserved) |
| Bronze | assessments.json | 3,000 | 3,000 | Raw → Parquet |
| Silver | students | 1,200 | 1,200 | Type cast, trim, normalize |
| Silver | attendance | 5,000 | **4,829** | Dedup 171 rows (PRESENT>ABSENT, latest) |
| Silver | assessments | 3,000 | **2,985** | Dedup 15 rows (latest created_at), +normalized_score |
| Gold | attendance+students | 4,829 | — | Enrich class_id via JOIN |
| Gold | mart | — | ~N×10 | Aggregate class×date performance |

---

## Prerequisites

- Docker Desktop ≥ 4.x
- Docker Compose v2 (`docker compose`)
- ~4 GB RAM allocated to Docker
- Make (optional but recommended)

---

## Quick Start

```bash
# 1. Clone / unzip the project
cd onlenpajak

# 2. Start all services (builds Airflow + Superset images on first run — ~5 min)
make up
# or: docker compose up -d --build

# 3. Wait for services to be healthy (~60s), then run the pipeline
make unpause-dags
make trigger-pipeline

# 4. Monitor pipeline at http://localhost:8080 (admin/admin)
#    Pipeline runs: DAG 01 → DAG 02 → DAG 03 automatically

# 5. After all 3 DAGs complete, set up the Superset dashboard
make setup-superset

# 6. View dashboard at http://localhost:8088 (admin/admin)
```

---

## Detailed Commands

```bash
make up                # Start all services
make down              # Stop (preserve data volumes)
make reset             # Full reset (delete all volumes)
make status            # Show container health

make trigger-pipeline  # Trigger DAG 01 (kicks off full pipeline)
make trigger-ingestion # Trigger only DAG 01
make trigger-silver    # Trigger only DAG 02
make trigger-gold      # Trigger only DAG 03

make setup-superset    # Create dashboard in Superset
make psql-gold         # psql session on gold_db
make query-mart        # Query mart_class_daily_performance
make minio-ls          # List MinIO buckets
make logs              # Tail all logs
```

---

## Project Structure

```
onlenpajak/
├── docker-compose.yml                  # All services
├── .env                                # Environment variables
├── Makefile                            # Convenience commands
├── data/                               # Source files (CSV/JSON)
├── docker/
│   ├── airflow/
│   │   ├── Dockerfile                  # Extends apache/airflow:2.9.3
│   │   └── requirements.txt            # duckdb, great-expectations, boto3, etc.
│   └── superset/
│       ├── Dockerfile                  # Extends apache/superset:3.1.0
│       ├── superset_config.py
│       ├── init_superset.sh
│       └── dashboard/
│           └── setup_dashboard.py      # Automated dashboard creation
├── dags/
│   ├── dag_01_ingestion.py             # Source → Bronze
│   ├── dag_02_bronze_to_silver.py      # Bronze → Silver (validate + dedup)
│   └── dag_03_silver_to_gold.py        # Silver → Gold (joins + mart)
├── scripts/
│   ├── utils/
│   │   ├── minio_client.py             # boto3 MinIO wrapper
│   │   └── postgres_client.py          # psycopg2 Gold DB wrapper
│   ├── ingestion/
│   │   └── ingest_sources.py           # CSV/JSON → Parquet upload
│   ├── transformation/
│   │   ├── bronze_to_silver.py         # DuckDB SQL transformations
│   │   └── silver_to_gold.py           # PostgreSQL loads + mart SQL
│   └── validation/
│       └── ge_validations.py           # Great Expectations suites
├── sql/
│   ├── init/
│   │   └── 01_init_databases.sh        # PostgreSQL bootstrap
│   └── gold/
│       └── schema.sql                  # Gold schema reference
├── notebooks/
│   └── 01_eda.ipynb                    # Exploratory Data Analysis (run locally, not in Docker)
├── README.md
├── THOUGHT_PROCESS.md                  # Architecture decisions, code review, tool rationale
└── LAKEHOUSE_FUTURE.md                 # Future scaling with Iceberg + Trino
```

---

## Data Validation (Great Expectations)

GE runs as part of DAG 02 (`dag_02_bronze_to_silver`). If validation fails:
- Pipeline **does NOT stop** (non-blocking)
- Failed raw data is copied to `s3://rejected/` in MinIO
- Failures are logged in Airflow task logs and the `log_silver_summary` task

### Expectations per source

**students**: `student_id` not null + unique + matches `S-\d{4}`, `enrollment_status` in known set, `class_id` matches `CLASS-\d{2}`

**attendance**: `attendance_id` not null + unique, `status` in `[PRESENT, ABSENT]`, `student_id` matches pattern

**assessments**: `assessment_id` not null + unique, `subject` in known set, `score` not null

### Where GE runs in the pipeline

GE runs inside **DAG 02**, step 2 of 5, between downloading Bronze and running the DuckDB transformation:

```
Bronze (MinIO)
    └─① Download raw Parquet
    └─② GE validate  ← HERE
          ├── PASS → continue
          └── FAIL → copy to rejected/ AND continue (non-blocking)
    └─③ DuckDB SQL (cast, dedup, normalize)
    └─④ Upload clean Parquet to Silver (MinIO)
    └─⑤ Return result dict with validation outcome
```

All 3 sources are validated in parallel tasks (`transform_students`, `transform_attendance`,
`transform_assessments`), then results are consolidated in `log_silver_summary`.

### Monitoring GE results — 4 places

#### 1. Airflow UI → `log_silver_summary` task (quickest)

Open http://localhost:8080 → `dag_02_bronze_to_silver` → click a run → click
`log_silver_summary` → **Logs**. You will see a summary table for all 3 sources:

```
============================================================
SILVER LAYER SUMMARY
============================================================
students        1200 raw → 1200 clean  (0 removed)    GE: ✓ [great_expectations]
attendance      5000 raw → 4829 clean  (171 removed)  GE: ✓ [great_expectations]
assessments     3000 raw → 2985 clean  (15 removed)   GE: ✓ [great_expectations]
============================================================
```

If a source fails, `✗` appears and the specific failed expectation names are listed below.

#### 2. Airflow UI → individual transform task logs (most detailed)

Click `transform_attendance` (or students/assessments) → **Logs**.
GE warnings appear immediately after validation runs:

```
WARNING [scripts.transformation.bronze_to_silver] [attendance] GE validation FAILED:
['status_valid', 'attendance_id_unique']
```

This is the most granular view — you see exactly which expectation broke and the engine used
(`great_expectations`, `fallback`, or `error`).

#### 3. MinIO Console → `rejected/` bucket (inspect bad data)

Open http://localhost:9001 → browse bucket **`rejected`**.

If any source fails GE, its **raw Bronze Parquet is copied here unchanged** so you can
download and inspect the exact rows that triggered the failure:

```
rejected/
  attendance/run_date=YYYY-MM-DD/rejected.parquet   ← unmodified raw rows
  assessments/run_date=YYYY-MM-DD/rejected.parquet
```

Inspect locally with:
```bash
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('rejected.parquet')
print(table.to_pandas())
"
```

#### 4. Silver Parquet → `_ge_passed` column (audit trail in data)

Every row written to Silver carries a `_ge_passed` boolean column set to the validation
result of that batch. This creates an **in-data audit trail** — downstream queries can
always tell which batches were clean vs problematic:

```sql
-- Check which Silver batches had GE failures (run via DuckDB locally or psql after loading)
SELECT _source, _ge_passed, COUNT(*) AS rows
FROM read_parquet('silver/attendance/run_date=.../attendance.parquet')
GROUP BY _source, _ge_passed;
```

### GE monitoring summary

| What you want | Where to look |
|---|---|
| Quick pass/fail across all 3 sources | Airflow → `log_silver_summary` logs |
| Which specific expectation failed | Airflow → `transform_*` task logs |
| The actual bad data rows | MinIO Console → `rejected/` bucket |
| Per-row audit trail in Silver | `_ge_passed` column in Silver Parquet |
| GE is blocking the pipeline | It never does — GE is **non-blocking** by design |

---

## Gold Layer Schema

### mart_class_daily_performance

| Column | Type | Description |
|--------|------|-------------|
| class_id | VARCHAR | e.g. CLASS-01 |
| date | DATE | Activity date |
| total_students | INT | ACTIVE students in class |
| students_with_attendance | INT | Distinct students with any record that day |
| present_count | INT | COUNT of PRESENT records |
| absent_count | INT | COUNT of ABSENT records |
| attendance_rate | NUMERIC(5,4) | present_count / total_students |
| students_with_assessment | INT | Distinct students with any assessment that day |
| assessment_count | INT | Total assessments taken |
| avg_score | NUMERIC(6,2) | Mean raw score (not normalized) |

---

## SQL Examples

After running the pipeline, connect to the Gold DB and run these queries.

```bash
make psql-gold     # Opens a psql session on gold_db
make query-mart    # Quick top-20 mart preview
```

### Class daily performance (the primary mart)

```sql
-- Full mart ordered by class and date
SELECT *
FROM gold.mart_class_daily_performance
ORDER BY class_id, date;

-- Average attendance rate per class (all time)
SELECT
    class_id,
    ROUND(AVG(attendance_rate) * 100, 2)  AS avg_attendance_pct,
    ROUND(AVG(avg_score), 2)              AS avg_score
FROM gold.mart_class_daily_performance
GROUP BY class_id
ORDER BY class_id;

-- Classes with the lowest attendance rate on a specific date
SELECT class_id, date, present_count, total_students, attendance_rate
FROM gold.mart_class_daily_performance
WHERE date = '2025-01-10'
ORDER BY attendance_rate ASC;

-- Daily trend for a single class
SELECT date, present_count, absent_count, attendance_rate, avg_score
FROM gold.mart_class_daily_performance
WHERE class_id = 'CLASS-01'
ORDER BY date;
```

### Students

```sql
-- Students per class and grade
SELECT class_id, grade_level, COUNT(*) AS students
FROM gold.dim_students
GROUP BY class_id, grade_level
ORDER BY class_id, grade_level;
```

### Assessment facts

```sql
-- Average score by subject
SELECT subject, ROUND(AVG(score), 2) AS avg_score, COUNT(*) AS assessments
FROM gold.fact_assessments
GROUP BY subject
ORDER BY avg_score DESC;

-- Top 5 students by average score
SELECT fa.student_id, ds.class_id, ROUND(AVG(fa.score), 2) AS avg_score
FROM gold.fact_assessments fa
JOIN gold.dim_students ds ON fa.student_id = ds.student_id
GROUP BY fa.student_id, ds.class_id
ORDER BY avg_score DESC
LIMIT 5;
```

### Attendance facts

```sql
-- Attendance records on a specific date with student details
SELECT a.student_id, s.student_name, a.class_id, a.status
FROM gold.fact_attendance a
JOIN gold.dim_students s ON a.student_id = s.student_id
WHERE a.attendance_date = '2025-01-10'
ORDER BY a.class_id, a.student_id;
```

---

## EDA Notebook

The notebook runs **locally**, not inside Docker. To run it:

```bash
# Install dependencies (once)
pip install pandas pyarrow matplotlib seaborn jupyter

# Launch Jupyter Lab
jupyter lab notebooks/
```

Then open `01_eda.ipynb`. Data files are read from `./data/` (relative to the project root).

Covers: dataset profiling, data quality checks (171 attendance dupes, 15 assessment dupes),
cross-dataset analysis, score heatmap by class × subject, and pipeline readiness checklist.

---

## Future Scaling

See [LAKEHOUSE_FUTURE.md](LAKEHOUSE_FUTURE.md) for the migration path when data volume grows significantly.
