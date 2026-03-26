# Thought Process — Student Data Pipeline

This document is a handholding walkthrough of every decision made in this pipeline:
why each tool was chosen, why each layer exists, what transformations were applied and why,
and what the code review found and changed.

Read this alongside the code. Every section explains the **why** behind a specific file or choice.

---

## Table of Contents

1. [The Problem Statement](#1-the-problem-statement)
2. [Design Principles](#2-design-principles)
3. [Why Three Layers? (Bronze / Silver / Gold)](#3-why-three-layers)
4. [Why Parquet? (Not CSV, JSON, or Avro)](#4-why-parquet)
5. [Why MinIO for Bronze and Silver?](#5-why-minio)
6. [Why PostgreSQL for Gold? (Not MinIO All the Way)](#6-why-postgresql-for-gold)
7. [Why DuckDB for Transformations?](#7-why-duckdb)
8. [Why Airflow for Orchestration?](#8-why-airflow)
9. [Why Great Expectations for Validation?](#9-why-great-expectations)
10. [Why Apache Superset for Dashboards?](#10-why-superset)
11. [Data Quality Findings and What We Did About Them](#11-data-quality-findings)
12. [Transformation Decisions — Layer by Layer](#12-transformation-decisions)
13. [Gold Layer Data Model Design](#13-gold-layer-data-model)
14. [Code Review Findings and Refactors Applied](#14-code-review)
15. [Known Limitations and Technical Debt](#15-known-limitations)
16. [What "Future Lakehouse" Means for This Project](#16-future-lakehouse)

---

## 1. The Problem Statement

We have three source files:
- `students.csv` — 1,200 rows, one row per student, static snapshot
- `attendance.csv` — 5,000 rows, one row per attendance event
- `assessments.json` — 3,000 rows, one row per assessment submission

The goal is to produce an analytics-ready table (`mart_class_daily_performance`) that answers:
> For each class, on each day: how many students attended? What was their average score?

This seems simple on the surface, but doing it **reliably at scale** requires thinking carefully
about data quality, transformation separation, auditability, and operability.

---

## 2. Design Principles

Before any tool was chosen, three principles were set:

**Principle 1: Separation of concerns per layer.**
Raw data should never be mixed with clean data. Validation should never be mixed with
transformation. Aggregation should never be mixed with fact storage.
This is why three layers exist instead of one big ETL script.

**Principle 2: Immutability of raw data.**
Once ingested, Bronze data is never modified. If a bug is found in the Silver
transformation, the Bronze file is still there. You can re-run Silver from Bronze without
re-fetching from the source.

**Principle 3: Idempotency.**
Every DAG run produces the same result when run again with the same input.
This is achieved through full-load TRUNCATE + INSERT at Gold (not delta/append)
and date-partitioned Bronze/Silver paths (so re-runs overwrite, not duplicate).

---

## 3. Why Three Layers?

**The single-script alternative:** You could read CSV → clean → aggregate → write to database
in one Python script. This seems simpler. Here's why it's actually worse:

| Concern | Single script | Three layers |
|---|---|---|
| Debugging a bad aggregation | Re-read source, re-clean, re-aggregate | Just re-run Gold from Silver |
| Auditing "what did the raw data look like?" | Gone | Preserved in Bronze forever |
| Source system changes a field type | Breaks everything silently | Breaks at Silver, Bronze intact |
| Validation failure on one source | Stops everything | Other sources proceed normally |
| Replay after a bug fix | Have to re-fetch from source | Just re-run from the broken layer |

### Bronze — Raw As-Is

Bronze's only job is to get data from the source into Parquet format, unchanged.
No type casting, no filtering, no deduplication. Even the 171 duplicate attendance rows
are preserved here intentionally — the duplicates are a fact about the source, and an
auditor might want to see them.

The Bronze files are partitioned by `run_date`:
```
bronze/attendance/run_date=2025-01-10/attendance.parquet
```
This means each daily run writes a new partition. Historical runs are preserved.

### Silver — Clean and Trusted

Silver's job is to make data trustworthy. This means:
- Cast every column to its correct type (strings → dates, integers, timestamps)
- Normalize strings (TRIM whitespace, UPPER enrollment_status)
- Deduplicate using explicit, documented business rules
- Add `normalized_score` as a derived field (Silver is where derivations happen)
- Flag rows that failed GE validation with `_ge_passed = false`

Silver is still source-faithful — it doesn't join tables, doesn't aggregate, and doesn't
drop records except exact duplicates.

### Gold — Analytics-Ready

Gold's job is to serve dashboards and analysts. This means:
- JOIN attendance/assessments with students to enrich `class_id` (denormalization for query speed)
- TRUNCATE + INSERT for full idempotent reload
- Compute `mart_class_daily_performance` — the primary analytics table
- Everything is in PostgreSQL so Superset can query it with standard SQL

Gold is designed to be read-heavy. Indexes are added on every commonly filtered/joined column.

---

## 4. Why Parquet?

Every Bronze and Silver file is stored as Parquet. Here's why that matters.

### The alternative: keep CSV/JSON as-is

If we kept attendance as CSV in MinIO and then read it for transformation, we'd have to:
1. Parse CSV on every read (CPU cost)
2. Infer or manually specify types on every read
3. Handle encoding issues, quoting edge cases, BOM characters
4. Download the full file every time (no column pruning possible)

### What Parquet gives you

**Columnar storage.** CSV is row-oriented — to read `attendance_date` from 5,000 rows,
you read all 5,000 full rows. Parquet is column-oriented — you read only the `attendance_date`
column bytes. At 5,000 rows this is irrelevant. At 50 million rows, it's the difference
between 3 seconds and 3 minutes.

**Embedded schema.** The Parquet file knows its column names and types. When you read it back,
`attendance_date` comes back as `DATE`, not as a string that might be `"2025-01-01"` or
`"01/01/2025"` depending on locale.

**Snappy compression.** CSV has no compression (unless you gzip it separately).
A 100MB CSV typically compresses to 15–20MB as Parquet with Snappy.
This means faster downloads from MinIO and lower storage costs.

**Null safety.** CSV doesn't distinguish between empty string `""` and missing value.
Parquet has a native null type.

### Why not Avro or ORC?

- **Avro** is row-based (like CSV) — better for streaming/Kafka where you need record-level
  access. Not ideal for analytical batch queries.
- **ORC** is columnar (like Parquet) but is tightly coupled to the Hive/HDFS ecosystem.
  Parquet is more widely supported (DuckDB, Spark, BigQuery, Athena, Redshift all read it natively).

### Why not Delta Lake / Iceberg format?

Delta/Iceberg add ACID transaction support and time-travel on top of Parquet.
For this project (static source files, daily batch, ~8,000 total rows), the overhead isn't
worth it. The LAKEHOUSE_FUTURE.md document describes when to migrate.

---

## 5. Why MinIO?

MinIO is an S3-compatible object storage server that runs entirely in Docker.
It is to S3 what Postgres is to Aurora — same API, self-hosted.

### What MinIO gives you

**S3-compatible API.** Every tool that can talk to AWS S3 can talk to MinIO without
code changes — just point the endpoint URL at `http://minio:9000` instead of
`https://s3.amazonaws.com`. This includes `boto3`, `duckdb` (via httpfs), `spark` (via s3a),
and all others.

**Object storage semantics.** Unlike a local filesystem, MinIO:
- Stores files by key (path), not directory hierarchy (even though it looks like one)
- Is accessible over HTTP from any container in the Docker network
- Supports bucket-level policies, versioning, and lifecycle rules when needed
- Has a built-in web console at port 9001

**Portability.** When this project moves to production on AWS or GCP, replacing MinIO with
S3 or GCS requires changing one environment variable (`MINIO_ENDPOINT`), not the code.

### Why not a local Docker volume?

Local volumes are not accessible over HTTP. DuckDB's httpfs extension, any Spark job,
and any other container would need filesystem mounts instead. MinIO gives each container
its own clean S3 client connection — no mount management needed.

### Why not a "real" data warehouse (Snowflake, BigQuery, Redshift)?

The rule for this project: **local Docker only, no cloud dependencies.**
MinIO + PostgreSQL replaces S3 + Redshift/Athena for local development.

---

## 6. Why PostgreSQL for Gold? (Not MinIO All the Way)

**Challenge:** Your first instinct might be to put Gold in MinIO too — just more Parquet files.
Why introduce PostgreSQL?

**Answer:** MinIO stores files. It does not execute SQL. Superset cannot connect to MinIO.
To serve dashboards, you need a SQL engine that Superset understands.

Options evaluated:

**DuckDB (embedded):**
- ✅ Reads Parquet from MinIO directly via S3 extension
- ✅ Very fast for analytical queries
- ❌ No persistent server — each process gets its own in-memory state
- ❌ Superset has no native DuckDB connector (community plugin, not production-grade)
- ❌ Concurrent reads from multiple Superset users would spin up separate DuckDB instances

**Trino (distributed query engine):**
- ✅ Full SQL on Parquet in MinIO via Iceberg connector
- ✅ Superset has a native Trino connector
- ❌ Needs Hive Metastore or Nessie catalog — 2 extra Docker services
- ❌ Cold startup takes 20+ seconds
- ❌ Overkill for 8,000 rows

**PostgreSQL:**
- ✅ Native Superset connector (zero configuration)
- ✅ Persistent, concurrent-safe
- ✅ Already needed for Airflow metadata — reuse the same instance with separate databases
- ✅ Full SQL including window functions (which the mart computation uses)
- ❌ Row-oriented — not columnar (fine at this data volume)
- ❌ Not a data lake — no schema evolution without migrations

**Decision:** PostgreSQL as the Gold layer serving database. It's pragmatic, well-understood,
and directly connectable by Superset. For the data volumes in this project, it's ideal.

---

## 7. Why DuckDB?

DuckDB is used in the Bronze→Silver and Silver→Gold transformations. Here's the reasoning.

### The pandas alternative

You could do all the Silver transformations in pandas:
```python
df['attendance_date'] = pd.to_datetime(df['attendance_date'])
df = df.sort_values(...).drop_duplicates(...)
```

This works but has problems:
- Deduplication with complex priority rules (PRESENT > ABSENT, then latest) requires
  sorting + groupby which is verbose in pandas
- Window functions (`ROW_NUMBER() OVER (PARTITION BY ...)`) don't exist in pandas natively
- Pandas loads the entire DataFrame into memory as Python objects

### What DuckDB gives you

**SQL for transformations.** The deduplication logic is naturally expressible as:
```sql
ROW_NUMBER() OVER (
    PARTITION BY student_id, attendance_date
    ORDER BY
        CASE WHEN status = 'PRESENT' THEN 1 ELSE 2 END,
        created_at DESC
)
```
This is readable, correct, and testable in isolation. The equivalent pandas code is much more
verbose and harder to reason about.

**In-memory columnar engine.** DuckDB reads PyArrow Tables directly (zero copy for most
operations). It processes columns in batches using SIMD instructions. For a 5,000-row
DataFrame this is microseconds.

**PyArrow integration.** `conn.execute(sql).fetch_arrow_table()` returns a PyArrow Table
directly — no Python object overhead, ready to serialize to Parquet.

**No server needed.** DuckDB runs in-process. No Docker service, no connection pool,
no startup time. `duckdb.connect()` is instant.

### Why not Spark?

Spark is the industry standard for large-scale distributed transformations. For 8,000 rows:
- JVM startup: 30+ seconds
- Minimum memory: 512MB per executor
- Configuration complexity: SparkConf, SparkContext, DataFrameReader, S3A connector jars...
- Result: 30× more complex setup for the same output

Spark earns its complexity at 100M+ rows or when you need true distributed parallelism.
This project doesn't qualify yet. See LAKEHOUSE_FUTURE.md for when to add it.

---

## 8. Why Airflow?

Airflow is used to orchestrate the three-DAG pipeline.

### Could you use cron instead?

Yes. A simple cron job running `python ingest.py && python transform.py && python load.py`
would work. Problems:
- No retry logic on failure
- No visibility into task-level success/failure (was it ingestion or transformation that failed?)
- No manual re-trigger of a single layer
- No dependency management between tasks

### Why Airflow specifically?

**Industry standard.** If you work at any company with a data team, they almost certainly
use Airflow or a Airflow-compatible orchestrator. Demonstrating Airflow proficiency is
valuable signal in a take-home test.

**Task-level granularity.** In DAG 01, the three ingest tasks run in parallel
(`ingest_students`, `ingest_attendance`, `ingest_assessments` all start simultaneously).
This is trivially expressed in Airflow:
```python
[ingest_students(), ingest_attendance(), ingest_assessments()] >> trigger
```
In a cron script, you'd have to handle parallelism manually with threads or subprocesses.

**TaskFlow API (Airflow 2.x).** The `@dag` and `@task` decorators make DAGs
feel like regular Python functions, with XCom passing handled transparently.
The return value of `@task` is automatically serialized and passed to downstream tasks.

**Visibility.** Every task has a UI, logs, retry history, and duration metrics.
When Silver transformation fails, you can see exactly which line errored in Airflow's log viewer.

### Why LocalExecutor (not CeleryExecutor)?

LocalExecutor runs tasks as subprocesses on the scheduler machine. No Redis, no separate
worker containers. For a single-machine local setup, this is simpler and sufficient.
CeleryExecutor adds Redis + Celery workers for horizontal scaling — not needed here.

---

## 9. Why Great Expectations?

GE runs as part of DAG 02 to validate Bronze data before Silver transformation.

### Could you write your own validation?

Yes — the fallback implementation in `ge_validations.py` does exactly this:
```python
{"name": "student_id_not_null", "check": lambda d: d["student_id"].notna().all()}
```
This is fine for simple checks. But as the pipeline grows:
- You accumulate bespoke validation functions per table
- There's no standardized reporting format
- There's no history of "what passed/failed over time"

### What GE gives you

**Standardized expectation vocabulary.** `expect_column_values_to_not_be_null`,
`expect_column_values_to_be_unique`, `expect_column_values_to_match_regex` — these are
the same regardless of which source you're validating. New team members know the API.

**Validation result objects.** GE returns a structured `ValidationResult` with:
- `success: bool`
- `statistics: {evaluated, successful, failed, success_percent}`
- Per-expectation results with sample unexpected values

**Non-blocking design decision.** We made an explicit choice: GE failures do NOT
halt the pipeline. Instead, failed rows go to `s3://rejected/` and the pipeline continues.
This is a **trade-off**:

  - Pro: A single bad row in attendance doesn't block the dashboard from being updated
  - Con: Bad data can silently flow into Silver if the team doesn't monitor the rejected/ bucket
  - Mitigation: All validation results are logged in Airflow with a `_ge_passed` column
    added to every Silver row, so analysts can filter to see which rows were validated

An alternative design is to fail-fast: if validation fails, raise an exception and stop.
This is safer for correctness but more disruptive operationally. The choice depends on
SLA requirements vs data quality tolerance.

---

## 10. Why Superset?

Superset provides the dashboard layer on top of the Gold PostgreSQL tables.

### Alternatives considered

**Grafana:**
- ✅ Excellent for time-series / metrics dashboards
- ❌ SQL-table exploration is secondary; not designed for ad-hoc analytics

**Metabase:**
- ✅ Very user-friendly, great for non-technical users
- ❌ Open-source version has limited features; business logic in UI, not in data model

**Redash:**
- ✅ SQL-first, clean interface
- ❌ Less actively maintained; smaller community

**Apache Superset:**
- ✅ Full open-source, Docker-native
- ✅ Connects to 40+ databases including PostgreSQL, DuckDB, Trino, Snowflake
- ✅ SQL Lab for ad-hoc queries
- ✅ REST API for automated dashboard creation (used in `setup_dashboard.py`)
- ✅ Native filter support, cross-filtering
- ❌ Complex initial setup (Flask app + Celery + metadata DB)
- ❌ Steep learning curve for custom visualizations

For a data engineering take-home, Superset signals the right level of tooling sophistication.
It's used in production at Airbnb, Twitter, and many others.

---

## 11. Data Quality Findings

The EDA notebook (`notebooks/01_eda.ipynb`) was run locally (not in Docker) and found these issues:

### Finding 1: 171 Duplicate Attendance Records

**What:** 171 cases where the same student has two or more records on the same date.

**Why it matters:** If you blindly COUNT(*) attendance, you'll overcount. A student appearing
as PRESENT twice inflates present_count. One appearing as PRESENT + ABSENT creates ambiguity.

**What the duplicates look like:** The EDA shows most duplicates are `PRESENT + ABSENT` pairs
(same student, same date, one record says present, another says absent). This is a real-world
pattern — the source system records both "check-in" and a subsequent manual override.

**How we handle it:**
```sql
ROW_NUMBER() OVER (
    PARTITION BY student_id, attendance_date
    ORDER BY
        CASE WHEN status = 'PRESENT' THEN 1 ELSE 2 END ASC,  -- PRESENT wins
        created_at DESC                                         -- latest wins on tie
)
```
Business rule applied: **any PRESENT signal wins**. If a student has PRESENT and ABSENT
on the same day, we keep PRESENT. This is an explicit business decision — document it.

**Alternative strategy:** Keep ABSENT wins (stricter: absence overrides check-in).
Or fail loudly and quarantine all duplicates for manual review.
**We chose PRESENT wins** because the use case is class performance reporting —
we don't want to penalize a student's attendance rate due to a data entry correction.

### Finding 2: 15 Duplicate Assessment Records

**What:** 15 cases where the same student took the same subject assessment on the same day twice.

**How we handle it:** Keep the latest `created_at`. Rationale: the second submission is
likely a correction or re-graded version. Latest = most authoritative.

### Finding 3: All students.updated_at are the same date

**What:** Every row in students.csv has `updated_at = 2025-01-01T00:00:00`. This means
you cannot use `updated_at > last_run_timestamp` as an incremental load filter.
The file is a static dump, not a CDC stream.

**Implication:** We always do a full load of students. For 1,200 students this is fine.
At 1 million students, you'd need a CDC source (Debezium + Kafka) to avoid scanning all rows.

### Finding 4: attendance_rate formula ambiguity

The handout schema shows:
```
total_students → 30
present_count → 25
attendance_rate → 0.83
```
`25 / 30 = 0.833 ≈ 0.83` → So `attendance_rate = present_count / total_students`.

But `students_with_attendance = 28` — meaning 2 students have no record at all.
This raises a question: are they absent or unrecorded?

**Our assumption:** No attendance record = unrecorded. We use `total_students` (not
`students_with_attendance`) as the denominator. This means students with no record
implicitly lower the attendance rate. This matches the handout math. **Document this assumption.**

---

## 12. Transformation Decisions — Layer by Layer

### Bronze Layer (`scripts/ingestion/ingest_sources.py`)

**Why store everything as strings at Bronze?**

Type inference at ingest is dangerous. Consider `grade_level = "10"`:
- Pandas `read_csv` might infer it as INTEGER ✅
- Or as FLOAT64 if there's any missing value ("10.0") ❌
- Or fail entirely if there's a value like "Grade 10" ❌

By storing everything as strings at Bronze, we guarantee:
- The Bronze file is always writable (no type mismatch failures)
- The exact bytes from the source are preserved
- Type casting failures happen in Silver where they're caught by GE, not at ingest

**The `_read_csv_as_strings` helper** uses `pd.read_csv(path, dtype=str, keep_default_na=False)`:
- `dtype=str`: forces ALL columns to string, no inference
- `keep_default_na=False`: prevents pandas from converting empty strings to `NaN`
  (which would be a null in Parquet, losing the distinction between "" and null)

### Silver Layer (`scripts/transformation/bronze_to_silver.py`)

**Why DuckDB SQL instead of pandas for transformation?**

The deduplication logic is most naturally expressed as a window function:
```sql
ROW_NUMBER() OVER (PARTITION BY student_id, attendance_date ORDER BY ...)
```
Pandas doesn't have this natively. You'd need:
```python
df.sort_values(['student_id', 'attendance_date', ...])
   .groupby(['student_id', 'attendance_date'])
   .first()
```
Which is less readable and harder to test in isolation.

**Why are SQL constants (`_SQL_STUDENTS`, `_SQL_ATTENDANCE`, `_SQL_ASSESSMENTS`) at module level?**

Before refactoring, the SQL was embedded inside each function. Module-level constants:
1. Are readable without scrolling through pipeline logic
2. Can be unit-tested: `duckdb.connect().execute(_SQL_STUDENTS, [...])` directly
3. Show up in `grep` searches without needing to parse function bodies

**The `_run_bronze_to_silver` abstraction:**

All three sources go through the same 5-step pipeline:
1. Download Parquet from Bronze
2. GE validate
3. (Optionally) upload to rejected/
4. DuckDB transform
5. Upload to Silver

Before refactoring, this 5-step sequence was copy-pasted 3 times (250 lines → 185 lines).
After refactoring, each source is a single-line call:
```python
def transform_attendance(run_date): 
    return _run_bronze_to_silver("attendance", run_date, validate_attendance_bronze, _SQL_ATTENDANCE)
```

### Gold Layer (`scripts/transformation/silver_to_gold.py`)

**Why TRUNCATE + INSERT and not UPSERT?**

UPSERT (INSERT … ON CONFLICT DO UPDATE) is ideal for incremental loads where you append
new rows and update changed ones. But our source data is static — every run loads the
same 1,200 students, 5,000 attendance records, 3,000 assessments.

For a full daily reload, TRUNCATE + INSERT is:
- Simpler SQL
- Idempotent: running it twice gives the same result
- Faster: no conflict detection overhead
- Safer: no risk of stale data from old runs

The mart table (`mart_class_daily_performance`) uses UPSERT because it's computed from
the Gold facts (not directly from Silver). An UPSERT here protects against partial failures
during the mart computation step.

**Why join with students in Silver-to-Gold (not in Silver)?**

`fact_attendance` needs `class_id` from the students table. We enrich it at Gold load time:
```sql
SELECT a.*, s.class_id FROM silver_att a LEFT JOIN silver_stu s ON a.student_id = s.student_id
```

**Why not add `class_id` in the Silver transformation instead?**

Silver's principle: stay source-faithful. The attendance source doesn't have `class_id` —
it's derived from joining with another source. Joins that cross source boundaries belong
in Gold, not Silver. This keeps Silver tables exactly representative of their source.

---

## 13. Gold Layer Data Model Design

### Why these four tables?

```
gold.dim_students          (1,200 rows)   — reference data for joining
gold.fact_attendance       (4,829 rows)   — event facts
gold.fact_assessments      (2,985 rows)   — event facts
gold.mart_class_daily_performance        — pre-aggregated for dashboard speed
```

This is a **simplified star schema**:
- `dim_students` is the dimension
- `fact_attendance` and `fact_assessments` are the facts
- The mart is a pre-aggregated wide table for dashboard performance

**Why no `dim_dates` table?**

A proper star schema would include a date dimension:
```sql
dim_dates (date_id, year, month, day, quarter, week_of_year, day_name, is_weekend, ...)
```
This enables queries like "attendance rate by month" or "avg score on weekdays vs weekends"
without computing date parts in SQL.

We omitted it for pragmatism — the handout doesn't require it, and Superset has built-in
time intelligence for date columns. In production, add it.

**Why no foreign key constraints?**

FK constraints would ensure referential integrity:
```sql
FOREIGN KEY (student_id) REFERENCES gold.dim_students(student_id)
```
We chose not to add them because:
1. TRUNCATE CASCADE with FK constraints requires careful load ordering
2. Our EDA confirmed zero orphan FK violations in the source data
3. FK constraints add insert overhead

**Important caveat:** If you ever add FK constraints, the `TRUNCATE TABLE ... CASCADE`
in `postgres_client.py:truncate_and_insert` will cascade to dependent tables.
Load order must be: dim_students first, then facts, then mart.

### Why no normalized score in Gold facts?

`normalized_score = score / max_score * 100` is computed in Silver and carried through to Gold.
This is correct: Silver is where derivations happen (derived from a single source),
Gold is where joins and aggregations happen (derived from multiple sources).

The mart's `avg_score` uses raw `score` (not `normalized_score`) because:
- All assessments in this dataset have `max_score = 100`
- `avg_score` in the handout example matches `mean(raw_score)`
- If `max_score` ever varies across subjects, switch the mart to `avg(normalized_score)`

---

## 14. Code Review

This section documents every issue found and the refactor applied.

### Issue 1: `ingest_sources.py` — Massive DRY violation

**Before:** Three functions of ~25 lines each, each doing:
`open file → list of dicts → build pa.table col-by-col → call local _upload`.
The pattern is identical. The only difference is the filename and column names.

**Why this is a problem:**
- Adding a metadata column (e.g., `_batch_id`) requires editing 3 places
- A bug in the upload logic (e.g., wrong bucket name) exists in 3 places
- The local `_upload` function reimplements `upload_parquet` from `minio_client.py`
  with slightly different behavior (no row count logging)

**Refactor applied:**
Extracted 4 shared helpers: `_read_csv_as_strings`, `_read_json_as_strings`,
`_with_metadata`, `_upload_bronze`. Each public function is now one line:
```python
def ingest_attendance(run_date):
    return _upload_bronze("attendance", run_date, _with_metadata(_read_csv_as_strings("attendance.csv"), "attendance.csv"))
```

**Additional improvement:** Replaced `csv.DictReader` (Python row-by-row loop) with
`pd.read_csv(dtype=str, keep_default_na=False)`. This is cleaner and avoids the list
comprehension per column: `[r["field"] for r in rows]`.

---

### Issue 2: `ge_validations.py` — GE boilerplate copy-pasted 3 times

**Before:** Each validation function repeated the same 6 lines:
```python
ctx   = gx.get_context(mode="ephemeral")
ds    = ctx.sources.add_pandas("...")
asset = ds.add_dataframe_asset("...")
batch = asset.build_batch_request(dataframe=df)
suite = ctx.add_or_update_expectation_suite("...")
v     = ctx.get_validator(batch_request=batch, expectation_suite=suite)
```
3 functions × 6 lines = 18 lines of pure boilerplate with no meaningful variation.

**Also:** `validate_assessments_bronze` computed `score_s` and `max_s` as local variables
but never used them (the lambdas recompute inline). These were dead variables.

**Refactor applied:**
Extracted `_run_ge_suite(df, suite_name, add_expectations)`. Each validator now passes
a closure for `add_expectations`:
```python
def add_expectations(v):
    v.expect_column_values_to_not_be_null("student_id")
    ...

return _run_ge_suite(df, "students_bronze", add_expectations)
```
Dead variables `score_s` and `max_s` removed.

---

### Issue 3: `bronze_to_silver.py` — Same pipeline copy-pasted 3 times + inconsistent response shape

**Before (three problems in one file):**

Problem A — Pipeline copy-paste:
The 5-step pipeline (download → validate → connect → count → execute → upload → build dict)
was written three times (~70 lines each). Any change to the pipeline required 3 edits.

Problem B — `_download` reimplements `download_parquet`:
```python
def _download(client, bucket, key):
    resp = client.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(resp["Body"].read())
    return pq.read_table(buf)
```
This is exactly `download_parquet` from `minio_client.py`. Having two implementations
of the same thing is a maintenance trap.

Problem C — Inconsistent response shapes (runtime bug):
`transform_students` returned `{"records": N, ...}` while `transform_attendance` returned
`{"records_raw": N, "records_silver": N, "deduplicated": N, ...}`.
In DAG 02's `log_silver_summary`, accessing `students["records"]` would KeyError at
runtime because the key is `records_raw` after the other functions set the precedent.

**Refactors applied:**

A — Extracted `_run_bronze_to_silver(entity, run_date, validate_fn, transform_sql)`.
All three public functions are now single-line.

B — Eliminated local `_download`. The pipeline reads directly inline:
```python
raw = pq.read_table(io.BytesIO(client.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()))
```

C — All three functions now return the same shape: `{records_raw, records_silver, deduplicated, key, validation}`.
DAG 02's summary task was updated to use the uniform shape.

**SQL moved to module-level constants** (`_SQL_STUDENTS`, `_SQL_ATTENDANCE`, `_SQL_ASSESSMENTS`):
SQL strings buried inside functions can't be easily read, grepped, or unit-tested.
Module-level constants are visible at the top of the file.

---

### Issue 4: `silver_to_gold.py` — Reimplements `download_parquet`

**Before:** `_read_silver` was a 4-line function doing exactly what `download_parquet`
in `minio_client.py` already does.

**Refactor applied:**
```python
def _read_silver(client, entity, run_date):
    return download_parquet(client, SILVER_BUCKET, f"{entity}/run_date={run_date}/{entity}.parquet")
```
Single line, uses the shared utility.

**Noted (not fixed yet):** `load_fact_attendance` and `load_fact_assessments` both call
`_read_silver(client, "students", run_date)` separately — the students Parquet is downloaded
twice per DAG run. At 1,200 rows / ~50KB this is negligible, but in a larger pipeline
you'd cache it with `functools.lru_cache` or pass it as a parameter.

---

### Issue 5: `minio_client.py` — Wrong exception type in `ensure_buckets` (silent bug)

**Before:**
```python
except client.exceptions.NoSuchBucket:
    client.create_bucket(Bucket=bucket)
```
`client.exceptions.NoSuchBucket` doesn't exist as an attribute on boto3 S3 clients.
boto3 raises `botocore.exceptions.ClientError` for ALL HTTP errors.
This `except` block would never catch anything — the function would either silently succeed
(bucket exists) or raise an uncaught `ClientError`.

**Fix applied:**
```python
from botocore.exceptions import ClientError
try:
    client.head_bucket(Bucket=bucket)
except ClientError as exc:
    if exc.response["Error"]["Code"] in ("404", "NoSuchBucket"):
        client.create_bucket(Bucket=bucket)
    else:
        raise  # re-raise unexpected errors (permissions, network, etc.)
```

---

### Issue 6: DAG 02 `log_silver_summary` — Would KeyError at runtime

**Before:**
```python
logger.info("Students: %d records", students["records"])  # KeyError: "records" → should be "records_raw"
```
After the bronze_to_silver refactor unified all response shapes, the DAG summary was
updated to use a loop over all three sources with the correct key names.

---

### Issue 7: MinIO Chainguard Wolfi image has no shell utilities (distroless)

**What:** The health check `["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]` failed
because `minio/minio:RELEASE.2024+` uses a Chainguard Wolfi base image — a distroless minimal
Linux that ships with no `curl`, no `wget`, and no shell. The container was running fine but
Docker Compose declared it unhealthy, blocking every dependent service.

**Fix applied:**
- Removed the health check from `minio` entirely (distroless images can't self-check)
- Changed all `condition: service_healthy` on minio to `condition: service_started`
- Added a retry loop in `minio-init` that polls until `mc alias set` succeeds:
```bash
until mc alias set local http://minio:9000 minioadmin minioadmin123 2>/dev/null; do
  sleep 3
done
```

**Lesson:** Always check the base image of a Docker image before relying on health check tools.
Distroless images are increasingly common for security — they remove attack surface but also
remove debugging tools. Use a sidecar or the application's own liveness probe instead.

---

### Issue 8: Airflow init command — YAML `>` scalar breaks multiline bash

**What:** The `airflow users create` command was written as:
```yaml
command: >
  bash -c "
    airflow users create
      --username admin
      --password admin
      ...
  "
```
The YAML `>` (folded scalar) joins newlines with spaces — but Docker Compose passed each
flag as a separate shell command instead of as a single bash `-c` string. The result was
`/bin/bash: line 4: --username: command not found`.

**Fix applied:** Changed to YAML list format with a `|` (literal scalar) for the bash script:
```yaml
command:
  - bash
  - -c
  - |
      airflow db migrate &&
      airflow users create \
        --username admin \
        --password admin \
        ...
```
With `|`, bash receives a real multi-line script. With `\` line continuation, each flag
stays part of the same command.

**Lesson:** For multi-line shell commands in Docker Compose, prefer the YAML list format
`[bash, -c, |script|]` over the `>` folded scalar — it is unambiguous and
behaves consistently across Docker Compose versions.

---

### Issue 9: Superset 3.x REST API — Three breaking changes from older docs

Three separate Superset API issues were encountered during integration:

**A. `slices` field removed from dashboard creation**

In Superset ≤2.x, `POST /api/v1/dashboard/` accepted a `slices` list of chart IDs.
In Superset 3.x this field was removed — passing it returns `{"slices": ["Unknown field."]}`.

Fix: Create the dashboard empty, then `PUT` with `json_metadata.positions` — a nested
dictionary tree where each `CHART` node references a chart by `chartId`.

**B. String metrics (`"metrics": ["column_name"]`) only work for saved metrics**

Passing a bare string like `"metrics": ["attendance_rate"]` only works if that metric
was pre-defined on the dataset in Superset's UI. For ad-hoc column aggregations, Superset
requires the full adhoc metric object:
```python
{"expressionType": "SIMPLE", "column": {"column_name": "attendance_rate"}, "aggregate": "AVG", "label": "..."}
```
Fix: Added `_m(column, aggregate, label)` helper to all chart param definitions.

**C. `bar` vs `dist_bar` — time-series vs categorical bar chart**

In Superset legacy, there are two different bar chart viz types:
- `"bar"` = time-series bar. Requires `granularity_sqla` + `main_dttm_col` on the dataset.
  Fails with `"Datetime column not provided"` when a non-temporal column is used as x-axis.
- `"dist_bar"` = categorical bar. Groups by any `groupby` column regardless of type.

After `set_dataset_temporal_col()` sets `main_dttm_col="date"` on the mart dataset, the
`bar` chart tries to use that time axis on every chart — breaking `"Average Score by Class"`
which groups by `class_id`, not by date.

Fix: Use `"dist_bar"` for all categorical bar charts. Only use `"bar"` (or the newer
`"echarts_timeseries_line"`) when you explicitly want the x-axis to be a time dimension.

**D. `create_chart` was not idempotent (skipped existing charts without updating)**

The original `create_chart` returned the existing ID immediately without touching params.
This meant fixed chart configurations never took effect on re-runs.

Fix: On match, always `PUT` the new params to the existing chart before returning its ID.
This makes `make setup-superset` idempotent — safe to run multiple times.

---

## 15. Known Limitations and Technical Debt

**1. No incremental load for any source.**
All three sources are full-loaded every run. For students (1,200 rows) this is fine.
If students grows to 1M rows, you need CDC (Debezium) and an `updated_at`-based filter.

**2. No data retention policy for Bronze.**
Every daily run writes a new partition. After a year, you have 365 copies of the
same 1,200-student file. Add a lifecycle rule to MinIO to expire Bronze files after 90 days
(or keep them for auditing — your choice, but make it explicit).

**3. No FK constraints in Gold schema.**
Referential integrity is enforced by the pipeline logic, not the database.
A bug in the pipeline could write `fact_attendance` rows with `student_id` values
that don't exist in `dim_students`. PostgreSQL wouldn't catch this.

**4. Superset dashboard JSON is programmatic, not version-controlled.**
The `setup_dashboard.py` script creates charts via API, but the dashboard definition
isn't stored in the repo. If you `make reset`, you lose the dashboard and must run
`make setup-superset` again. Ideal solution: export the dashboard ZIP after creation
and check it into source control.

**5. The mart uses TRUNCATE + UPSERT hybrid.**
Fact tables (dim_students, fact_attendance, fact_assessments) are truncated then
reloaded. The mart uses UPSERT. This is slightly inconsistent — ideally either
everything is truncated or everything is upserted.

**6. Great Expectations version (0.18.x) is end-of-life.**
GE 1.0 was released in 2024 with a significantly different API. The migration is
non-trivial. The fallback validation in `ge_validations.py` ensures the pipeline
continues working if GE is unavailable or errors, but plan to upgrade.

---

## 16. What "Future Lakehouse" Means for This Project

The current stack is intentionally minimal. When you hit these thresholds, migrate:

| Trigger | What breaks | Migration |
|---|---|---|
| Students > 1M rows | Full-load is too slow | Add CDC (Debezium → Kafka), incremental loads |
| Bronze Parquet > 100M rows total | No ACID, no schema evolution | Add Apache Iceberg on top of MinIO |
| Gold queries > 5s in Superset | PostgreSQL row-scan bottleneck | Add Trino as query engine over Iceberg Gold |
| 10+ source systems | DAG proliferation, no lineage | Add dbt for SQL model management |
| 5+ concurrent BI users | PostgreSQL connection saturation | Add PgBouncer or migrate Gold to Trino |

The full migration path is documented in **LAKEHOUSE_FUTURE.md**, including Docker Compose
snippets for adding Trino, Project Nessie (Iceberg catalog), and dbt.

The key insight: **MinIO doesn't go away in the lakehouse**. It remains the storage layer.
What changes is the *format* (plain Parquet → Iceberg) and the *query engine*
(PostgreSQL → Trino). The medallion architecture, Airflow orchestration, and Superset
visualization all stay the same.
