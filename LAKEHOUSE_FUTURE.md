# Future Lakehouse Architecture

This document describes when and how to evolve the current pipeline into a full **open lakehouse** architecture as data volume, team size, and analytical complexity grow.

---

## When to Migrate

The current stack (MinIO + PostgreSQL + Airflow) is intentionally lean. Migrate when you hit **any** of these triggers:

| Trigger | Threshold | Symptom |
|---------|-----------|---------|
| Row volume | > 100M rows across fact tables | PostgreSQL query time > 5s on mart |
| Team size | > 3 data engineers | DAG conflicts, no SQL lineage, hard to test |
| Source count | > 10 source systems | Manual ingestion scripts don't scale |
| History requirements | > 3 years of data | No time-travel, snapshot compares are slow |
| Schema changes | Frequent upstream changes | No ACID schema evolution on raw Parquet |
| Concurrent users | > 20 BI users | Superset + PostgreSQL saturates under load |

---

## Target Lakehouse Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                         Source Systems                                │
│  CSV / JSON / REST APIs / Databases / Kafka streams                   │
└───────────────────────────────┬───────────────────────────────────────┘
                                │  Batch (Airflow) or Stream (Kafka)
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│                     MinIO — Object Storage                            │
│                    (S3-compatible, runs locally)                      │
│                                                                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │  Bronze (Iceberg)│  │ Silver (Iceberg) │  │  Gold (Iceberg/PG)  │  │
│  │  Raw Parquet     │  │ Clean Parquet    │  │  Aggregate Tables   │  │
│  │  + time travel   │  │ + schema evol.  │  │  Mart tables        │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │
└───────────────────────────────┬───────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
        ┌───────────────────┐   ┌───────────────────────┐
        │  Trino (Query)    │   │  Apache Spark          │
        │  SQL on Parquet   │   │  (large transforms,    │
        │  via Iceberg      │   │   ML feature pipelines)│
        └─────────┬─────────┘   └───────────────────────┘
                  │
                  ▼
        ┌───────────────────┐
        │       dbt          │
        │  SQL transforms,  │
        │  lineage, tests   │
        └─────────┬─────────┘
                  │
                  ▼
        ┌───────────────────┐
        │  Apache Superset  │
        │  Dashboards (BI)  │
        └───────────────────┘
```

---

## New Components and Roles

### Apache Iceberg (Open Table Format)
**Replaces**: Plain Parquet files in MinIO

**Why**:
- ACID transactions on object storage — safe concurrent writes from Airflow/Spark
- Time travel — `SELECT * FROM students FOR SYSTEM_TIME AS OF '2025-01-01'`
- Schema evolution — add/rename/drop columns without rewriting all files
- Partition evolution — change partition strategy without data migration
- Row-level deletes — required for GDPR/right-to-erasure compliance

**Local setup**: Iceberg uses a catalog (REST catalog, Hive Metastore, or Nessie). For local Docker, use **Project Nessie** (open source, Git-like catalog for Iceberg).

```yaml
# Add to docker-compose.yml
nessie:
  image: projectnessie/nessie:0.75.0
  ports:
    - "19120:19120"
```

### Apache Hive Metastore (or Project Nessie)
**Replaces**: No catalog in current setup (files just live in MinIO with no registry)

**Why**: Trino and Spark need a catalog to know the schema and location of Iceberg tables. Nessie is simpler for local use; Hive Metastore is more production-proven.

### Trino (Distributed Query Engine)
**Replaces**: DuckDB in-process transformations for Silver→Gold

**Why**:
- Distributed SQL over Iceberg tables in MinIO — scales horizontally
- Supports complex joins across multiple catalogs (Iceberg + PostgreSQL + Kafka)
- Superset connects natively to Trino via `trino://` connector
- Can replace PostgreSQL as the Gold query layer entirely

```yaml
# Add to docker-compose.yml
trino:
  image: trinodb/trino:435
  ports:
    - "8090:8080"
  volumes:
    - ./trino/catalog:/etc/trino/catalog
```

```ini
# trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie.uri=http://nessie:19120/api/v1
iceberg.nessie.default-reference-name=main
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin123
```

### dbt (Data Build Tool)
**Replaces**: Raw Python/DuckDB SQL transformation scripts

**Why**:
- SQL-first transformations with version control
- Built-in lineage graph (`dbt docs`)
- Built-in tests (`not_null`, `unique`, `accepted_values`, `relationships`)
- Modular models: staging → intermediate → mart
- Works with Trino via `dbt-trino` adapter

```bash
dbt run --target prod          # run all models
dbt test                       # run all tests
dbt docs generate && dbt docs serve  # lineage UI
```

### Apache Spark (Optional, for large-scale ETL)
**When to add**: When a single Trino query can't process a full historical load within the SLA, or when you need ML feature pipelines.

**Local setup**: Use `bitnami/spark` Docker image with one master + one worker.

---

## Migration Strategy

### Phase 1 — Add Iceberg (no disruption)
Write new Bronze partitions as Iceberg format while keeping old Parquet. Use Trino to register existing Parquet as external tables via Iceberg REST catalog.

### Phase 2 — Replace DuckDB transforms with dbt + Trino
Convert `scripts/transformation/bronze_to_silver.py` logic into dbt models:
- `models/staging/stg_students.sql`
- `models/staging/stg_attendance.sql`
- `models/staging/stg_assessments.sql`
- `models/intermediate/int_attendance_deduped.sql`
- `models/marts/mart_class_daily_performance.sql`

### Phase 3 — Replace PostgreSQL Gold with Trino-served Iceberg Gold
Remove the PostgreSQL serving layer. Point Superset directly at Trino. Use Iceberg Gold tables for analytics.

### Phase 4 — Add streaming (Kafka) for real-time attendance
For near-real-time dashboards:
- Attendance events → Kafka topic
- Kafka Connect → Iceberg sink (via Iceberg Kafka Sink Connector)
- Dashboard refresh interval: 5 min instead of daily

---

## Comparison: Current vs Future

| Concern | Current | Future |
|---------|---------|--------|
| Storage | MinIO (plain Parquet) | MinIO (Apache Iceberg) |
| Table format | Files only | ACID, time travel, schema evolution |
| Query engine | DuckDB (in-process) + PostgreSQL | Trino (distributed) |
| Transformations | Python scripts | dbt SQL models |
| Catalog | None | Project Nessie |
| Orchestration | Airflow (LocalExecutor) | Airflow (CeleryExecutor / K8s) |
| BI layer | Superset → PostgreSQL | Superset → Trino |
| Testing | Great Expectations (GE) | GE + dbt tests |
| Schema changes | Manual migration | Iceberg schema evolution |
| Historical data | Overwrite on reload | Time travel retained |
| Scale | Millions of rows | Billions of rows |
| Team | 1-3 engineers | 3-10 engineers |

---

## Key Trade-offs

**Iceberg vs Delta Lake vs Hudi**: All three are open table formats. Iceberg is the most vendor-neutral and has the best Trino integration. Delta Lake is tightly coupled to Spark/Databricks. Hudi is optimised for streaming upserts.

**Trino vs Spark SQL**: Trino is better for interactive/dashboard queries (lower latency, JDBC-native). Spark is better for large historical backfills and ML pipelines.

**dbt vs hand-written SQL**: dbt adds overhead for a 3-table pipeline but pays back immediately at 10+ models — lineage, documentation, and incremental materializations justify the learning curve.

**Nessie vs Hive Metastore**: Nessie (Git-for-data semantics, REST catalog) is better for local dev and multi-branch workflows. Hive Metastore is more production-proven but heavier. Both work with Trino and Iceberg.

---

## Docker Compose Addition (Minimal Lakehouse)

```yaml
# Additions to existing docker-compose.yml for Phase 1-2

nessie:
  image: projectnessie/nessie:0.75.0
  container_name: onlenpajak-nessie
  ports: ["19120:19120"]
  networks: [onlenpajak-net]

trino:
  image: trinodb/trino:435
  container_name: onlenpajak-trino
  ports: ["8090:8080"]
  volumes:
    - ./trino/catalog:/etc/trino/catalog:ro
    - ./trino/config:/etc/trino:ro
  networks: [onlenpajak-net]

# dbt runs as an Airflow operator or standalone CLI container
dbt:
  image: ghcr.io/dbt-labs/dbt-trino:1.7.0
  container_name: onlenpajak-dbt
  volumes:
    - ./dbt_project:/usr/app/dbt
  environment:
    DBT_PROFILES_DIR: /usr/app/dbt
  networks: [onlenpajak-net]
```

Total additional services: **3** (Nessie, Trino, dbt). Total Docker memory: ~6-8 GB.
