#!/bin/bash
# Runs automatically when the postgres container first starts.
# Creates all required databases, users, and the gold schema.
set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Initializing PostgreSQL databases..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ─── Create users and databases ──────────────────────────────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Airflow metadata DB
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow_db OWNER airflow;

    -- Gold serving layer DB
    CREATE USER gold WITH PASSWORD 'gold';
    CREATE DATABASE gold_db OWNER gold;

    -- Superset metadata DB
    CREATE USER superset WITH PASSWORD 'superset';
    CREATE DATABASE superset_db OWNER superset;

    GRANT ALL PRIVILEGES ON DATABASE airflow_db  TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE gold_db     TO gold;
    GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset;
EOSQL

echo "  ✓ Databases and users created"

# ─── Gold schema and tables ───────────────────────────────────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "gold_db" <<-EOSQL
    GRANT ALL ON SCHEMA public TO gold;
    CREATE SCHEMA IF NOT EXISTS gold;
    GRANT ALL ON SCHEMA gold TO gold;
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO gold;
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON SEQUENCES TO gold;

    -- ─────────────────────────────────────────────────────
    -- Dimension: Students
    -- ─────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS gold.dim_students (
        student_id          VARCHAR(20)     PRIMARY KEY,
        student_name        VARCHAR(100)    NOT NULL,
        class_id            VARCHAR(20)     NOT NULL,
        grade_level         INTEGER         NOT NULL,
        enrollment_status   VARCHAR(20)     NOT NULL,
        updated_at          TIMESTAMP       NOT NULL,
        _ingested_at        TIMESTAMP       NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_dim_students_class  ON gold.dim_students(class_id);
    CREATE INDEX IF NOT EXISTS idx_dim_students_status ON gold.dim_students(enrollment_status);

    -- ─────────────────────────────────────────────────────
    -- Fact: Attendance (deduplicated in Silver)
    -- ─────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS gold.fact_attendance (
        attendance_id       VARCHAR(20)     PRIMARY KEY,
        student_id          VARCHAR(20)     NOT NULL,
        class_id            VARCHAR(20)     NOT NULL,
        attendance_date     DATE            NOT NULL,
        status              VARCHAR(10)     NOT NULL,
        created_at          TIMESTAMP       NOT NULL,
        _ingested_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
        CONSTRAINT chk_attendance_status CHECK (status IN ('PRESENT', 'ABSENT'))
    );
    CREATE INDEX IF NOT EXISTS idx_fact_att_student    ON gold.fact_attendance(student_id);
    CREATE INDEX IF NOT EXISTS idx_fact_att_class_date ON gold.fact_attendance(class_id, attendance_date);
    CREATE INDEX IF NOT EXISTS idx_fact_att_date       ON gold.fact_attendance(attendance_date);

    -- ─────────────────────────────────────────────────────
    -- Fact: Assessments (deduplicated + normalized_score in Silver)
    -- ─────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS gold.fact_assessments (
        assessment_id       VARCHAR(20)     PRIMARY KEY,
        student_id          VARCHAR(20)     NOT NULL,
        class_id            VARCHAR(20)     NOT NULL,
        subject             VARCHAR(50)     NOT NULL,
        score               NUMERIC(6,2)    NOT NULL,
        max_score           NUMERIC(6,2)    NOT NULL,
        normalized_score    NUMERIC(6,2)    NOT NULL,   -- score/max_score * 100
        assessment_date     DATE            NOT NULL,
        created_at          TIMESTAMP       NOT NULL,
        _ingested_at        TIMESTAMP       NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_fact_asm_student    ON gold.fact_assessments(student_id);
    CREATE INDEX IF NOT EXISTS idx_fact_asm_class_date ON gold.fact_assessments(class_id, assessment_date);
    CREATE INDEX IF NOT EXISTS idx_fact_asm_subject    ON gold.fact_assessments(subject);
    CREATE INDEX IF NOT EXISTS idx_fact_asm_date       ON gold.fact_assessments(assessment_date);

    -- ─────────────────────────────────────────────────────
    -- Mart: Class Daily Performance
    -- Primary analytics table matching the handout schema
    -- ─────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS gold.mart_class_daily_performance (
        class_id                    VARCHAR(20)     NOT NULL,
        date                        DATE            NOT NULL,
        -- Student dimension
        total_students              INTEGER         NOT NULL DEFAULT 0,
        -- Attendance metrics
        students_with_attendance    INTEGER         NOT NULL DEFAULT 0,
        present_count               INTEGER         NOT NULL DEFAULT 0,
        absent_count                INTEGER         NOT NULL DEFAULT 0,
        attendance_rate             NUMERIC(5,4),           -- present_count / total_students
        -- Assessment metrics
        students_with_assessment    INTEGER         NOT NULL DEFAULT 0,
        assessment_count            INTEGER         NOT NULL DEFAULT 0,
        avg_score                   NUMERIC(6,2)    DEFAULT 0.00,
        -- Metadata
        _computed_at                TIMESTAMP       NOT NULL DEFAULT NOW(),

        PRIMARY KEY (class_id, date)
    );
    CREATE INDEX IF NOT EXISTS idx_mart_cdp_date  ON gold.mart_class_daily_performance(date);
    CREATE INDEX IF NOT EXISTS idx_mart_cdp_class ON gold.mart_class_daily_performance(class_id);

EOSQL

echo "  ✓ Gold schema and tables created"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PostgreSQL initialization complete"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
