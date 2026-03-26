-- ============================================================
-- Gold Layer Schema — PostgreSQL
-- Reference copy (executed automatically via sql/init/01_init_databases.sh)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ─────────────────────────────────────────────────────────────────────────────
-- dim_students
-- One row per student. Source: silver/students
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_students (
    student_id          VARCHAR(20)     PRIMARY KEY,
    student_name        VARCHAR(100)    NOT NULL,
    class_id            VARCHAR(20)     NOT NULL,
    grade_level         INTEGER         NOT NULL,
    enrollment_status   VARCHAR(20)     NOT NULL,
    updated_at          TIMESTAMP       NOT NULL,
    _ingested_at        TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- fact_attendance
-- One row per attendance event (deduplicated in Silver layer).
-- class_id is enriched via JOIN with dim_students.
-- Dedup strategy: PRESENT > ABSENT, then latest created_at.
-- ─────────────────────────────────────────────────────────────────────────────
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

-- ─────────────────────────────────────────────────────────────────────────────
-- fact_assessments
-- One row per assessment event (deduplicated in Silver layer).
-- Adds normalized_score = score / max_score * 100.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.fact_assessments (
    assessment_id       VARCHAR(20)     PRIMARY KEY,
    student_id          VARCHAR(20)     NOT NULL,
    class_id            VARCHAR(20)     NOT NULL,
    subject             VARCHAR(50)     NOT NULL,
    score               NUMERIC(6,2)    NOT NULL,
    max_score           NUMERIC(6,2)    NOT NULL,
    normalized_score    NUMERIC(6,2)    NOT NULL,
    assessment_date     DATE            NOT NULL,
    created_at          TIMESTAMP       NOT NULL,
    _ingested_at        TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_class_daily_performance
-- Aggregated mart: one row per (class_id, date).
-- Serves as the primary analytics table for the Superset dashboard.
--
-- Key notes:
--   attendance_rate = present_count / total_students  (NOT / students_with_attendance)
--   total_students  = count of ACTIVE students in that class from dim_students
--   avg_score       = mean of score (not normalized_score) across all assessments that day
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.mart_class_daily_performance (
    class_id                    VARCHAR(20)     NOT NULL,
    date                        DATE            NOT NULL,
    total_students              INTEGER         NOT NULL DEFAULT 0,
    students_with_attendance    INTEGER         NOT NULL DEFAULT 0,
    present_count               INTEGER         NOT NULL DEFAULT 0,
    absent_count                INTEGER         NOT NULL DEFAULT 0,
    attendance_rate             NUMERIC(5,4),
    students_with_assessment    INTEGER         NOT NULL DEFAULT 0,
    assessment_count            INTEGER         NOT NULL DEFAULT 0,
    avg_score                   NUMERIC(6,2)    DEFAULT 0.00,
    _computed_at                TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (class_id, date)
);
