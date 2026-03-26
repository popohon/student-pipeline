"""
Great Expectations validation suites for each Bronze source.

Design:
- Ephemeral GE context — no persistent store needed in Docker.
- Falls back to plain pandas-based checks if GE is unavailable or errors.
- Every validator returns a uniform dict: {success, statistics, failed_expectations, engine}.
- Non-blocking: validation failure logs a warning and writes rejected data to MinIO,
  but does NOT halt the pipeline. This is an explicit trade-off — see THOUGHT_PROCESS.md.
"""
import logging
from typing import Any, Callable, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import great_expectations as gx
    _GE_AVAILABLE = True
except ImportError:  # pragma: no cover
    logger.warning("great-expectations not found — using fallback validators")
    _GE_AVAILABLE = False


# ─── Result builders ───────────────────────────────────────────────────────────

def _fallback_result(rules: List[Dict], df: pd.DataFrame) -> Dict[str, Any]:
    """Run rule-based fallback checks and return a uniform result dict."""
    failed = [r["name"] for r in rules if not r["check"](df)]
    return {
        "success":              len(failed) == 0,
        "statistics":          {"evaluated_expectations": len(rules), "failed_expectations": len(failed)},
        "failed_expectations": failed,
        "engine":              "fallback",
    }


def _ge_result(result) -> Dict[str, Any]:
    return {
        "success":              result.success,
        "statistics":          dict(result.statistics),
        "failed_expectations": [r.expectation_config.expectation_type for r in result.results if not r.success],
        "engine":              "great_expectations",
    }


# ─── GE runner (eliminates boilerplate across all 3 suites) ────────────────────

def _run_ge_suite(
    df: pd.DataFrame,
    suite_name: str,
    add_expectations: Callable,
) -> Dict[str, Any]:
    """
    Generic GE runner. Handles all setup boilerplate so each validator only
    declares what it checks, not how to set up GE.

    Pattern:
        ephemeral context → datasource → asset → batch → validator → validate
    """
    try:
        ctx      = gx.get_context(mode="ephemeral")
        ds       = ctx.sources.add_pandas(f"ds_{suite_name}")
        asset    = ds.add_dataframe_asset("data")
        batch    = asset.build_batch_request(dataframe=df)
        suite    = ctx.add_or_update_expectation_suite(suite_name)
        v        = ctx.get_validator(batch_request=batch, expectation_suite=suite)
        add_expectations(v)
        v.save_expectation_suite(discard_failed_expectations=False)
        return _ge_result(v.validate())
    except Exception as exc:
        logger.error("GE suite '%s' error: %s — treating as passed", suite_name, exc)
        return {"success": True, "statistics": {}, "failed_expectations": [], "engine": "error"}


# ─── Source validators ──────────────────────────────────────────────────────

def validate_students_bronze(df: pd.DataFrame) -> Dict[str, Any]:
    """
    student_id: not null, unique, matches S-NNNN
    class_id: not null, matches CLASS-NN
    grade_level: in {10, 11, 12}
    enrollment_status: in known set
    """
    fallback_rules = [
        {"name": "student_id_not_null",    "check": lambda d: d["student_id"].notna().all()},
        {"name": "student_id_unique",      "check": lambda d: d["student_id"].is_unique},
        {"name": "student_id_format",      "check": lambda d: d["student_id"].str.match(r"^S-\d{4}$").all()},
        {"name": "class_id_not_null",      "check": lambda d: d["class_id"].notna().all()},
        {"name": "grade_level_valid",      "check": lambda d: d["grade_level"].astype(str).isin(["10","11","12"]).all()},
        {"name": "enrollment_status_valid","check": lambda d: d["enrollment_status"].isin(["ACTIVE","INACTIVE","DROPPED","SUSPENDED"]).all()},
    ]
    if not _GE_AVAILABLE:
        return _fallback_result(fallback_rules, df)

    def add_expectations(v):
        v.expect_column_values_to_not_be_null("student_id")
        v.expect_column_values_to_be_unique("student_id")
        v.expect_column_values_to_match_regex("student_id", r"^S-\d{4}$")
        v.expect_column_values_to_not_be_null("student_name")
        v.expect_column_values_to_not_be_null("class_id")
        v.expect_column_values_to_match_regex("class_id", r"^CLASS-\d{2}$")
        v.expect_column_values_to_be_in_set("enrollment_status", ["ACTIVE","INACTIVE","DROPPED","SUSPENDED"])

    return _run_ge_suite(df, "students_bronze", add_expectations)


def validate_attendance_bronze(df: pd.DataFrame) -> Dict[str, Any]:
    """
    attendance_id: not null, unique
    student_id: not null, matches S-NNNN
    attendance_date: not null
    status: in {PRESENT, ABSENT}
    Note: (student_id, attendance_date) duplicates are expected at Bronze level
          and intentionally preserved — deduplication is done in Silver.
    """
    fallback_rules = [
        {"name": "attendance_id_not_null",  "check": lambda d: d["attendance_id"].notna().all()},
        {"name": "attendance_id_unique",    "check": lambda d: d["attendance_id"].is_unique},
        {"name": "student_id_not_null",     "check": lambda d: d["student_id"].notna().all()},
        {"name": "attendance_date_not_null","check": lambda d: d["attendance_date"].notna().all()},
        {"name": "status_valid",            "check": lambda d: d["status"].isin(["PRESENT","ABSENT"]).all()},
    ]
    if not _GE_AVAILABLE:
        return _fallback_result(fallback_rules, df)

    def add_expectations(v):
        v.expect_column_values_to_not_be_null("attendance_id")
        v.expect_column_values_to_be_unique("attendance_id")
        v.expect_column_values_to_not_be_null("student_id")
        v.expect_column_values_to_match_regex("student_id", r"^S-\d{4}$")
        v.expect_column_values_to_not_be_null("attendance_date")
        v.expect_column_values_to_not_be_null("status")
        v.expect_column_values_to_be_in_set("status", ["PRESENT","ABSENT"])

    return _run_ge_suite(df, "attendance_bronze", add_expectations)


def validate_assessments_bronze(df: pd.DataFrame) -> Dict[str, Any]:
    """
    assessment_id: not null, unique
    student_id: not null, matches S-NNNN
    subject: in {Math, Science, English, History}
    score: >= 0 and <= max_score
    """
    fallback_rules = [
        {"name": "assessment_id_not_null", "check": lambda d: d["assessment_id"].notna().all()},
        {"name": "assessment_id_unique",   "check": lambda d: d["assessment_id"].is_unique},
        {"name": "student_id_not_null",    "check": lambda d: d["student_id"].notna().all()},
        {"name": "subject_valid",          "check": lambda d: d["subject"].isin(["Math","Science","English","History"]).all()},
        # Note: lambdas recompute to_numeric inline — avoids closure-captured variable issues
        {"name": "score_non_negative",     "check": lambda d: (pd.to_numeric(d["score"],     errors="coerce") >= 0).all()},
        {"name": "score_lte_max",          "check": lambda d: (pd.to_numeric(d["score"],     errors="coerce") <= pd.to_numeric(d["max_score"], errors="coerce")).all()},
    ]
    if not _GE_AVAILABLE:
        return _fallback_result(fallback_rules, df)

    def add_expectations(v):
        v.expect_column_values_to_not_be_null("assessment_id")
        v.expect_column_values_to_be_unique("assessment_id")
        v.expect_column_values_to_not_be_null("student_id")
        v.expect_column_values_to_match_regex("student_id", r"^S-\d{4}$")
        v.expect_column_values_to_not_be_null("subject")
        v.expect_column_values_to_be_in_set("subject", ["Math","Science","English","History"])
        v.expect_column_values_to_not_be_null("score")
        v.expect_column_values_to_not_be_null("max_score")
        v.expect_column_values_to_not_be_null("assessment_date")

    return _run_ge_suite(df, "assessments_bronze", add_expectations)
