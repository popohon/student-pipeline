
import os
import logging
from typing import List

import psycopg2
import psycopg2.extras
import pandas as pd

logger = logging.getLogger(__name__)


def get_gold_connection():
    """Return a psycopg2 connection to the Gold DB."""
    return psycopg2.connect(
        host=os.environ.get("GOLD_DB_HOST", "postgres"),
        port=int(os.environ.get("GOLD_DB_PORT", "5432")),
        dbname=os.environ.get("GOLD_DB_NAME", "gold_db"),
        user=os.environ.get("GOLD_DB_USER", "gold"),
        password=os.environ.get("GOLD_DB_PASSWORD", "gold"),
    )


def truncate_and_insert(conn, schema: str, table: str, df: pd.DataFrame) -> int:
    """
    TRUNCATE a table then INSERT all rows from a DataFrame.
    Idempotent for full-batch reloads (runs daily from a static source).
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping %s.%s", schema, table)
        return 0

    # Convert any pandas Timestamp columns to Python datetime (psycopg2 handles them natively)
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].dt.to_pydatetime()

    columns = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE")
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO {schema}.{table} ({col_list}) VALUES %s",
            values,
            template=f"({placeholders})",
            page_size=1000,
        )
        count = cur.rowcount

    conn.commit()
    logger.info("Loaded %d rows into %s.%s", count, schema, table)
    return count


def upsert_dataframe(
    conn, schema: str, table: str, df: pd.DataFrame, primary_keys: List[str]
) -> int:
    """
    INSERT … ON CONFLICT DO UPDATE for incremental / idempotent loads.
    Useful when you want to append without full truncation.
    """
    if df.empty:
        return 0

    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].dt.to_pydatetime()

    columns = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)

    update_cols = [c for c in columns if c not in primary_keys]
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    conflict_str = ", ".join(primary_keys)

    query = (
        f"INSERT INTO {schema}.{table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
    )

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, query, values,
            template=f"({placeholders})",
            page_size=1000,
        )
        count = cur.rowcount

    conn.commit()
    logger.info("Upserted %d rows into %s.%s", count, schema, table)
    return count


def execute_sql(conn, sql: str) -> int:
    """Execute a raw SQL statement and return rowcount."""
    with conn.cursor() as cur:
        cur.execute(sql)
        count = cur.rowcount
    conn.commit()
    return count
