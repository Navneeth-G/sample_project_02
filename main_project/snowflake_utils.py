"""Utility functions for interacting with Snowflake."""

import os
import time
from typing import Optional

import snowflake.connector
from airflow.models import Variable


def _get_connection():
    """Return a Snowflake connection using env vars or Airflow variables."""
    creds = {
        "user": Variable.get("SNOWFLAKE_USER", default_var=os.getenv("SNOWFLAKE_USER")),
        "password": Variable.get("SNOWFLAKE_PASSWORD", default_var=os.getenv("SNOWFLAKE_PASSWORD")),
        "account": Variable.get("SNOWFLAKE_ACCOUNT", default_var=os.getenv("SNOWFLAKE_ACCOUNT")),
        "warehouse": Variable.get("SNOWFLAKE_WAREHOUSE", default_var=os.getenv("SNOWFLAKE_WAREHOUSE")),
        "database": Variable.get("SNOWFLAKE_DATABASE", default_var=os.getenv("SNOWFLAKE_DATABASE")),
        "schema": Variable.get("SNOWFLAKE_SCHEMA", default_var=os.getenv("SNOWFLAKE_SCHEMA")),
    }
    retries = 3
    delay = 1
    for attempt in range(retries):
        try:
            return snowflake.connector.connect(**creds)
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2


def update_audit_status(
     index_name: str,
     date: str,
     status: str,
     source_count: Optional[int] = None,
     target_count: Optional[int] = None,
     retry_number: int = 0,
     error_message: Optional[str] = None,
     audit_table: Optional[str] = None,
 ) -> None:
     """Insert or update a row in the audit table.

     Parameters correspond to columns in the audit table.
     """

     audit_table = audit_table or Variable.get(
         "SNOWFLAKE_AUDIT_TABLE", default_var=os.getenv("SNOWFLAKE_AUDIT_TABLE")
     )

     conn = _get_connection()
     cur = conn.cursor()

     params = {
         "index_name": index_name,
         "date": date,
         "status": status,
         "source_count": source_count,
         "target_count": target_count,
         "retry_number": retry_number,
         "error_message": error_message,
     }

     try:
         cur.execute(
             f"SELECT COUNT(1) FROM {audit_table} WHERE index_name=%(index_name)s AND elt_date=%(date)s",
             params,
         )
         exists = cur.fetchone()[0] > 0

         if exists:
             cur.execute(
                 f"""
                 UPDATE {audit_table}
                 SET status=%(status)s,
                     source_count=COALESCE(%(source_count)s, source_count),
                     target_count=COALESCE(%(target_count)s, target_count),
                     retry_number=%(retry_number)s,
                     error_message=%(error_message)s
                 WHERE index_name=%(index_name)s
                   AND elt_date=%(date)s
                 """,
                 params,
             )
         else:
             cur.execute(
                 f"""
                 INSERT INTO {audit_table}
                     (index_name, elt_date, status, source_count, target_count, retry_number, error_message)
                 VALUES (%(index_name)s, %(date)s, %(status)s, %(source_count)s, %(target_count)s, %(retry_number)s, %(error_message)s)
                 """,
                 params,
             )
         conn.commit()
     finally:
         cur.close()
         conn.close()


def get_snowflake_row_count(
     index_name: str,
     date: str,
     target_table: Optional[str] = None,
 ) -> int:
     """Return the row count for the given index and date in the target table."""

     target_table = target_table or Variable.get(
         "SNOWFLAKE_TARGET_TABLE", default_var=os.getenv("SNOWFLAKE_TARGET_TABLE")
     )

     conn = _get_connection()
     cur = conn.cursor()
     try:
         cur.execute(
             f"SELECT COUNT(*) FROM {target_table} WHERE index_name=%(index_name)s AND elt_date=%(date)s",
             {"index_name": index_name, "date": date},
         )
         result = cur.fetchone()
         return int(result[0]) if result else 0
     finally:
         cur.close()
         conn.close()


def delete_snowflake_records(
     index_name: str,
     date: str,
     target_table: Optional[str] = None,
 ) -> None:
     """Delete records from the target table for a given index and date."""

     target_table = target_table or Variable.get(
         "SNOWFLAKE_TARGET_TABLE", default_var=os.getenv("SNOWFLAKE_TARGET_TABLE")
     )

     conn = _get_connection()
     cur = conn.cursor()
     try:
         cur.execute(
             f"DELETE FROM {target_table} WHERE index_name=%(index_name)s AND elt_date=%(date)s",
             {"index_name": index_name, "date": date},
         )
         conn.commit()
     finally:
         cur.close()
         conn.close()
