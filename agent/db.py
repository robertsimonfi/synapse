"""
Database connection layer.
Supports Oracle (via oracledb) and SQLite (for local dev/testing).
Set DB_TYPE=oracle or DB_TYPE=sqlite in your .env file.
"""

import os
import json
from typing import Any
from dotenv import load_dotenv

load_dotenv()

DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()


def _adapt_sql(sql: str) -> str:
    """
    Translate Oracle-specific SQL to SQLite syntax when DB_TYPE is sqlite.
    Handles:  FETCH FIRST N ROWS ONLY  →  LIMIT N
    """
    if DB_TYPE != "sqlite":
        return sql
    import re
    # FETCH FIRST :lim ROWS ONLY  →  LIMIT :lim
    sql = re.sub(
        r"\bFETCH\s+FIRST\s+(:?\w+)\s+ROWS\s+ONLY\b",
        r"LIMIT \1",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def get_connection():
    """Return a DB-API 2.0 compatible connection."""
    if DB_TYPE == "oracle":
        import oracledb
        return oracledb.connect(
            user=os.getenv("ORA_USER"),
            password=os.getenv("ORA_PASSWORD"),
            dsn=os.getenv("ORA_DSN"),   # e.g. "host:1521/XEPDB1"
        )
    else:
        import sqlite3
        db_path = os.getenv("SQLITE_PATH", "telco_dev.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn


def query(sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return results as a list of dicts.
    Uses named bind variables (:name for Oracle, :name for SQLite).
    """
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(_adapt_sql(sql), params or {})
        cols = [d[0].lower() for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return rows
    finally:
        cursor.close()
        conn.close()


def query_one(sql: str, params: dict | None = None) -> dict[str, Any] | None:
    rows = query(sql, params)
    return rows[0] if rows else None


def rows_to_json(rows: list[dict]) -> str:
    """Serialize query results for the LLM — dates to str, truncate CLOBs."""
    def _clean(v):
        if v is None:
            return None
        if hasattr(v, "read"):          # Oracle CLOB
            return v.read()[:500] + "..." if len(v.read()) > 500 else v.read()
        return str(v) if not isinstance(v, (int, float, bool, str)) else v
    return json.dumps([{k: _clean(v) for k, v in row.items()} for row in rows], indent=2)
