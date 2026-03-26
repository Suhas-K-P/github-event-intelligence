"""
api/database.py
────────────────
MySQL connection pool for the FastAPI layer.

Uses mysql-connector-python's built-in pooling so requests share a small
set of persistent connections rather than opening a new socket per query.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config

log = logging.getLogger("api.database")

# ─── Connection Pool ─────────────────────────────────────────────────────────
_pool: MySQLConnectionPool | None = None


def get_pool() -> MySQLConnectionPool:
    """Lazily create and return the singleton connection pool."""
    global _pool
    if _pool is None:
        _pool = MySQLConnectionPool(
            pool_name="event_intel_pool",
            pool_size=5,
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME,
            charset="utf8mb4",
            autocommit=True,
            connection_timeout=10,
        )
        log.info("MySQL pool created (size=5, db=%s)", config.DB_NAME)
    return _pool


@contextmanager
def get_cursor() -> Generator[mysql.connector.cursor.MySQLCursor, None, None]:
    """Context manager that yields a cursor and handles cleanup."""
    conn   = get_pool().get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


# ─── Query Helpers ────────────────────────────────────────────────────────────

def fetchall(sql: str, params: tuple = ()) -> list[dict]:
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() or []


def fetchone(sql: str, params: tuple = ()) -> dict | None:
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def execute(sql: str, params: tuple = ()) -> int:
    """Execute a write statement; return rowcount."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount