from __future__ import annotations
import sqlite3
from typing import Any, Tuple

from config import DATABASE_URL, DB_PATH

IS_POSTGRES = bool(DATABASE_URL)


def _pg_connect():
    import psycopg
    from psycopg.rows import dict_row

    url = DATABASE_URL
    if url and "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}sslmode=require"
    return psycopg.connect(url, row_factory=dict_row)


def db_connect():
    if IS_POSTGRES:
        return _pg_connect()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def db_exec(sql: str, params: Tuple[Any, ...] = ()):
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        con.commit()
        return cur
    finally:
        try:
            con.close()
        except Exception:
            pass


def db_fetchone(sql: str, params: Tuple[Any, ...] = ()):
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        try:
            con.close()
        except Exception:
            pass


def db_fetchall(sql: str, params: Tuple[Any, ...] = ()):
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        try:
            con.close()
        except Exception:
            pass


def db_insert(sql: str, params: Tuple[Any, ...] = ()) -> int:
    """Execute an INSERT using ? placeholders and return the new row id."""
    con = db_connect()
    try:
        cur = con.cursor()
        if IS_POSTGRES:
            pg_sql = psql(sql)
            if "RETURNING" not in pg_sql.upper():
                pg_sql = pg_sql.rstrip("; ") + " RETURNING id"
            cur.execute(pg_sql, params)
            row_id = int(cur.fetchone()["id"])
        else:
            cur.execute(sql, params)
            row_id = cur.lastrowid
        con.commit()
        return row_id
    finally:
        try:
            con.close()
        except Exception:
            pass


def psql(sql: str) -> str:
    """Convierte placeholders ? (sqlite) a %s (postgres)."""
    if not IS_POSTGRES:
        return sql
    return sql.replace("?", "%s")
