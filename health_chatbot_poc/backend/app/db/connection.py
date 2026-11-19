"""Postgres access via a shared psycopg connection pool."""

from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from backend.app.config import get_settings

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


@lru_cache
def get_pool() -> ConnectionPool:
    pool = ConnectionPool(
        get_settings().database_url,
        min_size=1,
        max_size=10,
        kwargs={"autocommit": True, "row_factory": dict_row},
        open=False,
    )
    pool.open(wait=True, timeout=30)
    return pool


@contextmanager
def conn() -> Iterator[Any]:
    with get_pool().connection() as c:
        yield c


def fetch_all(sql: str, params: tuple | dict = ()) -> list[dict]:
    with conn() as c:
        return c.execute(sql, params).fetchall()


def fetch_one(sql: str, params: tuple | dict = ()) -> dict | None:
    with conn() as c:
        return c.execute(sql, params).fetchone()


def init_schema() -> None:
    with conn() as c:
        c.execute(SCHEMA_FILE.read_text())
