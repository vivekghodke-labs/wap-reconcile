"""
Postgres connection pool for the WAP reconciliation framework.

Design decisions:
- ThreadedConnectionPool: framework is used inside Airflow (threaded
  executor) and pytest (parallel workers). A per-call connect() would
  exhaust Postgres connection limits under load. Pool is the only
  correct choice.
- Singleton pattern via module-level _pool: the pool is initialized
  once per process on first use (lazy init), not at import time. This
  means importing this module has zero side effects — no connection
  attempt, no env-var requirement — until get_connection() is first
  called. Critical for unit tests that never touch a DB.
- All configuration via environment variables — no hardcoded defaults
  for host/user/password. The only defaults are pool sizing, which are
  operational tuning knobs, not credentials.
- Context manager (get_connection) returns a connection and guarantees
  it is returned to the pool on exit, even on exception. Callers must
  not close the connection themselves — they must only commit/rollback,
  and let the context manager return it.

Environment variables:
    DATABASE_URL        Full DSN, e.g.:
                        postgresql://wap_user:secret@postgres:5432/wap_db
                        If set, takes precedence over individual vars.
    POSTGRES_HOST       Default: localhost
    POSTGRES_PORT       Default: 5432
    POSTGRES_DB         Required if DATABASE_URL not set
    POSTGRES_USER       Required if DATABASE_URL not set
    POSTGRES_PASSWORD   Required if DATABASE_URL not set
    WAP_POOL_MIN_CONN   Default: 2
    WAP_POOL_MAX_CONN   Default: 10
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

# ---------------------------------------------------------------------------
# Module-level pool state — do not access directly outside this module.
# ---------------------------------------------------------------------------

_pool: ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_dsn() -> str:
    """
    Resolve the Postgres DSN from environment variables.

    DATABASE_URL takes precedence. If absent, individual POSTGRES_*
    vars are assembled into a DSN. Raises EnvironmentError for any
    missing required variable so the failure is explicit at startup,
    not at first query execution.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    required = {
        "POSTGRES_DB": os.environ.get("POSTGRES_DB"),
        "POSTGRES_USER": os.environ.get("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables for Postgres connection: "
            f"{', '.join(missing)}. "
            "Set DATABASE_URL or all of POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD."
        )

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")

    return (
        f"postgresql://{required['POSTGRES_USER']}:{required['POSTGRES_PASSWORD']}"
        f"@{host}:{port}/{required['POSTGRES_DB']}"
    )


def _get_pool() -> ThreadedConnectionPool:
    """
    Return the singleton pool, initializing it on first call.

    Thread-safe: double-checked locking ensures only one thread
    initializes the pool even under concurrent first access.
    """
    global _pool
    if _pool is not None:
        return _pool

    with _pool_lock:
        # Re-check inside the lock — another thread may have initialized
        # while we were waiting.
        if _pool is not None:
            return _pool

        dsn = _build_dsn()
        min_conn = int(os.environ.get("WAP_POOL_MIN_CONN", "2"))
        max_conn = int(os.environ.get("WAP_POOL_MAX_CONN", "10"))

        try:
            _pool = ThreadedConnectionPool(min_conn, max_conn, dsn=dsn)
        except psycopg2.OperationalError as exc:
            raise ConnectionError(
                f"Failed to initialize Postgres connection pool. "
                f"DSN resolved (credentials redacted). "
                f"Underlying error: {exc}"
            ) from exc

    return _pool


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Acquire a connection from the pool and yield it as a context manager.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()

    The connection is returned to the pool on exit regardless of whether
    an exception was raised. Callers are responsible for commit/rollback
    — this manager does NOT auto-commit.

    Raises:
        ConnectionError: Pool not available or all connections exhausted.
    """
    pool = _get_pool()
    conn = None
    try:
        conn = pool.getconn()
        if conn is None:
            raise ConnectionError(
                "Connection pool exhausted — all connections are in use. "
                "Increase WAP_POOL_MAX_CONN or investigate connection leaks."
            )
        yield conn
    finally:
        if conn is not None:
            # putconn returns the connection to the pool.
            # close=False means the underlying TCP connection stays alive.
            pool.putconn(conn, close=False)


def close_pool() -> None:
    """
    Close all connections in the pool and reset the singleton.

    Called during application shutdown or between integration test runs
    to ensure a clean state. Safe to call if the pool was never
    initialized.
    """
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.closeall()
            _pool = None


def reset_pool() -> None:
    """
    Force-close the current pool and re-initialize on next get_connection().

    Used in integration tests to pick up a new DATABASE_URL after
    a testcontainer or compose service restarts. Not for production use.
    """
    close_pool()