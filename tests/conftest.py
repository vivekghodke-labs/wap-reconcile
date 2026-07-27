"""
Integration test fixtures for the WAP reconciliation framework.

These tests require a running Postgres instance. In CI and local dev,
this is provided by the `postgres` service in docker-compose.yml.

The DATABASE_URL is set via the environment (docker-compose injects it;
locally, source .env before running pytest).

Fixture hierarchy:
    db_session (session-scoped)
        Runs migrations once per test session. Provides a raw psycopg2
        connection for direct SQL in fixture setup/teardown.

    clean_db (function-scoped)
        Truncates all WAP tables between tests. Guarantees each test
        starts from a known empty state without re-running migrations
        (which would be expensive and test-order-dependent).

    staging_writer (function-scoped)
        A fresh PostgresStagingWriter with a new run_id for each test.
"""

from __future__ import annotations

import os

import pytest

from backends.postgres.connection import get_connection, reset_pool
from backends.postgres.staging_writer import PostgresStagingWriter
from core.models import new_run_id
from db.migrations.run_migrations import run_migrations

# ---------------------------------------------------------------------------
# Session-scoped: run migrations once for the entire test session
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def db_session():
    """
    Ensure the database schema is up to date before any integration test runs.

    This fixture:
    1. Resets the connection pool so it picks up the DATABASE_URL from
       the current environment (important when running under docker-compose
       vs. locally with a different host).
    2. Runs all pending migrations (idempotent — safe to call every session).
    3. Yields a direct psycopg2 connection for fixtures that need raw SQL.
    4. Closes the pool after all tests complete.
    """
    # Validate that DATABASE_URL is set — fail early with a clear message
    # rather than a cryptic connection error deep in the test run.
    if not os.environ.get("DATABASE_URL") and not os.environ.get("POSTGRES_DB"):
        pytest.skip(
            "Integration tests require DATABASE_URL or POSTGRES_* env vars. "
            "Run via docker-compose (docker compose run --rm test) or "
            "source .env before running pytest."
        )

    reset_pool()  # Ensure pool picks up current env, not a stale one
    run_migrations()

    with get_connection() as conn:
        yield conn

    reset_pool()


# ---------------------------------------------------------------------------
# Function-scoped: clean slate between tests
# ---------------------------------------------------------------------------

# Tables to truncate between tests, in dependency order.
# CASCADE handles FK constraints; RESTART IDENTITY resets sequences.
_TRUNCATE_TABLES = [
    "wap_review_queue",
    "wap_audit_log",
    "published.records",
    "staging.records",
]


@pytest.fixture()
def clean_db(db_session):
    """
    Truncate all WAP data tables before each test.

    Yields the session-level connection for tests that need direct SQL
    access (e.g. asserting on table row counts after an operation).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in _TRUNCATE_TABLES:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
        conn.commit()

    yield db_session


# ---------------------------------------------------------------------------
# Convenience fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def run_id() -> str:
    """A fresh UUID4 run_id for each test."""
    return new_run_id()


@pytest.fixture()
def staging_writer(run_id: str, clean_db) -> PostgresStagingWriter:
    """
    A PostgresStagingWriter with a fresh run_id, against a clean database.

    Depends on clean_db to ensure no stale staging records from prior tests
    interfere with unique constraint checks.
    """
    return PostgresStagingWriter(run_id=run_id)
