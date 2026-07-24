"""
WAP migration runner.

Applies SQL migration files in lexicographic order (001_, 002_, ...).
Each migration is tracked in a wap_migrations table; already-applied
migrations are skipped. Every migration runs in its own transaction —
a failure rolls back that migration only and halts the runner.

Usage:
    python -m db.migrations.run_migrations

    Or from application startup:
        from db.migrations.run_migrations import run_migrations
        run_migrations()

Environment:
    DATABASE_URL or POSTGRES_* vars (see backends/postgres/connection.py)

Design:
- Zero external dependency: no Alembic, no Flyway, no Liquibase.
  The runner is ~80 lines of standard library + psycopg2.
- Idempotent: safe to call on every container start. Already-applied
  migrations are skipped, not re-applied.
- Ordered: files are applied in filename order. Prefix convention
  (001_, 002_, ...) guarantees deterministic ordering on any OS.
- Atomic per migration: each file is a single transaction. A partial
  migration leaves the schema in a known bad state (transaction rolled
  back) rather than a silently partial one.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import psycopg2

from backends.postgres.connection import get_connection

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent

_CREATE_TRACKING_TABLE = """
    CREATE TABLE IF NOT EXISTS wap_migrations (
        id          SERIAL      PRIMARY KEY,
        filename    TEXT        NOT NULL UNIQUE,
        applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
"""

_CHECK_APPLIED = """
    SELECT 1 FROM wap_migrations WHERE filename = %(filename)s
"""

_RECORD_APPLIED = """
    INSERT INTO wap_migrations (filename) VALUES (%(filename)s)
    ON CONFLICT (filename) DO NOTHING
"""


def _get_migration_files() -> list[Path]:
    """
    Return all .sql files in the migrations directory, sorted by name.
    run_migrations.py itself is excluded.
    """
    files = sorted(
        p for p in MIGRATIONS_DIR.glob("*.sql")
        if p.is_file()
    )
    return files


def run_migrations(migrations_dir: Path | None = None) -> None:
    """
    Apply all pending migrations in order.

    Args:
        migrations_dir: Override the default migrations directory.
                        Used in tests to point at a fixture directory.

    Raises:
        psycopg2.Error: Any DB error during migration execution.
                        The offending migration is rolled back; prior
                        migrations in this call are committed.
        FileNotFoundError: migrations_dir does not exist.
    """
    target_dir = migrations_dir or MIGRATIONS_DIR
    if not target_dir.exists():
        raise FileNotFoundError(f"Migrations directory not found: {target_dir}")

    migration_files = sorted(
        p for p in target_dir.glob("*.sql") if p.is_file()
    )

    if not migration_files:
        logger.info("No migration files found in %s — nothing to apply.", target_dir)
        return

    with get_connection() as conn:
        # Ensure tracking table exists — this is the one DDL statement
        # that runs outside a per-migration transaction.
        with conn.cursor() as cur:
            cur.execute(_CREATE_TRACKING_TABLE)
        conn.commit()

        for migration_path in migration_files:
            filename = migration_path.name

            with conn.cursor() as cur:
                cur.execute(_CHECK_APPLIED, {"filename": filename})
                already_applied = cur.fetchone() is not None

            if already_applied:
                logger.debug("Skipping already-applied migration: %s", filename)
                continue

            sql = migration_path.read_text(encoding="utf-8")
            logger.info("Applying migration: %s", filename)

            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(_RECORD_APPLIED, {"filename": filename})
                conn.commit()
                logger.info("Migration applied successfully: %s", filename)
            except psycopg2.Error as exc:
                conn.rollback()
                logger.error(
                    "Migration failed: %s — rolled back. Error: %s",
                    filename, exc
                )
                raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_migrations()
    logger.info("All migrations complete.")