"""
PostgresStagingWriter — Postgres-backed implementation of StagingWriter.

Writes arbitrary dict/list payloads to staging.records as JSONB.
Reads them back by staging_ref for assertion and publish steps.

Staging ref format:  staging://{dataset_key}/{run_id}
Published ref format: published://{dataset_key}/{run_id}
  (published_ref is produced by Publisher, not this class — documented
  here for cross-reference since the same run_id is shared.)

Design invariants:
- write() is NOT idempotent by default: two writes with the same
  (dataset_key, run_id) will raise StagingWriteError (unique constraint
  on staging_ref). Each pipeline run must use a fresh run_id via
  new_run_id(). This is intentional — silent overwrites in the staging
  area are how phantom-freshness bugs are introduced (exactly the
  article's FX scenario).
- Payload must be dict or list. Any other type raises StagingWriteError
  before touching the database. This enforces the JSONB constraint at
  the application layer, not just at query time, so the error message
  is actionable.
- All DB errors are wrapped in StagingWriteError so callers only need
  to handle one exception type from this class.
"""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from backends.postgres.connection import get_connection
from core.staging import StagingWriteError, StagingWriter

# Matches staging://{dataset_key}/{run_id}
_STAGING_REF_PATTERN = re.compile(
    r"^staging://(?P<dataset_key>[^/]+)/(?P<run_id>[0-9a-f-]{36})$"
)


def _parse_staging_ref(staging_ref: str) -> tuple[str, str]:
    """
    Parse a staging_ref string into (dataset_key, run_id).

    Raises:
        StagingWriteError: staging_ref does not match expected format.
    """
    match = _STAGING_REF_PATTERN.match(staging_ref)
    if not match:
        raise StagingWriteError(
            f"Invalid staging_ref format: '{staging_ref}'. "
            "Expected: staging://{{dataset_key}}/{{run_id}}"
        )
    return match.group("dataset_key"), match.group("run_id")


def _validate_payload(data: Any) -> None:
    """
    Enforce that payload is a dict or list before any DB round-trip.

    psycopg2's Json() adapter will accept almost anything and silently
    serialize it — including raw strings and numbers — which can produce
    JSONB rows that assertions cannot sensibly operate on. Reject early
    with a clear message.
    """
    if not isinstance(data, (dict, list)):
        raise StagingWriteError(
            f"Payload must be a dict or list for JSONB storage. "
            f"Got: {type(data).__name__}. "
            "Wrap scalar values in a dict, e.g. {{'value': data}}."
        )


def _validate_dataset_key(key: str) -> None:
    """Dataset key must be a non-empty string with no path separators."""
    if not key or not isinstance(key, str):
        raise StagingWriteError("dataset_key must be a non-empty string.")
    if "/" in key or "\\" in key:
        raise StagingWriteError(
            f"dataset_key must not contain '/' or '\\'. Got: '{key}'."
        )


def _validate_run_id(run_id: str) -> None:
    """run_id must be a valid UUID4 string."""
    try:
        UUID(run_id, version=4)
    except (ValueError, AttributeError) as exc:
        raise StagingWriteError(
            f"run_id must be a valid UUID4 string. Got: '{run_id}'."
        ) from exc


class PostgresStagingWriter(StagingWriter):
    """
    Postgres-backed StagingWriter.

    Args:
        run_id: UUID4 string identifying this pipeline run. Obtain via
                core.models.new_run_id(). Must be the same run_id used
                across write(), assertion reads, and the publish step so
                audit trail entries are correlated.

    Example:
        writer = PostgresStagingWriter(run_id=new_run_id())
        staging_ref = writer.write("fx_rates", {"USD_GBP": 0.79, ...})
        data = writer.read(staging_ref)
    """

    def __init__(self, run_id: str) -> None:
        _validate_run_id(run_id)
        self._run_id = run_id

    # ------------------------------------------------------------------
    # StagingWriter interface
    # ------------------------------------------------------------------

    def write(self, key: str, data: Any) -> str:
        """
        Write `data` to staging.records under `key` for this run.

        Returns:
            staging_ref: "staging://{key}/{run_id}"

        Raises:
            StagingWriteError: payload is not dict/list, key is invalid,
                               run_id already exists for this key
                               (unique constraint), or any DB error.
        """
        _validate_dataset_key(key)
        _validate_payload(data)

        staging_ref = f"staging://{key}/{self._run_id}"

        sql = """
            INSERT INTO staging.records
                (staging_ref, dataset_key, run_id, payload)
            VALUES
                (%(staging_ref)s, %(dataset_key)s, %(run_id)s::uuid, %(payload)s)
        """
        try:
            with get_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "staging_ref": staging_ref,
                        "dataset_key": key,
                        "run_id": self._run_id,
                        "payload": Json(data),
                    },
                )
                conn.commit()
        except psycopg2.errors.UniqueViolation as exc:
            raise StagingWriteError(
                f"A staged record already exists for dataset_key='{key}' "
                f"run_id='{self._run_id}'. Each pipeline run must use a "
                "fresh run_id via new_run_id()."
            ) from exc
        except psycopg2.Error as exc:
            raise StagingWriteError(
                f"Failed to write staged record for dataset_key='{key}': {exc}"
            ) from exc

        return staging_ref

    def read(self, staging_ref: str) -> dict[str, Any] | list[Any]:
        """
        Read back a previously staged payload by its staging_ref.

        Returns:
            The original payload as dict or list (round-trip fidelity
            guaranteed — JSONB deserialization returns the same structure
            that was written).

        Raises:
            StagingWriteError: staging_ref not found, invalid format,
                               or any DB error.
        """
        _dataset_key, _run_id = _parse_staging_ref(staging_ref)

        sql = """
            SELECT payload
            FROM staging.records
            WHERE staging_ref = %(staging_ref)s
        """
        try:
            with (
                get_connection() as conn,
                conn.cursor(cursor_factory=RealDictCursor) as cur,
            ):
                cur.execute(sql, {"staging_ref": staging_ref})
                row = cur.fetchone()
        except psycopg2.Error as exc:
            raise StagingWriteError(
                f"Failed to read staged record '{staging_ref}': {exc}"
            ) from exc

        if row is None:
            raise StagingWriteError(
                f"No staged record found for staging_ref='{staging_ref}'. "
                "Ensure write() was called with the same run_id before read()."
            )

        return row["payload"]

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def run_id(self) -> str:
        return self._run_id

    def staging_ref_for(self, key: str) -> str:
        """
        Compute the staging_ref for a given key without writing.
        Useful for pre-computing refs in orchestration layers.
        """
        _validate_dataset_key(key)
        return f"staging://{key}/{self._run_id}"
