"""
ReferenceSource contract + SnapshotReferenceSource implementation.

The contract (ReferenceSource ABC) is a Day 1 deliverable.
SnapshotReferenceSource is the Day 2 concrete implementation that reads
from the published.latest_by_dataset view — i.e. "the reference for
today's run is whatever was last successfully published for this
dataset_key."

This is the most universally applicable reference pattern: it requires
zero external systems, works for any dataset, and directly encodes the
WAP invariant — if nothing has ever been published, the reference is
unavailable (ReferenceResolutionError), which is the correct behaviour:
a first-ever run has no prior truth to reconcile against, and the
pipeline must be told that explicitly rather than silently skipping
verification.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from backends.postgres.connection import get_connection


class ReferenceResolutionError(Exception):
    """
    Raised when a reference source cannot produce data for a given key
    (e.g. no prior snapshot exists, upstream API unreachable).

    This is deliberately a distinct exception type — the pipeline must
    be able to tell "reference unavailable" apart from "assertion
    failed" and route them differently (ERRORED vs. ROUTED_TO_REVIEW).
    """


class ReferenceSource(ABC):
    """
    Base class for all pluggable reference sources.

    `key` identifies which logical dataset/table/entity is being
    reconciled (e.g. "fx_rates", "well_production_daily"). Its meaning
    is defined by the concrete implementation, not by this interface.
    """

    @abstractmethod
    def resolve(self, key: str) -> Any:
        """
        Return the independent reference data for `key`.

        Must raise ReferenceResolutionError (not return None or an
        empty result silently) if the reference cannot be produced.
        A silent empty reference is how meaning-preserving bugs slip
        through assertions that don't defensively check for it.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Day 2 — Concrete Implementation
# ---------------------------------------------------------------------------


class SnapshotReferenceSource(ReferenceSource):
    """
    Resolves reference data from the most recently published snapshot
    for a given dataset_key.

    Reads from the published.latest_by_dataset view, which is defined
    in db/migrations/001_staging_published.sql as:

        SELECT DISTINCT ON (dataset_key)
            dataset_key, published_ref, payload, published_at
        FROM published.records
        ORDER BY dataset_key, published_at DESC;

    Design contract:
    - Returns the raw `payload` (dict) from the last published record.
    - Raises ReferenceResolutionError if no published record exists for
      the key — never returns None or an empty dict silently.
    - Uses a read-only query; never mutates published data.

    Thread safety: each resolve() call acquires/releases its own
    connection from the pool via the context manager. Safe for
    concurrent pipeline runs resolving the same or different keys.
    """

    def resolve(self, key: str) -> dict[str, Any]:
        """
        Return the payload of the most recently published record for `key`.

        Args:
            key: Dataset identifier (e.g. "fx_rates", "well_production_daily").

        Returns:
            dict — the JSONB payload stored at last publish time.

        Raises:
            ReferenceResolutionError: No prior published record exists for `key`.
            ReferenceResolutionError: Database query fails.
        """
        sql = """
            SELECT payload, published_at
            FROM published.latest_by_dataset
            WHERE dataset_key = %(key)s
        """
        try:
            with (
                get_connection() as conn,
                conn.cursor(cursor_factory=RealDictCursor) as cur,
            ):
                cur.execute(sql, {"key": key})
                row = cur.fetchone()
        except psycopg2.Error as exc:
            raise ReferenceResolutionError(
                f"Database error resolving reference for '{key}': {exc}"
            ) from exc

        if row is None:
            raise ReferenceResolutionError(
                f"No prior published snapshot found for dataset_key='{key}'. "
                "This is expected on the very first run — seed an initial "
                "published record or use a different ReferenceSource for "
                "bootstrap scenarios."
            )

        return dict(row["payload"])
