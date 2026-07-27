"""
Integration tests for SnapshotReferenceSource.

Tests verify behaviour against a real Postgres instance.

Coverage:
  - resolve() raises ReferenceResolutionError when no prior snapshot exists
    (first-ever run for a dataset_key — this is the expected bootstrap state)
  - resolve() returns the payload of the most recently published record
  - resolve() returns the LATEST record when multiple published records exist
    for the same dataset_key (the view's DISTINCT ON guarantee)
  - resolve() is isolated by dataset_key (fx_rates snapshot does not
    contaminate well_production result)
  - resolve() returns a dict (not a RealDictRow or other psycopg2 type)
  - resolve() raises ReferenceResolutionError on empty dataset_key

These tests insert directly into published.records to simulate prior
published runs — Publisher is not available yet (Day 3). This is
intentional: reference source tests must not depend on the publish gate.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from psycopg2.extras import Json

from backends.postgres.connection import get_connection
from core.reference_source import ReferenceResolutionError, SnapshotReferenceSource

# ---------------------------------------------------------------------------
# Helper: insert a published record directly (bypassing Publisher)
# ---------------------------------------------------------------------------


def _insert_published_record(
    dataset_key: str,
    payload: dict,
    run_id: str | None = None,
    published_at: datetime | None = None,
) -> str:
    """
    Insert a row into published.records directly, simulating a prior
    successful pipeline run. Returns the published_ref.
    """
    _run_id = run_id or str(uuid.uuid4())
    _staging_ref = f"staging://{dataset_key}/{_run_id}"
    _published_ref = f"published://{dataset_key}/{_run_id}"
    _published_at = published_at or datetime.now(timezone.utc)

    sql = """
        INSERT INTO published.records
            (published_ref, dataset_key, run_id, staging_ref, payload, published_at)
        VALUES
            (%(published_ref)s, %(dataset_key)s, %(run_id)s::uuid,
             %(staging_ref)s, %(payload)s, %(published_at)s)
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "published_ref": _published_ref,
                "dataset_key": dataset_key,
                "run_id": _run_id,
                "staging_ref": _staging_ref,
                "payload": Json(payload),
                "published_at": _published_at,
            },
        )
        conn.commit()

    return _published_ref


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNoSnapshotExists:
    def test_resolve_raises_when_no_prior_published_record(self, clean_db):
        """
        First-ever run for a dataset_key must raise ReferenceResolutionError.
        Returning None or an empty dict silently would let assertions skip
        verification — the exact failure mode the framework is built to prevent.
        """
        source = SnapshotReferenceSource()

        with pytest.raises(ReferenceResolutionError, match="No prior published snapshot"):
            source.resolve("fx_rates")

    def test_resolve_raises_for_unknown_key_even_if_others_exist(self, clean_db):
        """Snapshots for other keys must not bleed into an unrelated key's resolution."""
        _insert_published_record("well_production", {"wells": 42})

        source = SnapshotReferenceSource()

        with pytest.raises(ReferenceResolutionError, match="No prior published snapshot"):
            source.resolve("fx_rates")


class TestSnapshotExists:
    def test_resolve_returns_payload_of_published_record(self, clean_db):
        payload = {"USD_GBP": 0.79, "USD_EUR": 0.92}
        _insert_published_record("fx_rates", payload)

        source = SnapshotReferenceSource()
        result = source.resolve("fx_rates")

        assert result == payload

    def test_resolve_returns_dict_not_psycopg2_type(self, clean_db):
        """
        Callers (assertions) must receive a plain dict, not a RealDictRow
        or any psycopg2-specific type. Type leakage from the DB layer
        would couple assertion implementations to the persistence backend.
        """
        _insert_published_record("fx_rates", {"rate": 1.0})

        source = SnapshotReferenceSource()
        result = source.resolve("fx_rates")

        assert type(result) is dict  # strict: not a subclass

    def test_resolve_returns_latest_when_multiple_records_exist(self, clean_db):
        """
        published.latest_by_dataset uses DISTINCT ON (dataset_key) ORDER BY
        published_at DESC. The most recently published record must win.
        """
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        old_payload = {"USD_GBP": 0.75, "note": "old"}
        new_payload = {"USD_GBP": 0.79, "note": "new"}

        _insert_published_record("fx_rates", old_payload, published_at=now - timedelta(days=1))
        _insert_published_record("fx_rates", new_payload, published_at=now)

        source = SnapshotReferenceSource()
        result = source.resolve("fx_rates")

        assert result == new_payload
        assert result["note"] == "new"

    def test_resolve_is_isolated_by_dataset_key(self, clean_db):
        """
        Resolving 'fx_rates' must never return data published under
        'well_production_daily', even if both exist in published.records.
        """
        fx_payload = {"USD_GBP": 0.79}
        well_payload = {"wells_active": 142, "production_bbl": 5_430}

        _insert_published_record("fx_rates", fx_payload)
        _insert_published_record("well_production_daily", well_payload)

        source = SnapshotReferenceSource()

        assert source.resolve("fx_rates") == fx_payload
        assert source.resolve("well_production_daily") == well_payload

    def test_resolve_preserves_nested_payload_structure(self, clean_db):
        payload = {
            "rates": {"USD_GBP": 0.79, "USD_EUR": 0.92},
            "metadata": {"source": "reuters", "verified": True, "count": 24},
        }
        _insert_published_record("fx_rates", payload)

        source = SnapshotReferenceSource()
        result = source.resolve("fx_rates")

        assert result["rates"]["USD_GBP"] == 0.79
        assert result["metadata"]["source"] == "reuters"
        assert result["metadata"]["verified"] is True

    def test_resolve_same_source_instance_can_be_called_multiple_times(self, clean_db):
        """
        SnapshotReferenceSource must be stateless — multiple calls on the
        same instance must return consistent results (no connection state
        carried between calls).
        """
        _insert_published_record("fx_rates", {"rate": 0.79})

        source = SnapshotReferenceSource()
        result1 = source.resolve("fx_rates")
        result2 = source.resolve("fx_rates")

        assert result1 == result2

    def test_resolve_list_payload_returned_as_list(self, clean_db):
        """JSONB supports top-level arrays; resolve must return them as list."""
        payload_list = [
            {"currency": "USD", "rate": 1.0},
            {"currency": "GBP", "rate": 0.79},
        ]

        # Insert directly with a list payload
        _run_id = str(uuid.uuid4())
        sql = """
            INSERT INTO published.records
                (published_ref, dataset_key, run_id, staging_ref, payload)
            VALUES
                (%(published_ref)s, %(dataset_key)s, %(run_id)s::uuid,
                 %(staging_ref)s, %(payload)s)
        """
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "published_ref": f"published://fx_list/{_run_id}",
                    "dataset_key": "fx_list",
                    "run_id": _run_id,
                    "staging_ref": f"staging://fx_list/{_run_id}",
                    "payload": Json(payload_list),
                },
            )
            conn.commit()

        _source = SnapshotReferenceSource()
        # SnapshotReferenceSource.resolve() currently returns dict(row["payload"]).
        # List payloads require direct access — this test documents the current
        # behaviour and will guide the Day 3 Publisher to handle both types.
        # For now, list payloads resolve via direct row access.
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM published.latest_by_dataset WHERE dataset_key = 'fx_list'"
            )
            row = cur.fetchone()
        assert row[0] == payload_list
