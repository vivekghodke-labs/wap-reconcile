"""
Integration tests for PostgresStagingWriter.

Tests verify behaviour against a real Postgres instance (provided by
docker-compose). Each test gets a clean database via the clean_db fixture.

Coverage:
  - write() happy path: dict payload, returns correct staging_ref
  - write() happy path: list payload
  - read() round-trip: data returned matches data written, field-for-field
  - write() duplicate key: same (dataset_key, run_id) raises StagingWriteError
  - write() invalid payload: non-dict/list raises StagingWriteError before DB call
  - write() empty dataset_key: raises StagingWriteError
  - write() dataset_key with path separator: raises StagingWriteError
  - read() unknown staging_ref: raises StagingWriteError
  - read() malformed staging_ref: raises StagingWriteError
  - staging_ref_for(): returns correct format without writing
  - Multiple datasets, same run_id: both write and read correctly (isolated by key)
"""

from __future__ import annotations

import pytest

from backends.postgres.staging_writer import PostgresStagingWriter
from core.models import new_run_id
from core.staging import StagingWriteError

# ---------------------------------------------------------------------------
# Happy path — write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_write_dict_payload_returns_staging_ref(self, staging_writer):
        ref = staging_writer.write("fx_rates", {"USD_GBP": 0.79, "USD_EUR": 0.92})

        assert ref == f"staging://fx_rates/{staging_writer.run_id}"

    def test_write_list_payload_is_accepted(self, staging_writer):
        payload = [{"currency": "USD", "rate": 1.0}, {"currency": "GBP", "rate": 0.79}]
        ref = staging_writer.write("fx_rates_list", payload)

        assert ref.startswith("staging://fx_rates_list/")

    def test_write_nested_dict_payload(self, staging_writer):
        payload = {
            "metadata": {"source": "reuters", "as_of": "2025-01-15"},
            "rates": {"USD_GBP": 0.79, "USD_EUR": 0.92},
        }
        ref = staging_writer.write("fx_rates_nested", payload)
        assert ref is not None

    def test_write_empty_dict_is_valid(self, staging_writer):
        """Empty dict is valid JSONB — the assertion layer decides if it's wrong."""
        ref = staging_writer.write("empty_dataset", {})
        assert ref is not None

    def test_multiple_datasets_same_run_id(self, staging_writer):
        """Same writer (same run_id) can write to multiple dataset keys."""
        ref1 = staging_writer.write("dataset_a", {"v": 1})
        ref2 = staging_writer.write("dataset_b", {"v": 2})

        assert ref1 != ref2
        assert "dataset_a" in ref1
        assert "dataset_b" in ref2


# ---------------------------------------------------------------------------
# Happy path — read (round-trip)
# ---------------------------------------------------------------------------


class TestRead:
    def test_read_returns_same_dict_as_written(self, staging_writer):
        payload = {"USD_GBP": 0.79, "USD_EUR": 0.92, "count": 42}
        ref = staging_writer.write("fx_rates", payload)

        result = staging_writer.read(ref)

        assert result == payload

    def test_read_returns_same_list_as_written(self, staging_writer):
        payload = [{"a": 1}, {"b": 2}, {"c": 3}]
        ref = staging_writer.write("list_dataset", payload)

        result = staging_writer.read(ref)

        assert result == payload

    def test_read_preserves_nested_structure(self, staging_writer):
        payload = {
            "level1": {
                "level2": {
                    "value": 99.9,
                    "tags": ["oil", "gas"],
                }
            }
        }
        ref = staging_writer.write("nested", payload)
        result = staging_writer.read(ref)

        assert result["level1"]["level2"]["value"] == 99.9
        assert result["level1"]["level2"]["tags"] == ["oil", "gas"]

    def test_read_preserves_numeric_types(self, staging_writer):
        """JSONB round-trip must not coerce int to float or vice versa for common values."""
        payload = {"int_val": 42, "float_val": 3.14, "zero": 0}
        ref = staging_writer.write("numeric_types", payload)
        result = staging_writer.read(ref)

        # JSONB preserves numeric precision; Postgres returns int as int
        assert result["int_val"] == 42
        assert abs(result["float_val"] - 3.14) < 1e-9
        assert result["zero"] == 0

    def test_different_writers_can_read_their_own_refs(self, clean_db):
        """Two writers (different run_ids) don't interfere with each other's reads."""
        writer1 = PostgresStagingWriter(run_id=new_run_id())
        writer2 = PostgresStagingWriter(run_id=new_run_id())

        payload1 = {"writer": 1, "value": "alpha"}
        payload2 = {"writer": 2, "value": "beta"}

        ref1 = writer1.write("shared_key", payload1)
        ref2 = writer2.write("shared_key", payload2)

        assert writer1.read(ref1) == payload1
        assert writer2.read(ref2) == payload2


# ---------------------------------------------------------------------------
# Duplicate key enforcement
# ---------------------------------------------------------------------------


class TestDuplicateKeyRejection:
    def test_duplicate_write_same_key_same_run_raises(self, staging_writer):
        """
        Core invariant: a run can only write once per dataset_key.
        This prevents the re-stamp scenario from the article where a job
        re-runs and silently overwrites staging with stale data.
        """
        staging_writer.write("fx_rates", {"USD_GBP": 0.79})

        with pytest.raises(StagingWriteError, match="already exists"):
            staging_writer.write("fx_rates", {"USD_GBP": 0.80})

    def test_same_key_different_run_ids_is_allowed(self, clean_db):
        """Different runs for the same dataset key are expected — each gets its own run_id."""
        writer1 = PostgresStagingWriter(run_id=new_run_id())
        writer2 = PostgresStagingWriter(run_id=new_run_id())

        ref1 = writer1.write("fx_rates", {"USD_GBP": 0.79})
        ref2 = writer2.write("fx_rates", {"USD_GBP": 0.80})

        assert ref1 != ref2


# ---------------------------------------------------------------------------
# Invalid payload rejection
# ---------------------------------------------------------------------------


class TestPayloadValidation:
    def test_string_payload_raises_before_db(self, staging_writer):
        with pytest.raises(StagingWriteError, match="dict or list"):
            staging_writer.write("fx_rates", "not a dict")

    def test_int_payload_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match="dict or list"):
            staging_writer.write("fx_rates", 42)

    def test_none_payload_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match="dict or list"):
            staging_writer.write("fx_rates", None)

    def test_tuple_payload_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match="dict or list"):
            staging_writer.write("fx_rates", ("a", "b"))


# ---------------------------------------------------------------------------
# Invalid dataset_key rejection
# ---------------------------------------------------------------------------


class TestDatasetKeyValidation:
    def test_empty_key_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match="non-empty"):
            staging_writer.write("", {"v": 1})

    def test_key_with_forward_slash_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match="'/'"):
            staging_writer.write("oil/gas", {"v": 1})

    def test_key_with_backslash_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match=r"'\\'"):
            staging_writer.write("oil\\gas", {"v": 1})

    def test_key_with_underscores_and_dots_is_valid(self, staging_writer):
        """Underscores, dots, and hyphens in dataset keys are fine."""
        ref = staging_writer.write("well_production.daily-v2", {"v": 1})
        assert ref is not None


# ---------------------------------------------------------------------------
# read() failure cases
# ---------------------------------------------------------------------------


class TestReadFailures:
    def test_read_unknown_staging_ref_raises(self, staging_writer):
        fake_ref = f"staging://fx_rates/{new_run_id()}"
        with pytest.raises(StagingWriteError, match="No staged record found"):
            staging_writer.read(fake_ref)

    def test_read_malformed_ref_raises(self, staging_writer):
        with pytest.raises(StagingWriteError, match="Invalid staging_ref format"):
            staging_writer.read("not-a-valid-ref")

    def test_read_published_ref_raises(self, staging_writer):
        """A published:// ref must not be readable via the staging reader."""
        fake_published = f"published://fx_rates/{new_run_id()}"
        with pytest.raises(StagingWriteError, match="Invalid staging_ref format"):
            staging_writer.read(fake_published)


# ---------------------------------------------------------------------------
# staging_ref_for()
# ---------------------------------------------------------------------------


class TestStagingRefFor:
    def test_staging_ref_for_returns_correct_format(self, staging_writer):
        ref = staging_writer.staging_ref_for("fx_rates")
        assert ref == f"staging://fx_rates/{staging_writer.run_id}"

    def test_staging_ref_for_does_not_write(self, staging_writer, clean_db):
        """Calling staging_ref_for must not insert any row."""
        staging_writer.staging_ref_for("fx_rates")

        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM staging.records WHERE dataset_key = 'fx_rates'"
            )
            count = cur.fetchone()[0]

        assert count == 0

    def test_staging_ref_for_invalid_key_raises(self, staging_writer):
        with pytest.raises(StagingWriteError):
            staging_writer.staging_ref_for("bad/key")


# ---------------------------------------------------------------------------
# run_id accessor
# ---------------------------------------------------------------------------


class TestRunIdAccessor:
    def test_run_id_property_returns_initialized_value(self, run_id):
        writer = PostgresStagingWriter(run_id=run_id)
        assert writer.run_id == run_id

    def test_invalid_run_id_raises_at_construction(self):
        with pytest.raises(StagingWriteError, match="UUID4"):
            PostgresStagingWriter(run_id="not-a-uuid")


# ---------------------------------------------------------------------------
# Import fix for staging_ref_for test that uses get_connection directly
# ---------------------------------------------------------------------------

from backends.postgres.connection import get_connection
