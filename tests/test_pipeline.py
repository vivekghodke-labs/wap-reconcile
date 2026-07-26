"""
Day 5 integration tests: ReconciliationPipeline.

Tests verify behaviour against a real Postgres instance (docker-compose).

Coverage matrix:
  PASS PATH
  - Full run: stage -> resolve reference -> assertions pass -> publish
  - Returned RunResult has status PUBLISHED and a correct published_ref
  - Audit log entry written by Publisher shows status=published

  FAIL PATH — THE ARTICLE'S EXACT BUG
  - Stale values behind a fresh timestamp: assertion fails -> routed to
    review, staging untouched, review queue + audit log both written

  ERROR PATHS (explicit behaviour per assertion, not silent catches)
  - Reference source has no prior snapshot -> ERRORED, pipeline writes
    its own audit log row directly (no Publisher/ReviewQueueRouter call)
  - Staging write failure (duplicate run_id+dataset_key) -> ERRORED,
    audit log written, error message actionable
  - A misconfigured assertion that raises unexpectedly -> isolated to
    an ERRORED AssertionResult for that assertion only; every other
    assertion in the batch still executes and the run is still
    classified correctly (ROUTED_TO_REVIEW, not pipeline-level ERRORED)

  MISCONFIGURATION
  - Empty assertions list raises ValueError before any I/O (no run_id
    side effects, no DB rows written)

  MULTI-ASSERTION SEVERITY
  - Mixed pass/fail across multiple assertions: highest failed severity
    drives review queue routing, exactly as in Day 3's ReviewQueueRouter
    tests, now driven end-to-end through the pipeline.
"""

from __future__ import annotations

import uuid

import pytest
from psycopg2.extras import Json

from assertions_library.fx_reconciliation import FxRateReconciliation
from assertions_library.row_count_delta import RowCountDelta
from backends.postgres.connection import get_connection
from backends.postgres.staging_writer import PostgresStagingWriter
from core.assertions import Assertion
from core.enums import AssertionStatus, RunStatus, Severity
from core.models import new_run_id
from core.pipeline import ReconciliationPipeline
from core.publisher import Publisher
from core.reference_source import SnapshotReferenceSource
from core.review_queue import ReviewQueueRouter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_published_record(dataset_key: str, payload: dict) -> None:
    """Simulate a prior successful pipeline run, bypassing Publisher."""
    run_id = str(uuid.uuid4())
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
                "published_ref": f"published://{dataset_key}/{run_id}",
                "dataset_key": dataset_key,
                "run_id": run_id,
                "staging_ref": f"staging://{dataset_key}/{run_id}",
                "payload": Json(payload),
            },
        )
        conn.commit()


def _make_pipeline() -> ReconciliationPipeline:
    return ReconciliationPipeline(
        staging_writer_factory=lambda run_id: PostgresStagingWriter(run_id=run_id),
        publisher=Publisher(),
        review_router=ReviewQueueRouter(),
    )


def _fetch_audit_row(run_id: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, published_ref, staging_ref, error, report
            FROM wap_audit_log
            WHERE run_id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "published_ref": row[1],
        "staging_ref": row[2],
        "error": row[3],
        "report": row[4],
    }


def _count_rows(table: str, run_id: str) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s::uuid", (run_id,))
        return cur.fetchone()[0]


class _AlwaysRaisesAssertion(Assertion):
    """Deliberately broken assertion — used to prove per-assertion isolation."""

    name = "broken_assertion"
    severity = Severity.WARN

    def check(self, staged, reference):
        raise RuntimeError("simulated assertion bug")


# ---------------------------------------------------------------------------
# PASS PATH
# ---------------------------------------------------------------------------


class TestPipelinePassPath:
    def test_full_run_publishes_when_assertions_pass(self, clean_db):
        _insert_published_record("fx_rates", {"USD_GBP": 0.79, "USD_EUR": 0.92})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79, "USD_EUR": 0.92},
            assertions=[FxRateReconciliation(tolerance=0.0001)],
            reference_source=SnapshotReferenceSource(),
        )

        assert result.status is RunStatus.PUBLISHED
        assert result.published_ref == f"published://fx_rates/{result.run_id}"
        assert result.error is None

    def test_published_audit_log_entry_is_correct(self, clean_db):
        _insert_published_record("fx_rates", {"USD_GBP": 0.79})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[FxRateReconciliation()],
            reference_source=SnapshotReferenceSource(),
        )

        audit = _fetch_audit_row(result.run_id)
        assert audit["status"] == RunStatus.PUBLISHED.value
        assert audit["published_ref"] == result.published_ref

    def test_staging_record_untouched_after_publish(self, clean_db):
        _insert_published_record("fx_rates", {"USD_GBP": 0.79})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[FxRateReconciliation()],
            reference_source=SnapshotReferenceSource(),
        )

        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM staging.records WHERE staging_ref = %s",
                (result.staging_ref,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == {"USD_GBP": 0.79}


# ---------------------------------------------------------------------------
# FAIL PATH — the article's exact bug
# ---------------------------------------------------------------------------


class TestPipelineFailPath:
    def test_stale_values_behind_fresh_timestamp_routes_to_review(self, clean_db):
        """
        Recreates the article's incident end-to-end: an upstream job
        re-stamped last_updated without refreshing the underlying FX
        values. The staged rates are yesterday's; the reference (last
        published snapshot) has today's. The pipeline must NOT publish
        and must route the run to human review with full evidence.
        """
        _insert_published_record(
            "fx_rates",
            {"USD_GBP": 0.79, "USD_EUR": 0.92},  # today's actual rates
        )

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.75, "USD_EUR": 0.88},  # stale — yesterday's rates
            assertions=[FxRateReconciliation(tolerance=0.0001)],
            reference_source=SnapshotReferenceSource(),
        )

        assert result.status is RunStatus.ROUTED_TO_REVIEW
        assert result.published_ref is None
        assert not result.report.all_passed
        assert "USD_GBP" in result.report.failed_results[0].evidence["mismatches"]

    def test_fail_path_writes_review_queue_and_audit_log(self, clean_db):
        _insert_published_record("fx_rates", {"USD_GBP": 0.79})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.75},
            assertions=[FxRateReconciliation(tolerance=0.0001)],
            reference_source=SnapshotReferenceSource(),
        )

        assert _count_rows("wap_review_queue", result.run_id) == 1
        assert _count_rows("wap_audit_log", result.run_id) == 1

    def test_fail_path_staging_untouched(self, clean_db):
        _insert_published_record("fx_rates", {"USD_GBP": 0.79})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.75},
            assertions=[FxRateReconciliation(tolerance=0.0001)],
            reference_source=SnapshotReferenceSource(),
        )

        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM staging.records WHERE staging_ref = %s",
                (result.staging_ref,),
            )
            row = cur.fetchone()
        assert row is not None  # staging record survives a failed run

    def test_mixed_assertions_severity_drives_routing(self, clean_db):
        """CRITICAL FX failure alongside a passing row-count check must
        still surface CRITICAL as the routing severity."""
        _insert_published_record("fx_rates", {"USD_GBP": 0.79, "row_count": 100})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.75, "row_count": 100},
            assertions=[
                FxRateReconciliation(tolerance=0.0001),
                RowCountDelta(max_absolute_delta=5),
            ],
            reference_source=SnapshotReferenceSource(),
        )

        assert result.status is RunStatus.ROUTED_TO_REVIEW
        assert result.report.highest_failed_severity is Severity.CRITICAL


# ---------------------------------------------------------------------------
# ERROR PATHS
# ---------------------------------------------------------------------------


class TestPipelineErrorPaths:
    def test_no_prior_snapshot_errors_explicitly(self, clean_db):
        """
        First-ever run for a dataset_key: SnapshotReferenceSource has
        nothing to resolve. This must not silently skip verification —
        it must be an explicit ERRORED run with a clear audit trail.
        """
        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[FxRateReconciliation()],
            reference_source=SnapshotReferenceSource(),
        )

        assert result.status is RunStatus.ERRORED
        assert "Reference resolution failed" in result.error
        assert result.published_ref is None

    def test_no_prior_snapshot_writes_audit_log_directly(self, clean_db):
        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[FxRateReconciliation()],
            reference_source=SnapshotReferenceSource(),
        )

        audit = _fetch_audit_row(result.run_id)
        assert audit is not None
        assert audit["status"] == RunStatus.ERRORED.value
        assert audit["published_ref"] is None
        # No report existed yet — pipeline must synthesize an empty one
        assert audit["report"]["results"] == []

        # Neither downstream table should have been touched
        assert _count_rows("wap_review_queue", result.run_id) == 0
        assert _count_rows("published.records", result.run_id) == 0

    def test_staging_write_failure_errors_explicitly(self, clean_db):
        """
        Duplicate (dataset_key, run_id) triggers StagingWriteError.
        Simulated by pre-writing with an explicit run_id, then re-running
        the pipeline with that same run_id.
        """
        _insert_published_record("fx_rates", {"USD_GBP": 0.79})
        fixed_run_id = new_run_id()
        PostgresStagingWriter(run_id=fixed_run_id).write("fx_rates", {"USD_GBP": 0.79})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.80},
            assertions=[FxRateReconciliation()],
            reference_source=SnapshotReferenceSource(),
            run_id=fixed_run_id,
        )

        assert result.status is RunStatus.ERRORED
        assert "Staging write failed" in result.error

    def test_broken_assertion_isolated_others_still_run(self, clean_db):
        """
        A misconfigured assertion that raises must not abort the run.
        It becomes an ERRORED AssertionResult; the well-behaved
        assertion alongside it still executes and its result is present
        in the final report.
        """
        _insert_published_record("fx_rates", {"USD_GBP": 0.79, "row_count": 100})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79, "row_count": 100},
            assertions=[
                _AlwaysRaisesAssertion(),
                FxRateReconciliation(tolerance=0.0001),
            ],
            reference_source=SnapshotReferenceSource(),
        )

        names_to_status = {r.assertion_name: r.status for r in result.report.results}
        assert names_to_status["broken_assertion"] is AssertionStatus.ERRORED
        assert names_to_status["fx_rate_reconciliation"] is AssertionStatus.PASSED
        # ERRORED is not PASSED -> report.all_passed is False -> routed to review
        assert result.status is RunStatus.ROUTED_TO_REVIEW

    def test_broken_assertion_evidence_names_exception_type(self, clean_db):
        _insert_published_record("fx_rates", {"USD_GBP": 0.79})

        pipeline = _make_pipeline()
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[_AlwaysRaisesAssertion()],
            reference_source=SnapshotReferenceSource(),
        )

        broken_result = result.report.results[0]
        assert broken_result.status is AssertionStatus.ERRORED
        assert broken_result.evidence["exception_type"] == "RuntimeError"


# ---------------------------------------------------------------------------
# MISCONFIGURATION
# ---------------------------------------------------------------------------


class TestPipelineMisconfiguration:
    def test_empty_assertions_raises_before_any_io(self, clean_db):
        pipeline = _make_pipeline()

        with pytest.raises(ValueError, match="non-empty"):
            pipeline.run(
                dataset_key="fx_rates",
                data={"USD_GBP": 0.79},
                assertions=[],
                reference_source=SnapshotReferenceSource(),
            )

        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM staging.records")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM wap_audit_log")
            assert cur.fetchone()[0] == 0
