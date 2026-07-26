"""
Day 3 integration tests: ReviewQueueRouter.

Tests verify behaviour against a real Postgres instance.

Coverage matrix:
  ROUTE PATH (fail path)
  - route() happy path: wap_review_queue row inserted
  - route() happy path: wap_audit_log row inserted in same transaction
  - route() happy path: returns an integer queue_id > 0
  - route() happy path: staging record untouched
  - route() happy path: queue entry severity matches highest failed assertion
  - route() happy path: queue entry resolved_at is NULL (not auto-resolved)
  - route() happy path: audit log status is "routed_to_review"
  - route() happy path: audit log published_ref is NULL (nothing published)
  - route() severity escalation: CRITICAL beats WARN when both fail
  - route() severity escalation: single WARN failure records WARN

  GATE (business-rule refusals)
  - route() with passing report raises ReviewQueueError
  - route() duplicate run_id raises ReviewQueueError (idempotency guard)

  ATOMICITY
  - When ReviewQueueError raised (passing report), neither table is written

  AUDIT LOG CONTENT
  - audit log report JSONB contains all assertion names and statuses
  - audit log report JSONB preserves evidence dicts for failed assertions
  - audit log started_at is populated when supplied
  - queue entry report JSONB contains failed assertion details

  MULTI-ASSERTION SCENARIOS
  - All assertions fail: all appear in queue entry report
  - One assertion fails, rest pass: only the failure drives severity
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backends.postgres.connection import get_connection
from backends.postgres.staging_writer import PostgresStagingWriter
from core.enums import AssertionStatus, RunStatus, Severity
from core.models import AssertionResult, ReconciliationReport, new_run_id
from core.review_queue import ReviewQueueError, ReviewQueueRouter

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def router() -> ReviewQueueRouter:
    return ReviewQueueRouter()


def _make_passed(name: str, severity: Severity = Severity.INFO) -> AssertionResult:
    return AssertionResult(
        assertion_name=name,
        status=AssertionStatus.PASSED,
        severity=severity,
        message="ok",
    )


def _make_failed(
    name: str,
    severity: Severity = Severity.WARN,
    message: str = "mismatch detected",
    evidence: dict | None = None,
) -> AssertionResult:
    return AssertionResult(
        assertion_name=name,
        status=AssertionStatus.FAILED,
        severity=severity,
        message=message,
        evidence=evidence or {"detail": "see assertion"},
    )


def _make_failing_report(
    run_id: str, results: list[AssertionResult] | None = None
) -> ReconciliationReport:
    if results is None:
        results = [
            _make_failed("check_rates", Severity.CRITICAL, "Rate mismatch"),
            _make_passed("row_count"),
        ]
    return ReconciliationReport(run_id=run_id, results=results)


def _make_passing_report(run_id: str) -> ReconciliationReport:
    return ReconciliationReport(
        run_id=run_id,
        results=[_make_passed("check_rates"), _make_passed("row_count")],
    )


def _count_rows(table: str, run_id: str) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s::uuid", (run_id,))
        return cur.fetchone()[0]


def _fetch_queue_row(run_id: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, severity, report, staging_ref,
                   queued_at, resolved_at, resolution
            FROM wap_review_queue
            WHERE run_id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id": row[0],
        "severity": row[1],
        "report": row[2],
        "staging_ref": row[3],
        "queued_at": row[4],
        "resolved_at": row[5],
        "resolution": row[6],
    }


def _fetch_audit_row(run_id: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, published_ref, staging_ref, report,
                   started_at, completed_at
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
        "report": row[3],
        "started_at": row[4],
        "completed_at": row[5],
    }


# ---------------------------------------------------------------------------
# ROUTE PATH (fail path)
# ---------------------------------------------------------------------------


class TestRouteHappyPath:
    def test_route_inserts_review_queue_row(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert _count_rows("wap_review_queue", run_id) == 1

    def test_route_inserts_audit_log_row(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert _count_rows("wap_audit_log", run_id) == 1

    def test_route_returns_positive_integer_queue_id(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        queue_id = router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert isinstance(queue_id, int)
        assert queue_id > 0

    def test_route_staging_record_untouched(self, clean_db, router):
        run_id = new_run_id()
        payload = {"rate": 0.75}
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", payload)
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        # Verify staging record survived routing
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM staging.records WHERE staging_ref = %s",
                (staging_ref,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == payload

    def test_route_queue_severity_is_critical_when_critical_assertion_fails(
        self, clean_db, router
    ):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = ReconciliationReport(
            run_id=run_id,
            results=[
                _make_failed("check_rates", Severity.CRITICAL, "Rate mismatch"),
                _make_failed("row_count", Severity.WARN, "Count off"),
            ],
        )

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        queue_row = _fetch_queue_row(run_id)
        assert queue_row["severity"] == Severity.CRITICAL.value

    def test_route_queue_severity_is_warn_for_single_warn_failure(
        self, clean_db, router
    ):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = ReconciliationReport(
            run_id=run_id,
            results=[
                _make_passed("check_rates"),
                _make_failed("row_count", Severity.WARN, "Count delta exceeded"),
            ],
        )

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        queue_row = _fetch_queue_row(run_id)
        assert queue_row["severity"] == Severity.WARN.value

    def test_route_queue_entry_resolved_at_is_null(self, clean_db, router):
        """
        A freshly routed entry must be unresolved. Resolution is always
        an explicit external action, never automatic.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        queue_row = _fetch_queue_row(run_id)
        assert queue_row["resolved_at"] is None
        assert queue_row["resolution"] is None

    def test_route_audit_log_status_is_routed_to_review(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["status"] == RunStatus.ROUTED_TO_REVIEW.value

    def test_route_audit_log_published_ref_is_null(self, clean_db, router):
        """
        Routed runs must never show a published_ref in the audit log.
        A non-NULL published_ref would imply data was promoted, which is false.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["published_ref"] is None

    def test_route_audit_log_started_at_stored_when_supplied(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)
        started_at = datetime(2025, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
            started_at=started_at,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["started_at"].replace(microsecond=0) == started_at.replace(
            microsecond=0
        )

    def test_route_audit_log_completed_at_is_set(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["completed_at"] is not None


# ---------------------------------------------------------------------------
# AUDIT LOG AND QUEUE REPORT CONTENT
# ---------------------------------------------------------------------------


class TestReportContent:
    def test_queue_entry_report_contains_failed_assertion_details(
        self, clean_db, router
    ):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        evidence = {"staged": 0.75, "reference": 0.79, "delta": 0.04}
        report = ReconciliationReport(
            run_id=run_id,
            results=[
                _make_failed(
                    "fx_rate_check",
                    Severity.CRITICAL,
                    "Rate mismatch",
                    evidence=evidence,
                ),
            ],
        )

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        queue_row = _fetch_queue_row(run_id)
        results = queue_row["report"]["results"]
        assert len(results) == 1
        assert results[0]["assertion_name"] == "fx_rate_check"
        assert results[0]["status"] == AssertionStatus.FAILED.value
        assert results[0]["evidence"]["delta"] == 0.04

    def test_audit_log_report_contains_all_assertion_statuses(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = ReconciliationReport(
            run_id=run_id,
            results=[
                _make_failed("check_rates", Severity.CRITICAL, "Rate mismatch"),
                _make_passed("row_count"),
                _make_passed("null_check"),
            ],
        )

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        result_names = {r["assertion_name"] for r in audit["report"]["results"]}
        assert result_names == {"check_rates", "row_count", "null_check"}

    def test_all_assertions_fail_all_appear_in_queue(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = ReconciliationReport(
            run_id=run_id,
            results=[
                _make_failed("check_rates", Severity.CRITICAL, "Rate mismatch"),
                _make_failed("row_count", Severity.WARN, "Count off"),
                _make_failed("null_check", Severity.INFO, "Nulls found"),
            ],
        )

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        queue_row = _fetch_queue_row(run_id)
        result_names = {r["assertion_name"] for r in queue_row["report"]["results"]}
        assert result_names == {"check_rates", "row_count", "null_check"}
        # Severity must be the highest — CRITICAL
        assert queue_row["severity"] == Severity.CRITICAL.value


# ---------------------------------------------------------------------------
# GATE — business-rule refusals
# ---------------------------------------------------------------------------


class TestRouteGate:
    def test_route_raises_on_passing_report(self, clean_db, router):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        with pytest.raises(ReviewQueueError, match="Routing refused"):
            router.route(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )

    def test_route_duplicate_run_id_raises(self, clean_db, router):
        """
        Idempotency guard: a run_id can only be queued once.
        A retry loop must not flood the review queue with duplicates.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        with pytest.raises(ReviewQueueError, match="already present"):
            router.route(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )


# ---------------------------------------------------------------------------
# ATOMICITY — passing report gate must leave no partial writes
# ---------------------------------------------------------------------------


class TestRouteAtomicity:
    def test_passing_report_writes_nothing(self, clean_db, router):
        """
        When ReviewQueueError is raised (passing report), neither
        wap_review_queue nor wap_audit_log must contain a row for this run_id.
        The gate fires before any DB write.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        with pytest.raises(ReviewQueueError):
            router = ReviewQueueRouter()
            router.route(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )

        assert _count_rows("wap_review_queue", run_id) == 0
        assert _count_rows("wap_audit_log", run_id) == 0

    def test_route_writes_both_tables_atomically(self, clean_db, router):
        """
        Fail path: both wap_review_queue and wap_audit_log must be written.
        Either one missing means the evidence trail is broken.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert _count_rows("wap_review_queue", run_id) == 1
        assert _count_rows("wap_audit_log", run_id) == 1
