"""
Day 3 integration tests: Publisher.

Tests verify behaviour against a real Postgres instance.

Coverage matrix:
  PASS PATH
  - promote() happy path: all assertions pass → published.records row inserted
  - promote() happy path: wap_audit_log row inserted in same transaction
  - promote() happy path: published_ref format is correct
  - promote() happy path: staging record is UNTOUCHED after promotion
  - promote() happy path: published payload matches staged payload exactly
  - promote() happy path: audit log status is "published"
  - promote() happy path: audit log published_ref matches returned ref

  GATE (business-rule refusals)
  - promote() with a failed report raises PublishGateError
  - promote() with a mixed report (some pass, some fail) raises PublishGateError
  - promote() duplicate run_id raises PublishGateError (idempotency guard)
  - promote() with a missing staging record raises PublishGateError

  ATOMICITY
  - When PublishGateError is raised due to a failed report, NO rows are
    written to either published.records or wap_audit_log

  AUDIT LOG CONTENT
  - audit log report JSONB contains all assertion names and statuses
  - audit log started_at is populated when supplied
  - audit log completed_at is set
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backends.postgres.connection import get_connection
from backends.postgres.staging_writer import PostgresStagingWriter
from core.enums import AssertionStatus, RunStatus, Severity
from core.models import AssertionResult, ReconciliationReport, new_run_id
from core.publisher import Publisher, PublishGateError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def publisher() -> Publisher:
    return Publisher()


def _make_passed_result(name: str = "check_rates") -> AssertionResult:
    return AssertionResult(
        assertion_name=name,
        status=AssertionStatus.PASSED,
        severity=Severity.CRITICAL,
        message="ok",
        evidence={"actual": 0.79, "expected": 0.79},
    )


def _make_failed_result(name: str = "check_rates") -> AssertionResult:
    return AssertionResult(
        assertion_name=name,
        status=AssertionStatus.FAILED,
        severity=Severity.CRITICAL,
        message="Rate mismatch: staged=0.75, reference=0.79",
        evidence={"staged": 0.75, "reference": 0.79, "delta": 0.04},
    )


def _make_passing_report(run_id: str) -> ReconciliationReport:
    return ReconciliationReport(
        run_id=run_id,
        results=[_make_passed_result("check_rates"), _make_passed_result("row_count")],
    )


def _make_failing_report(run_id: str) -> ReconciliationReport:
    return ReconciliationReport(
        run_id=run_id,
        results=[_make_failed_result("check_rates"), _make_passed_result("row_count")],
    )


def _count_rows(table: str, run_id: str) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s::uuid", (run_id,))
        return cur.fetchone()[0]


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


def _fetch_published_row(run_id: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT published_ref, dataset_key, staging_ref, payload, published_at
            FROM published.records
            WHERE run_id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "published_ref": row[0],
        "dataset_key": row[1],
        "staging_ref": row[2],
        "payload": row[3],
        "published_at": row[4],
    }


def _fetch_staging_row(staging_ref: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT staging_ref, payload FROM staging.records WHERE staging_ref = %s",
            (staging_ref,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {"staging_ref": row[0], "payload": row[1]}


# ---------------------------------------------------------------------------
# PASS PATH
# ---------------------------------------------------------------------------


class TestPublishPassPath:
    def test_promote_inserts_published_record(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        payload = {"USD_GBP": 0.79, "USD_EUR": 0.92}
        staging_ref = writer.write("fx_rates", payload)
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert _count_rows("published.records", run_id) == 1

    def test_promote_returns_correct_published_ref(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        published_ref = publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert published_ref == f"published://fx_rates/{run_id}"

    def test_promote_inserts_audit_log_entry(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert _count_rows("wap_audit_log", run_id) == 1

    def test_promote_audit_log_status_is_published(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["status"] == RunStatus.PUBLISHED.value

    def test_promote_audit_log_published_ref_matches_return(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        published_ref = publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["published_ref"] == published_ref

    def test_promote_published_payload_matches_staged_payload(
        self, clean_db, publisher
    ):
        run_id = new_run_id()
        payload = {"USD_GBP": 0.79, "USD_EUR": 0.92, "source": "reuters"}
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", payload)
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        pub_row = _fetch_published_row(run_id)
        assert pub_row["payload"] == payload

    def test_promote_staging_record_untouched(self, clean_db, publisher):
        """
        Core WAP guarantee: staging record must survive promotion.
        Audit replay requires that the staged data is still readable
        after the publish step.
        """
        run_id = new_run_id()
        payload = {"rate": 0.79}
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", payload)
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        staging_row = _fetch_staging_row(staging_ref)
        assert staging_row is not None, "Staging record must not be deleted on promote"
        assert staging_row["payload"] == payload

    def test_promote_audit_log_contains_assertion_names(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        result_names = [r["assertion_name"] for r in audit["report"]["results"]]
        assert "check_rates" in result_names
        assert "row_count" in result_names

    def test_promote_audit_log_started_at_is_stored(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)
        started_at = datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
            started_at=started_at,
        )

        audit = _fetch_audit_row(run_id)
        # DB returns timezone-aware datetime; compare stripped to seconds
        assert audit["started_at"].replace(microsecond=0) == started_at.replace(
            microsecond=0
        )

    def test_promote_audit_log_completed_at_is_set(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        audit = _fetch_audit_row(run_id)
        assert audit["completed_at"] is not None


# ---------------------------------------------------------------------------
# GATE — business-rule refusals
# ---------------------------------------------------------------------------


class TestPublishGate:
    def test_promote_raises_on_failed_report(self, clean_db, publisher):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        with pytest.raises(PublishGateError, match="Promotion refused"):
            publisher.promote(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )

    def test_promote_error_message_includes_failed_assertion_names(
        self, clean_db, publisher
    ):
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        with pytest.raises(PublishGateError) as exc_info:
            publisher.promote(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )

        # The error message must name the failing assertion so the caller
        # can log something actionable without unpacking the report.
        assert "check_rates" in str(exc_info.value)

    def test_promote_duplicate_run_id_raises(self, clean_db, publisher):
        """
        Idempotency guard: a run_id can only be published once.
        Retrying with the same run_id after a successful publish must raise,
        not silently overwrite.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        # Second call with same run_id — must raise
        with pytest.raises(PublishGateError, match="already been published"):
            publisher.promote(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )

    def test_promote_missing_staging_ref_raises(self, clean_db, publisher):
        """
        If the staging record has been deleted between assertion execution
        and promotion, we must fail explicitly — not promote empty data.
        """
        run_id = new_run_id()
        # Deliberately do NOT write to staging
        fake_staging_ref = f"staging://fx_rates/{run_id}"
        report = _make_passing_report(run_id)

        with pytest.raises(PublishGateError, match="Staged record not found"):
            publisher.promote(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=fake_staging_ref,
                report=report,
            )


# ---------------------------------------------------------------------------
# ATOMICITY — failed gate must leave no partial writes
# ---------------------------------------------------------------------------


class TestPublishAtomicity:
    def test_failed_report_writes_nothing_to_published_or_audit(
        self, clean_db, publisher
    ):
        """
        When PublishGateError is raised due to failed assertions, the gate
        fires BEFORE any DB write. Both published.records and wap_audit_log
        must be empty for this run_id.

        This is a critical atomicity test: a partial write (e.g. audit log
        written but published.records not) would corrupt the evidence trail.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.75})
        report = _make_failing_report(run_id)

        with pytest.raises(PublishGateError):
            publisher.promote(
                run_id=run_id,
                dataset_key="fx_rates",
                staging_ref=staging_ref,
                report=report,
            )

        assert _count_rows("published.records", run_id) == 0
        assert _count_rows("wap_audit_log", run_id) == 0

    def test_successful_promote_writes_both_tables_atomically(
        self, clean_db, publisher
    ):
        """
        Pass path: both published.records and wap_audit_log must be written.
        Either one missing means the evidence trail is broken.
        """
        run_id = new_run_id()
        writer = PostgresStagingWriter(run_id=run_id)
        staging_ref = writer.write("fx_rates", {"rate": 0.79})
        report = _make_passing_report(run_id)

        publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref=staging_ref,
            report=report,
        )

        assert _count_rows("published.records", run_id) == 1
        assert _count_rows("wap_audit_log", run_id) == 1
