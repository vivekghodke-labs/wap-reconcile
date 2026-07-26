"""
Day 1 tests: models.py invariants only.

No DB, no I/O, no assertions/reference_source implementations — those
don't exist yet by design. These tests exist to lock in the
contracts before Day 2 builds persistence on top of them.
"""

from dataclasses import FrozenInstanceError

import pytest

from core.enums import AssertionStatus, RunStatus, Severity
from core.models import (
    AssertionResult,
    ReconciliationReport,
    RunResult,
    new_run_id,
)


def make_result(
    status: AssertionStatus,
    severity: Severity = Severity.WARN,
    message: str = "ok",
    name: str = "test_assertion",
) -> AssertionResult:
    return AssertionResult(
        assertion_name=name,
        status=status,
        severity=severity,
        message=message,
        evidence={"actual": 1, "expected": 1},
    )


class TestAssertionResult:
    def test_passed_property(self):
        r = make_result(AssertionStatus.PASSED)
        assert r.passed is True

    def test_failed_property(self):
        r = make_result(AssertionStatus.FAILED, message="mismatch")
        assert r.passed is False

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="assertion_name"):
            make_result(AssertionStatus.PASSED, name="")

    def test_failed_without_message_rejected(self):
        with pytest.raises(ValueError, match="no message"):
            AssertionResult(
                assertion_name="x",
                status=AssertionStatus.FAILED,
                severity=Severity.CRITICAL,
                message="",
            )

    def test_is_immutable(self):
        r = make_result(AssertionStatus.PASSED)
        with pytest.raises(FrozenInstanceError):
            r.message = "mutated"  # type: ignore[misc]

    def test_evidence_defaults_to_empty_dict(self):
        r = AssertionResult(
            assertion_name="x",
            status=AssertionStatus.PASSED,
            severity=Severity.INFO,
            message="ok",
        )
        assert r.evidence == {}

    def test_checked_at_is_auto_populated(self):
        r = make_result(AssertionStatus.PASSED)
        assert r.checked_at is not None


class TestReconciliationReport:
    def test_all_passed_true_when_no_failures(self):
        report = ReconciliationReport(
            run_id=new_run_id(),
            results=[
                make_result(AssertionStatus.PASSED),
                make_result(AssertionStatus.PASSED),
            ],
        )
        assert report.all_passed is True
        assert report.failed_results == ()

    def test_all_passed_false_on_single_failure(self):
        report = ReconciliationReport(
            run_id=new_run_id(),
            results=[
                make_result(AssertionStatus.PASSED),
                make_result(AssertionStatus.FAILED, message="bad"),
            ],
        )
        assert report.all_passed is False
        assert len(report.failed_results) == 1

    def test_highest_failed_severity_none_when_all_pass(self):
        report = ReconciliationReport(
            run_id=new_run_id(), results=[make_result(AssertionStatus.PASSED)]
        )
        assert report.highest_failed_severity is None

    def test_highest_failed_severity_picks_critical_over_warn(self):
        report = ReconciliationReport(
            run_id=new_run_id(),
            results=[
                make_result(
                    AssertionStatus.FAILED, severity=Severity.WARN, message="a"
                ),
                make_result(
                    AssertionStatus.FAILED, severity=Severity.CRITICAL, message="b"
                ),
                make_result(AssertionStatus.PASSED, severity=Severity.INFO),
            ],
        )
        assert report.highest_failed_severity is Severity.CRITICAL

    def test_is_immutable(self):
        report = ReconciliationReport(run_id=new_run_id(), results=[])
        with pytest.raises(FrozenInstanceError):
            report.run_id = "x"  # type: ignore[misc]


class TestRunResult:
    def test_published_requires_published_ref(self):
        report = ReconciliationReport(
            run_id=new_run_id(), results=[make_result(AssertionStatus.PASSED)]
        )
        with pytest.raises(ValueError, match="published_ref is None"):
            RunResult(
                run_id=new_run_id(),
                status=RunStatus.PUBLISHED,
                report=report,
                staging_ref="staging://fx_rates/run-1",
                published_ref=None,
            )

    def test_published_with_ref_succeeds(self):
        report = ReconciliationReport(
            run_id=new_run_id(), results=[make_result(AssertionStatus.PASSED)]
        )
        rr = RunResult(
            run_id=new_run_id(),
            status=RunStatus.PUBLISHED,
            report=report,
            staging_ref="staging://fx_rates/run-1",
            published_ref="published://fx_rates/run-1",
        )
        assert rr.published_ref == "published://fx_rates/run-1"

    def test_errored_requires_error_message(self):
        report = ReconciliationReport(run_id=new_run_id(), results=[])
        with pytest.raises(ValueError, match="no error message"):
            RunResult(
                run_id=new_run_id(),
                status=RunStatus.ERRORED,
                report=report,
                staging_ref="staging://x/run-1",
                error=None,
            )

    def test_routed_to_review_does_not_require_published_ref(self):
        report = ReconciliationReport(
            run_id=new_run_id(),
            results=[make_result(AssertionStatus.FAILED, message="mismatch")],
        )
        rr = RunResult(
            run_id=new_run_id(),
            status=RunStatus.ROUTED_TO_REVIEW,
            report=report,
            staging_ref="staging://fx_rates/run-1",
        )
        assert rr.published_ref is None

    def test_is_immutable(self):
        report = ReconciliationReport(run_id=new_run_id(), results=[])
        rr = RunResult(
            run_id=new_run_id(),
            status=RunStatus.ROUTED_TO_REVIEW,
            report=report,
            staging_ref="staging://x/run-1",
        )
        with pytest.raises(FrozenInstanceError):
            rr.staging_ref = "mutated"  # type: ignore[misc]


class TestNewRunId:
    def test_generates_unique_ids(self):
        assert new_run_id() != new_run_id()
