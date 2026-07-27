"""
Day 6 tests: WAPReconciliationOperator.

Requires apache-airflow to be importable (requirements-airflow.txt).
Skipped entirely, not failed, if Airflow isn't installed — core/ has no
Airflow dependency, and neither should the rest of the test suite when
someone runs `pytest tests/` without the optional adapter extra.

No Postgres required for any test in this file: ReconciliationPipeline
is driven entirely through fakes (FakeStagingWriter, FakePublisher,
FakeReviewRouter, FakeReferenceSource) that satisfy the same ABCs/
duck-typed interfaces as the real Postgres-backed implementations. This
keeps the adapter's own logic — argument resolution, XCom evidence,
failure-mode translation — testable in isolation from Day 2/3's
integration-tested persistence layer.

Coverage matrix:
  ARGUMENT RESOLUTION
  - Literal values are passed straight through
  - Callable values are resolved against the task context at execute() time

  SUCCESS PATH
  - PUBLISHED run: task succeeds, return value + XCom both carry evidence

  FAILURE SEMANTICS
  - ROUTED_TO_REVIEW + fail_on_review_route=True (default): raises
    WAPReviewRequiredException; evidence still pushed to XCom first
  - ROUTED_TO_REVIEW + fail_on_review_route=False: task succeeds,
    downstream can branch on XCom `status`
  - ERRORED (reference resolution failure, zero DB config): raises
    WAPReconciliationError with a clean, typed message — the pipeline's
    own audit-log-write failure must never leak an unrelated
    OSError/ConnectionError through the operator
  - Evidence is pushed to XCom BEFORE the exception is raised in all
    failure cases — a failed task must never be silent

  DAG-LEVEL SMOKE TEST
  - The operator instantiates cleanly inside a real Airflow DAG and
    proves the "plug into real pipelines" claim end-to-end, not just at
    the unit level. Skipped (not failed) if the installed Airflow
    version's dag.test() API is unavailable.
"""

from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow", reason="apache-airflow not installed")

from adapters.airflow_operator import (
    WAPReconciliationError,
    WAPReconciliationOperator,
    WAPReviewRequiredException,
)
from core.assertions import Assertion
from core.enums import AssertionStatus, Severity
from core.models import AssertionResult
from core.reference_source import (
    ReferenceResolutionError,
    ReferenceSource,
)
from core.staging import StagingWriter


@pytest.fixture(scope="session", autouse=True)
def _airflow_metadata_db():
    """
    Ensure Airflow's own metadata DB (task_instance, dag_run, etc.) is
    migrated before any test in this module runs.

    Mirrors tests/conftest.py's db_session fixture, which auto-runs the
    framework's own Postgres migrations rather than requiring a manual
    step before `pytest` can be invoked. DAG.test() in TestDagLevelSmoke
    queries task_instance/dag_run directly — on a fresh AIRFLOW_HOME
    (e.g. a clean CI runner) those tables don't exist yet without this.

    Uses the public `airflow db migrate` command via subprocess rather
    than a version-specific internal API, since the internal migration
    entrypoint has moved across Airflow major versions.
    """
    import subprocess
    import sys

    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        check=True,
        capture_output=True,
        text=True,
    )
    yield


# ---------------------------------------------------------------------------
# Fakes — satisfy the same interfaces as the real Postgres-backed classes,
# entirely in memory. No DB, no network.
# ---------------------------------------------------------------------------


class FakeStagingWriter(StagingWriter):
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self._store: dict[str, object] = {}

    def write(self, key: str, data):
        ref = f"staging://{key}/{self.run_id}"
        self._store[ref] = data
        return ref

    def read(self, staging_ref: str):
        return self._store[staging_ref]


class FakeReferenceSource(ReferenceSource):
    def __init__(self, data=None, raises: bool = False) -> None:
        self._data = data
        self._raises = raises

    def resolve(self, key: str):
        if self._raises:
            raise ReferenceResolutionError(f"no snapshot for '{key}'")
        return self._data


class PassAssertion(Assertion):
    name = "pass_assertion"
    severity = Severity.INFO

    def check(self, staged, reference):
        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.PASSED,
            severity=self.severity,
            message="ok",
        )


class FailAssertion(Assertion):
    name = "fail_assertion"
    severity = Severity.CRITICAL

    def check(self, staged, reference):
        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.FAILED,
            severity=self.severity,
            message="mismatch",
            evidence={"delta": 0.04},
        )


class FakePublisher:
    def promote(self, *, run_id, dataset_key, staging_ref, report, started_at=None):
        return f"published://{dataset_key}/{run_id}"


class FakeReviewRouter:
    def route(self, *, run_id, dataset_key, staging_ref, report, started_at=None):
        return 1


class FakeTaskInstance:
    """Stand-in for Airflow's TaskInstance — only xcom_push is exercised."""

    def __init__(self) -> None:
        self.pushed: dict[str, object] = {}

    def xcom_push(self, key, value) -> None:
        self.pushed[key] = value


def _staging_writer_factory():
    """Fresh FakeStagingWriter per run_id — mirrors PostgresStagingWriter's
    per-run construction contract."""
    return lambda run_id: FakeStagingWriter(run_id)


# ---------------------------------------------------------------------------
# ARGUMENT RESOLUTION
# ---------------------------------------------------------------------------


class TestArgumentResolution:
    def test_literal_values_pass_through(self):
        op = WAPReconciliationOperator(
            task_id="literal_args",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[PassAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )
        result = op.execute({"ti": FakeTaskInstance()})
        assert result["status"] == "published"

    def test_callables_are_resolved_against_context(self):
        """dataset_key and data sourced dynamically — the XCom-pull pattern."""
        op = WAPReconciliationOperator(
            task_id="callable_args",
            dataset_key=lambda ctx: ctx["dataset_key_from_upstream"],
            data=lambda ctx: ctx["ti"].xcom_pull(),
            assertions=[PassAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )

        class _TI(FakeTaskInstance):
            def xcom_pull(self):
                return {"USD_GBP": 0.79}

        ctx = {"ti": _TI(), "dataset_key_from_upstream": "fx_rates"}
        result = op.execute(ctx)
        assert result["status"] == "published"


# ---------------------------------------------------------------------------
# SUCCESS PATH
# ---------------------------------------------------------------------------


class TestPublishedPath:
    def test_task_succeeds_and_returns_summary(self):
        op = WAPReconciliationOperator(
            task_id="pub_ok",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[PassAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )
        result = op.execute({"ti": FakeTaskInstance()})

        assert result["status"] == "published"
        assert result["published_ref"] is not None
        assert result["error"] is None

    def test_evidence_pushed_to_xcom(self):
        op = WAPReconciliationOperator(
            task_id="pub_xcom",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[PassAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )
        ti = FakeTaskInstance()
        op.execute({"ti": ti})

        assert ti.pushed["status"] == "published"
        assert ti.pushed["run_id"]
        assert ti.pushed["published_ref"] is not None


# ---------------------------------------------------------------------------
# FAILURE SEMANTICS — ROUTED_TO_REVIEW
# ---------------------------------------------------------------------------


class TestReviewRouteSemantics:
    def test_raises_by_default(self):
        op = WAPReconciliationOperator(
            task_id="review_hard",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.75},
            assertions=[FailAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )
        ti = FakeTaskInstance()

        with pytest.raises(WAPReviewRequiredException, match="fail_assertion"):
            op.execute({"ti": ti})

        # Evidence must be pushed BEFORE the exception propagates.
        assert ti.pushed["status"] == "routed_to_review"
        assert ti.pushed["published_ref"] is None

    def test_soft_path_succeeds_when_disabled(self):
        op = WAPReconciliationOperator(
            task_id="review_soft",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.75},
            assertions=[FailAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
            fail_on_review_route=False,
        )
        result = op.execute({"ti": FakeTaskInstance()})

        assert result["status"] == "routed_to_review"
        assert result["published_ref"] is None


# ---------------------------------------------------------------------------
# FAILURE SEMANTICS — ERRORED
# ---------------------------------------------------------------------------


class TestErroredSemantics:
    def test_reference_resolution_failure_raises_clean_typed_error(self):
        """
        No DATABASE_URL/POSTGRES_* env configured in this test process.
        The pipeline's own attempt to write an ERRORED audit log entry
        will itself fail (OSError from backends.postgres.connection) —
        that must be swallowed internally and must NOT leak through the
        operator as an unrelated, unclassified exception. The operator
        must still raise exactly WAPReconciliationError.
        """
        op = WAPReconciliationOperator(
            task_id="err_ref",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[PassAssertion()],
            reference_source=FakeReferenceSource(raises=True),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )
        ti = FakeTaskInstance()

        with pytest.raises(WAPReconciliationError, match="Reference resolution failed"):
            op.execute({"ti": ti})

        assert ti.pushed["status"] == "errored"
        assert ti.pushed["published_ref"] is None
        assert "Reference resolution failed" in ti.pushed["error"]

    def test_no_task_instance_in_context_does_not_raise(self):
        """
        A bare execute() call without a real task instance (e.g. a
        lightweight unit test context) must not itself crash while
        trying to push XCom — evidence is still available via the
        return value.
        """
        op = WAPReconciliationOperator(
            task_id="no_ti",
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79},
            assertions=[PassAssertion()],
            reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
            staging_writer_factory=_staging_writer_factory(),
            publisher=FakePublisher(),
            review_router=FakeReviewRouter(),
        )
        result = op.execute({})
        assert result["status"] == "published"


# ---------------------------------------------------------------------------
# DAG-LEVEL SMOKE TEST
# ---------------------------------------------------------------------------


class TestDagLevelSmoke:
    def test_operator_runs_inside_a_real_dag(self):
        """
        Proves the "plug into real pipelines" claim: the operator
        instantiates and executes inside an actual Airflow DAG object,
        not just as a bare Python class. Skipped (not failed) on
        Airflow versions where dag.test() isn't available, since the
        API has moved across major versions.
        """
        from airflow import DAG

        if not hasattr(DAG, "test"):
            pytest.skip("Installed Airflow version has no DAG.test() API")

        from datetime import datetime, timezone

        with DAG(
            dag_id="wap_reconciliation_smoke",
            schedule=None,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            catchup=False,
        ) as dag:
            WAPReconciliationOperator(
                task_id="reconcile_fx_rates",
                dataset_key="fx_rates",
                data={"USD_GBP": 0.79},
                assertions=[PassAssertion()],
                reference_source=FakeReferenceSource(data={"USD_GBP": 0.79}),
                staging_writer_factory=_staging_writer_factory(),
                publisher=FakePublisher(),
                review_router=FakeReviewRouter(),
            )

        # dag.test() runs every task via the local executor and raises
        # if any task instance ends in a failed state.
        dag.test()
