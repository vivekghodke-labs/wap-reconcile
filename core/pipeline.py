"""
ReconciliationPipeline — orchestrates staging -> reference -> assertions ->
publish/review.

Day 5 deliverable. Ties together:
  - StagingWriter          (Day 1/2)
  - ReferenceSource        (Day 1/2)
  - Assertion library      (Day 1/4)
  - Publisher / ReviewQueueRouter (Day 3)

Design invariants (do not relax without a documented reason):

1. EVERY terminal outcome writes exactly one wap_audit_log row.
   Publisher and ReviewQueueRouter already write their own audit row on
   success. Failures that occur BEFORE a ReconciliationReport exists
   (staging write, staging read-back, reference resolution) — and
   refusals raised BY Publisher/ReviewQueueRouter themselves — are not
   covered by either of those components. The pipeline is responsible
   for writing the audit row in every one of those cases. A run that
   errors and leaves no evidence trail is the exact failure mode this
   framework exists to prevent.

2. NO SINGLE ASSERTION EXCEPTION ABORTS THE RUN. Each Assertion.check()
   call is individually guarded. An assertion that raises unexpectedly
   produces an ERRORED AssertionResult for itself only; every other
   assertion in the batch still runs. A reviewer must see the full
   picture, not a partial one truncated by the first misbehaving
   assertion. (Per the Assertion ABC contract, expected failures are
   already FAILED results returned by well-behaved assertions — this
   guard is for genuinely broken/misconfigured assertions only.)

3. STATELESS / INJECTABLE. Publisher and ReviewQueueRouter are
   constructor-injected (defaulted, never hardcoded) so the
   orchestrator can be unit-tested with fakes and is not welded to
   Postgres. staging_writer_factory is injected because StagingWriter
   implementations are constructed per run_id (see
   PostgresStagingWriter), not reusable across runs.

4. run_id IS THE CORRELATION KEY across staging, published, and audit
   tables for one run. Generated via new_run_id() unless the caller
   supplies one explicitly (tests / deterministic replay).

5. ASSERTIONS MUST BE NON-EMPTY. A publish gate with zero assertions
   verifies nothing and would auto-pass every run — that is a caller
   misconfiguration, not a valid "all assertions passed" scenario, and
   is rejected before any I/O occurs.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json

from backends.postgres.connection import get_connection
from core.assertions import Assertion
from core.enums import AssertionStatus, RunStatus
from core.models import (
    AssertionResult,
    ReconciliationReport,
    RunResult,
    new_run_id,
)
from core.publisher import Publisher, PublishGateError
from core.reference_source import ReferenceResolutionError, ReferenceSource
from core.review_queue import ReviewQueueError, ReviewQueueRouter
from core.staging import StagingWriteError, StagingWriter

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ReconciliationPipeline:
    """
    Orchestrates one end-to-end WAP reconciliation run.

    Usage:
        pipeline = ReconciliationPipeline(
            staging_writer_factory=lambda run_id: PostgresStagingWriter(run_id=run_id),
        )
        result = pipeline.run(
            dataset_key="fx_rates",
            data={"USD_GBP": 0.79, "USD_EUR": 0.92},
            assertions=[FxRateReconciliation(), RowCountDelta(max_pct_delta=5.0)],
            reference_source=SnapshotReferenceSource(),
        )
        # result.status is one of RunStatus.PUBLISHED / ROUTED_TO_REVIEW / ERRORED
    """

    def __init__(
        self,
        *,
        staging_writer_factory: Callable[[str], StagingWriter],
        publisher: Publisher | None = None,
        review_router: ReviewQueueRouter | None = None,
    ) -> None:
        self._staging_writer_factory = staging_writer_factory
        self._publisher = publisher or Publisher()
        self._review_router = review_router or ReviewQueueRouter()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        dataset_key: str,
        data: Any,
        assertions: Sequence[Assertion],
        reference_source: ReferenceSource,
        run_id: str | None = None,
    ) -> RunResult:
        """
        Execute one full reconciliation run.

        Args:
            dataset_key:      Logical dataset name (e.g. "fx_rates").
            data:              Payload to stage (dict or list).
            assertions:        Assertions to run against staged data +
                               reference. Must be non-empty.
            reference_source:  Resolves the independent reference for
                               dataset_key.
            run_id:            Optional explicit run_id. Defaults to
                               new_run_id().

        Returns:
            RunResult with status PUBLISHED, ROUTED_TO_REVIEW, or
            ERRORED. Every expected failure path is represented in the
            returned RunResult rather than raised — callers branch on
            `status`, never on exceptions, per RunResult's own contract.

        Raises:
            ValueError: assertions is empty (misconfiguration, rejected
                        before any I/O).
        """
        if not assertions:
            raise ValueError(
                "assertions must be non-empty. A publish gate with zero "
                "assertions verifies nothing and would auto-pass every "
                "run — this is a caller misconfiguration."
            )

        _run_id = run_id or new_run_id()
        started_at = _utc_now()

        logger.info(
            "ReconciliationPipeline.run() starting — run_id=%s dataset_key=%s",
            _run_id,
            dataset_key,
        )

        writer = self._staging_writer_factory(_run_id)

        # ------------------------------------------------------------------
        # Step 1: Write to staging
        # ------------------------------------------------------------------
        try:
            staging_ref = writer.write(dataset_key, data)
        except StagingWriteError as exc:
            return self._errored(
                run_id=_run_id,
                dataset_key=dataset_key,
                staging_ref=f"staging://{dataset_key}/{_run_id}",
                error=f"Staging write failed: {exc}",
                started_at=started_at,
            )

        # ------------------------------------------------------------------
        # Step 2: Read back staged data — round-trip fidelity, and closes
        # the TOCTOU window between write and assertion execution.
        # ------------------------------------------------------------------
        try:
            staged_data = writer.read(staging_ref)
        except StagingWriteError as exc:
            return self._errored(
                run_id=_run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                error=f"Staging read-back failed: {exc}",
                started_at=started_at,
            )

        # ------------------------------------------------------------------
        # Step 3: Resolve independent reference
        # ------------------------------------------------------------------
        try:
            reference_data = reference_source.resolve(dataset_key)
        except ReferenceResolutionError as exc:
            return self._errored(
                run_id=_run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                error=f"Reference resolution failed: {exc}",
                started_at=started_at,
            )

        # ------------------------------------------------------------------
        # Step 4: Execute all assertions. One misbehaving assertion must
        # not hide the results of the others (invariant #2).
        # ------------------------------------------------------------------
        results: list[AssertionResult] = [
            self._run_one_assertion(a, staged_data, reference_data) for a in assertions
        ]
        report = ReconciliationReport(run_id=_run_id, results=results)

        # ------------------------------------------------------------------
        # Step 5: Publish gate or review routing
        # ------------------------------------------------------------------
        if report.all_passed:
            return self._publish(
                run_id=_run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                report=report,
                started_at=started_at,
            )

        return self._route_to_review(
            run_id=_run_id,
            dataset_key=dataset_key,
            staging_ref=staging_ref,
            report=report,
            started_at=started_at,
        )

    # ------------------------------------------------------------------
    # Internal helpers — step 5 branches
    # ------------------------------------------------------------------

    def _publish(
        self,
        *,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        report: ReconciliationReport,
        started_at: datetime,
    ) -> RunResult:
        try:
            published_ref = self._publisher.promote(
                run_id=run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                report=report,
                started_at=started_at,
            )
        except PublishGateError as exc:
            return self._errored(
                run_id=run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                error=f"Publish gate refused promotion: {exc}",
                started_at=started_at,
                report=report,
            )

        logger.info(
            "ReconciliationPipeline.run() PUBLISHED — run_id=%s published_ref=%s",
            run_id,
            published_ref,
        )
        return RunResult(
            run_id=run_id,
            status=RunStatus.PUBLISHED,
            report=report,
            staging_ref=staging_ref,
            published_ref=published_ref,
            started_at=started_at,
            completed_at=_utc_now(),
        )

    def _route_to_review(
        self,
        *,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        report: ReconciliationReport,
        started_at: datetime,
    ) -> RunResult:
        try:
            self._review_router.route(
                run_id=run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                report=report,
                started_at=started_at,
            )
        except ReviewQueueError as exc:
            return self._errored(
                run_id=run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                error=f"Review queue routing refused: {exc}",
                started_at=started_at,
                report=report,
            )

        logger.warning(
            "ReconciliationPipeline.run() ROUTED_TO_REVIEW — run_id=%s failed_assertions=%d",
            run_id,
            len(report.failed_results),
        )
        return RunResult(
            run_id=run_id,
            status=RunStatus.ROUTED_TO_REVIEW,
            report=report,
            staging_ref=staging_ref,
            started_at=started_at,
            completed_at=_utc_now(),
        )

    # ------------------------------------------------------------------
    # Internal helpers — assertion isolation
    # ------------------------------------------------------------------

    @staticmethod
    def _run_one_assertion(
        assertion: Assertion, staged_data: Any, reference_data: Any
    ) -> AssertionResult:
        """
        Execute a single assertion, converting any unexpected exception
        into an ERRORED AssertionResult rather than propagating it.
        """
        try:
            return assertion.check(staged_data, reference_data)
        except Exception as exc:
            logger.exception(
                "Assertion '%s' raised unexpectedly during check()",
                assertion.name,
            )
            return AssertionResult(
                assertion_name=assertion.name,
                status=AssertionStatus.ERRORED,
                severity=assertion.severity,
                message=f"Assertion raised an unexpected exception: {exc}",
                evidence={"exception_type": type(exc).__name__},
            )

    # ------------------------------------------------------------------
    # Internal helpers — pipeline-level ERRORED path + its own audit write
    # ------------------------------------------------------------------

    def _errored(
        self,
        *,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        error: str,
        started_at: datetime,
        report: ReconciliationReport | None = None,
    ) -> RunResult:
        """
        Build an ERRORED RunResult and write its own audit log entry.

        Covers two families of failure:
          (a) pre-report failures (staging write/read, reference
              resolution) — no ReconciliationReport exists yet, so an
              empty one is synthesized purely for audit/serialization
              shape consistency.
          (b) Publisher/ReviewQueueRouter refusals — a report already
              exists and is passed through unchanged.

        Neither Publisher nor ReviewQueueRouter write an audit row when
        THEY raise (they only write on their own success path), so this
        is the pipeline's responsibility per invariant #1.
        """
        completed_at = _utc_now()
        _report = report or ReconciliationReport(run_id=run_id, results=[])

        logger.error(
            "ReconciliationPipeline.run() ERRORED — run_id=%s dataset_key=%s error=%s",
            run_id,
            dataset_key,
            error,
        )

        try:
            self._write_errored_audit_log(
                run_id=run_id,
                dataset_key=dataset_key,
                staging_ref=staging_ref,
                report=_report,
                error=error,
                started_at=started_at,
                completed_at=completed_at,
            )
        except Exception:
            # The audit write itself failed on top of the original error —
            # this includes psycopg2.Error, but also OSError/ConnectionError
            # raised by get_connection() itself (missing DATABASE_URL, pool
            # init failure, etc). This method's sole contract is "always
            # return a classified RunResult"; letting an infrastructure
            # exception escape here would replace a clear ERRORED
            # classification with an opaque crash — strictly worse than the
            # already-degraded state we're in. Log loudly — this is the one
            # scenario where the evidence trail may genuinely be incomplete
            # — but still return a definitive answer to the caller.
            logger.exception(
                "Failed to write ERRORED audit log entry — run_id=%s. "
                "Evidence trail for this run may be incomplete.",
                run_id,
            )

        return RunResult(
            run_id=run_id,
            status=RunStatus.ERRORED,
            report=_report,
            staging_ref=staging_ref,
            error=error,
            started_at=started_at,
            completed_at=completed_at,
        )

    @staticmethod
    def _report_to_json(report: ReconciliationReport) -> dict:
        """Identical serialization contract to publisher.py / review_queue.py."""

        def _default(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if hasattr(obj, "value"):
                return obj.value
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        raw = dataclasses.asdict(report)
        return json.loads(json.dumps(raw, default=_default))

    def _write_errored_audit_log(
        self,
        *,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        report: ReconciliationReport,
        error: str,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO wap_audit_log
                    (run_id, dataset_key, status, report, staging_ref,
                     published_ref, error, started_at, completed_at)
                VALUES
                    (%(run_id)s::uuid, %(dataset_key)s, %(status)s, %(report)s,
                     %(staging_ref)s, NULL, %(error)s,
                     %(started_at)s, %(completed_at)s)
                """,
                {
                    "run_id": run_id,
                    "dataset_key": dataset_key,
                    "status": RunStatus.ERRORED.value,
                    "report": Json(self._report_to_json(report)),
                    "staging_ref": staging_ref,
                    "error": error,
                    "started_at": started_at,
                    "completed_at": completed_at,
                },
            )
            conn.commit()
