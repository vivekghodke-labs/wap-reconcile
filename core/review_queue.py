"""
ReviewQueueRouter — failed-run routing for the WAP reconciliation framework.

Responsibility: when a ReconciliationReport contains one or more failed
assertions, route the run to wap_review_queue so a human (or a delegated
triage tool) can inspect it, and simultaneously write the audit log entry
so every run has a durable evidence trail regardless of outcome.

Design invariants (do not relax without a documented reason):

1. ATOMICITY — The review queue insert and the audit log insert execute
   in a single database transaction. A run that is queued for review
   but has no audit entry is not auditable. One commit, or full rollback.

2. DEFENSIVE GATE — route() raises ReviewQueueError if report.all_passed
   is True. Routing a passing run to the review queue is a caller bug —
   it would silently suppress a successful publish. We make the misuse
   loud rather than tolerating it.

3. IDEMPOTENCY GUARD — wap_review_queue has a UNIQUE constraint on
   run_id (migration 002). Calling route() twice for the same run_id
   raises ReviewQueueError. This prevents a retry loop from flooding
   the queue with duplicate entries for the same failure.

4. STAGING RECORD UNTOUCHED — route() does not modify or delete the
   staging record. The staged payload remains available for human
   inspection and audit replay. Staging cleanup is a separate
   operational concern.

5. RESOLUTION IS EXTERNAL — This class only writes the initial queue
   entry. Resolving a queue item (approved_publish, rejected,
   false_positive) is a separate operation performed by a human via
   tooling, or by an explicitly delegated automated resolver. The
   framework does not auto-resolve review queue entries.

Severity routing:
    The queue entry's `severity` field is set from
    report.highest_failed_severity — the most critical severity among
    all failed assertions. This drives triage priority in downstream
    tooling: CRITICAL failures float to the top, INFO-level distribution
    drifts do not page an on-call engineer.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json

from backends.postgres.connection import get_connection
from core.enums import RunStatus, Severity
from core.models import ReconciliationReport

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ReviewQueueError(Exception):
    """
    Raised when routing is refused or fails for a business-rule reason.

    Distinct from psycopg2 errors so callers can distinguish a business-
    rule refusal (report passed, duplicate run_id) from a transient
    infrastructure failure.
    """


# ---------------------------------------------------------------------------
# Internal helpers (module-private)
# ---------------------------------------------------------------------------


def _report_to_json(report: ReconciliationReport) -> dict:
    """
    Serialize a ReconciliationReport to a plain dict for JSONB storage.

    Identical logic to publisher._report_to_json — kept local to avoid
    coupling review_queue to publisher's internal helpers. If a shared
    serialization utility is needed in future, extract to core/serialization.py.
    """

    def _default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "value"):
            return obj.value
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    raw = dataclasses.asdict(report)
    return json.loads(json.dumps(raw, default=_default))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _severity_value(severity: Severity | None) -> str:
    """
    Convert the highest failed severity to its string value for storage.

    Falls back to WARN if somehow None slips through — this should never
    happen because route() already validates the report has failures, but
    a default prevents a NULL constraint violation if it does.
    """
    if severity is None:
        return Severity.WARN.value
    return severity.value


# ---------------------------------------------------------------------------
# ReviewQueueRouter
# ---------------------------------------------------------------------------


class ReviewQueueRouter:
    """
    Routes a failed reconciliation run to the human review queue.

    Stateless — safe to instantiate once per process and reuse. Thread-safe:
    each route() call acquires its own connection from the pool.

    Usage (called by ReconciliationPipeline in Day 5):

        router = ReviewQueueRouter()
        queue_id = router.route(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref="staging://fx_rates/<uuid>",
            report=reconciliation_report,
            started_at=pipeline_start_time,
        )
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def route(
        self,
        *,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        report: ReconciliationReport,
        started_at: datetime | None = None,
    ) -> int:
        """
        Insert a failed run into the review queue and audit log.

        Args:
            run_id:      UUID4 string — correlation key across all tables.
            dataset_key: Logical dataset name (e.g. "fx_rates").
            staging_ref: The staging_ref produced by StagingWriter.write().
                         Stored in the queue entry so reviewers can locate
                         the staged data without additional lookups.
            report:      ReconciliationReport with at least one failed
                         assertion. Must NOT have all_passed == True.
            started_at:  Pipeline run start time for audit log duration
                         tracking. Defaults to now() if not supplied.

        Returns:
            queue_id (int): The primary key of the wap_review_queue row.
                            Useful for callers that need to reference the
                            queue entry (e.g. linking in notifications).

        Raises:
            ReviewQueueError: report.all_passed is True (misuse), or
                              run_id has already been queued (duplicate).
            psycopg2.Error:   Transient DB failure — propagates to caller
                              for ERRORED classification.
        """
        self._validate_report_failed(report, dataset_key)

        now = _utc_now()
        run_started_at = started_at or now
        severity = _severity_value(report.highest_failed_severity)

        logger.warning(
            "ReviewQueueRouter.route() — run_id=%s dataset_key=%s severity=%s failed_assertions=%d",
            run_id,
            dataset_key,
            severity,
            len(report.failed_results),
        )

        try:
            with get_connection() as conn:
                queue_id = self._execute_route_transaction(
                    conn=conn,
                    run_id=run_id,
                    dataset_key=dataset_key,
                    staging_ref=staging_ref,
                    report=report,
                    severity=severity,
                    started_at=run_started_at,
                    completed_at=now,
                )
                conn.commit()

        except psycopg2.errors.UniqueViolation as exc:
            raise ReviewQueueError(
                f"run_id='{run_id}' is already present in the review queue "
                f"for dataset_key='{dataset_key}'. Duplicate routing suppressed. "
                "If this is a genuine re-run, use a new run_id via new_run_id()."
            ) from exc

        except psycopg2.Error:
            logger.exception(
                "ReviewQueueRouter.route() DB error — run_id=%s dataset_key=%s",
                run_id,
                dataset_key,
            )
            raise

        logger.warning(
            "ReviewQueueRouter.route() queued — queue_id=%d run_id=%s",
            queue_id,
            run_id,
        )
        return queue_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_report_failed(report: ReconciliationReport, dataset_key: str) -> None:
        """
        Defensive gate: refuse routing if the report has no failures.

        A passing report routed to the review queue would block a
        legitimate publish and confuse reviewers. Surface the misuse
        loudly at the call site.
        """
        if not report.all_passed:
            return

        raise ReviewQueueError(
            f"Routing refused for dataset_key='{dataset_key}' "
            f"run_id='{report.run_id}': report.all_passed is True — "
            "there is nothing to review. Route passing runs to "
            "Publisher.promote(), not ReviewQueueRouter."
        )

    @staticmethod
    def _insert_review_queue_entry(
        cur: psycopg2.extensions.cursor,
        *,
        run_id: str,
        dataset_key: str,
        severity: str,
        report: ReconciliationReport,
        staging_ref: str,
        queued_at: datetime,
    ) -> int:
        """
        Insert into wap_review_queue and return the generated primary key.
        """
        cur.execute(
            """
            INSERT INTO wap_review_queue
                (run_id, dataset_key, severity, report, staging_ref, queued_at)
            VALUES
                (%(run_id)s::uuid, %(dataset_key)s, %(severity)s,
                 %(report)s, %(staging_ref)s, %(queued_at)s)
            RETURNING id
            """,
            {
                "run_id": run_id,
                "dataset_key": dataset_key,
                "severity": severity,
                "report": Json(_report_to_json(report)),
                "staging_ref": staging_ref,
                "queued_at": queued_at,
            },
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("INSERT RETURNING id yielded no rows")
        return row[0]  # RETURNING id

    @staticmethod
    def _insert_audit_log(
        cur: psycopg2.extensions.cursor,
        *,
        run_id: str,
        dataset_key: str,
        report: ReconciliationReport,
        staging_ref: str,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        cur.execute(
            """
            INSERT INTO wap_audit_log
                (run_id, dataset_key, status, report, staging_ref,
                 published_ref, error, started_at, completed_at)
            VALUES
                (%(run_id)s::uuid, %(dataset_key)s, %(status)s, %(report)s,
                 %(staging_ref)s, NULL, NULL,
                 %(started_at)s, %(completed_at)s)
            """,
            {
                "run_id": run_id,
                "dataset_key": dataset_key,
                "status": RunStatus.ROUTED_TO_REVIEW.value,
                "report": Json(_report_to_json(report)),
                "staging_ref": staging_ref,
                "started_at": started_at,
                "completed_at": completed_at,
            },
        )

    def _execute_route_transaction(
        self,
        *,
        conn: psycopg2.extensions.connection,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        report: ReconciliationReport,
        severity: str,
        started_at: datetime,
        completed_at: datetime,
    ) -> int:
        """
        Execute all writes for the fail path inside a single transaction.

        Order:
          1. Insert into wap_review_queue (returns queue_id)
          2. Insert into wap_audit_log

        If any step fails, the transaction is rolled back — no partial state.
        Returns queue_id for the caller.
        """
        with conn.cursor() as cur:
            queue_id = self._insert_review_queue_entry(
                cur,
                run_id=run_id,
                dataset_key=dataset_key,
                severity=severity,
                report=report,
                staging_ref=staging_ref,
                queued_at=completed_at,
            )

            self._insert_audit_log(
                cur,
                run_id=run_id,
                dataset_key=dataset_key,
                report=report,
                staging_ref=staging_ref,
                started_at=started_at,
                completed_at=completed_at,
            )

        return queue_id
