"""
Publisher — the WAP publish gate.

Responsibility: promote a staged record to published.records if and
only if all assertions in the supplied ReconciliationReport passed.
Every call — pass or raise — writes an entry to wap_audit_log so the
audit trail is complete regardless of outcome.

Design invariants (do not relax without a documented reason):

1. ATOMICITY — The publish insert and the audit log insert are executed
   in a single database transaction. A published record with no audit
   entry, or an audit entry claiming publication that never happened,
   are both silent correctness failures worse than the FX scenario
   from the article. One commit covers both, or both are rolled back.

2. HARD GATE — promote() raises PublishGateError if report.all_passed
   is False. It does not attempt to publish partial results, it does
   not warn and continue, it does not accept a force flag. Callers that
   need to handle a failed report must route to ReviewQueueRouter, not
   to Publisher.

3. IDEMPOTENCY GUARD — A run_id can only be published once. The
   published.records table has a UNIQUE constraint on run_id
   (migration 001). If promote() is called twice for the same run_id
   (e.g. a retry after a transient commit failure), it raises
   PublishGateError rather than silently overwriting. Callers must
   issue a new run_id for a genuine re-run.

4. NO PAYLOAD RE-FETCH — Publisher reads the staged payload from
   staging.records inside the same transaction as the publish insert.
   This eliminates a TOCTOU window where the staged record could be
   truncated (by clean_db in tests, or by a concurrent maintenance job)
   between assertion execution and publish. The payload that assertions
   ran against is exactly the payload that gets promoted.

5. STAGING RECORD UNTOUCHED — promote() does NOT delete or update the
   staging record. Staging cleanup is a separate operational concern
   (e.g. a scheduled truncation job). This preserves audit replayability:
   a human can always re-read staging.records for a run_id and verify
   that what was promoted matches what the assertions saw.

published_ref format: published://{dataset_key}/{run_id}
    Mirrors the staging_ref scheme from PostgresStagingWriter.
    The run_id component is shared — the audit log correlates them.
"""

from __future__ import annotations
from typing import Any, cast

import dataclasses
import json
import logging
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from backends.postgres.connection import get_connection
from core.enums import RunStatus
from core.models import ReconciliationReport

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PublishGateError(Exception):
    """
    Raised when promotion is refused.

    Distinct from psycopg2 errors so callers can distinguish a business-
    rule refusal (assertions failed, duplicate run_id) from a transient
    infrastructure error. Only business-rule refusals raise this; DB
    errors propagate as-is so the pipeline layer can classify them as
    ERRORED runs.
    """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _report_to_json(report: ReconciliationReport) -> dict:
    """
    Serialize a ReconciliationReport to a plain dict for JSONB storage.

    Uses dataclasses.asdict() for deep traversal. datetime fields are
    ISO-formatted strings so they survive a JSONB round-trip without
    losing timezone information.
    """

    def _default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Enums: str(Enum) gives "ClassName.VALUE"; .value gives "value"
        if hasattr(obj, "value"):
            return obj.value
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    raw = dataclasses.asdict(report)
    # Round-trip through json to apply _default to nested enums/datetimes,
    # then parse back to dict for psycopg2's Json() adapter.
    return json.loads(json.dumps(raw, default=_default))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


class Publisher:
    """
    Promotes staged data to the published schema after all assertions pass.

    Stateless — no instance state beyond the logger. Safe to instantiate
    once per process and reuse across pipeline runs. Thread-safe: each
    promote() call acquires its own connection from the pool.

    Usage (called by ReconciliationPipeline in Day 5, not directly by
    application code):

        publisher = Publisher()
        published_ref = publisher.promote(
            run_id=run_id,
            dataset_key="fx_rates",
            staging_ref="staging://fx_rates/<uuid>",
            report=reconciliation_report,
        )
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def promote(
        self,
        *,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        report: ReconciliationReport,
        started_at: datetime | None = None,
    ) -> str:
        """
        Promote a staged record to published.records.

        All assertions in `report` must have passed. If any failed, raises
        PublishGateError — callers must route to ReviewQueueRouter instead.

        Args:
            run_id:      UUID4 string. Must match the run_id used when
                         staging the data. Used as the correlation key
                         across staging, published, and audit_log tables.
            dataset_key: Logical dataset name (e.g. "fx_rates"). Used to
                         construct the published_ref URI and for audit
                         log indexing.
            staging_ref: The ref returned by StagingWriter.write(). The
                         staged payload is read from this ref inside the
                         publish transaction.
            report:      ReconciliationReport produced by running all
                         assertions. Must have all_passed == True.
            started_at:  Pipeline run start time for audit log. Defaults
                         to now() if not supplied.

        Returns:
            published_ref: "published://{dataset_key}/{run_id}"

        Raises:
            PublishGateError: report.all_passed is False, or run_id has
                              already been published (duplicate).
            psycopg2.Error:   Transient DB failure — caller classifies
                              as ERRORED and retries with a new run_id.
        """
        self._validate_report_passed(report, dataset_key)

        published_ref = f"published://{dataset_key}/{run_id}"
        now = _utc_now()
        run_started_at = started_at or now

        logger.info(
            "Publisher.promote() starting — run_id=%s dataset_key=%s",
            run_id,
            dataset_key,
        )

        try:
            with get_connection() as conn:
                self._execute_publish_transaction(
                    conn=conn,
                    run_id=run_id,
                    dataset_key=dataset_key,
                    staging_ref=staging_ref,
                    published_ref=published_ref,
                    report=report,
                    started_at=run_started_at,
                    completed_at=now,
                )
                conn.commit()

        except psycopg2.errors.UniqueViolation as exc:
            # The UNIQUE constraint on published.records(run_id) fired.
            # This is a business-rule violation, not a transient DB error.
            raise PublishGateError(
                f"run_id='{run_id}' has already been published for "
                f"dataset_key='{dataset_key}'. Each pipeline run must use "
                "a fresh run_id via new_run_id(). Do not retry with the "
                "same run_id after a successful publish."
            ) from exc

        except psycopg2.Error:
            # Transient infrastructure failure — let it propagate so the
            # pipeline layer can classify this run as ERRORED.
            logger.exception(
                "Publisher.promote() DB error — run_id=%s dataset_key=%s",
                run_id,
                dataset_key,
            )
            raise

        logger.info(
            "Publisher.promote() succeeded — published_ref=%s",
            published_ref,
        )
        return published_ref

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_report_passed(report: ReconciliationReport, dataset_key: str) -> None:
        """
        Hard gate: refuse promotion if any assertion failed.

        Raises PublishGateError with a human-readable summary of which
        assertions failed — this message goes into the caller's logs and
        optionally into the review queue, so it must be actionable.
        """
        if report.all_passed:
            return

        failed_names = [
            f"{r.assertion_name} [{r.severity.value}]: {r.message}" for r in report.failed_results
        ]
        summary = "; ".join(failed_names)

        raise PublishGateError(
            f"Promotion refused for dataset_key='{dataset_key}' "
            f"run_id='{report.run_id}': "
            f"{len(report.failed_results)} assertion(s) failed — {summary}. "
            "Route this run to ReviewQueueRouter, not Publisher."
        )

    @staticmethod
    def _read_staged_payload(cur: psycopg2.extensions.cursor, staging_ref: str) -> dict | list:
        """
        Read the staged payload inside the publish transaction.

        Acquiring the payload inside the transaction (rather than before
        it) closes the TOCTOU window between assertion execution and
        promotion — if the staging record was deleted concurrently, we
        fail explicitly here rather than promoting an empty payload.
        """
        cur.execute(
            "SELECT payload FROM staging.records WHERE staging_ref = %(ref)s FOR SHARE",
            {"ref": staging_ref},
        )
        row = cur.fetchone()
        if row is None:
            raise PublishGateError(
                f"Staged record not found for staging_ref='{staging_ref}'. "
                "The record may have been deleted between assertion execution "
                "and promotion. Re-run with a new run_id."
            )
        return cast(dict[str, Any], row)["payload"]

    @staticmethod
    def _insert_published_record(
        cur: psycopg2.extensions.cursor,
        *,
        published_ref: str,
        dataset_key: str,
        run_id: str,
        staging_ref: str,
        payload: dict | list,
        published_at: datetime,
    ) -> None:
        cur.execute(
            """
            INSERT INTO published.records
                (published_ref, dataset_key, run_id, staging_ref, payload, published_at)
            VALUES
                (%(published_ref)s, %(dataset_key)s, %(run_id)s::uuid,
                 %(staging_ref)s, %(payload)s, %(published_at)s)
            """,
            {
                "published_ref": published_ref,
                "dataset_key": dataset_key,
                "run_id": run_id,
                "staging_ref": staging_ref,
                "payload": Json(payload),
                "published_at": published_at,
            },
        )

    @staticmethod
    def _insert_audit_log(
        cur: psycopg2.extensions.cursor,
        *,
        run_id: str,
        dataset_key: str,
        status: str,
        report: ReconciliationReport,
        staging_ref: str,
        published_ref: str | None,
        started_at: datetime,
        completed_at: datetime,
        error: str | None = None,
    ) -> None:
        cur.execute(
            """
            INSERT INTO wap_audit_log
                (run_id, dataset_key, status, report, staging_ref,
                 published_ref, error, started_at, completed_at)
            VALUES
                (%(run_id)s::uuid, %(dataset_key)s, %(status)s, %(report)s,
                 %(staging_ref)s, %(published_ref)s, %(error)s,
                 %(started_at)s, %(completed_at)s)
            """,
            {
                "run_id": run_id,
                "dataset_key": dataset_key,
                "status": status,
                "report": Json(_report_to_json(report)),
                "staging_ref": staging_ref,
                "published_ref": published_ref,
                "error": error,
                "started_at": started_at,
                "completed_at": completed_at,
            },
        )

    def _execute_publish_transaction(
        self,
        *,
        conn: psycopg2.extensions.connection,
        run_id: str,
        dataset_key: str,
        staging_ref: str,
        published_ref: str,
        report: ReconciliationReport,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        """
        Execute all writes for the pass path inside a single transaction.

        Order:
          1. Read staged payload (FOR SHARE lock — prevents concurrent deletion)
          2. Insert into published.records
          3. Insert into wap_audit_log

        If any step fails, conn.commit() is never called and the
        transaction is rolled back by the caller's context manager exit.
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Step 1: read staged payload inside the transaction
            payload = self._read_staged_payload(cur, staging_ref)

            # Step 2: promote to published schema
            self._insert_published_record(
                cur,
                published_ref=published_ref,
                dataset_key=dataset_key,
                run_id=run_id,
                staging_ref=staging_ref,
                payload=payload,
                published_at=completed_at,
            )

            # Step 3: write audit log — same transaction, same commit
            self._insert_audit_log(
                cur,
                run_id=run_id,
                dataset_key=dataset_key,
                status=RunStatus.PUBLISHED.value,
                report=report,
                staging_ref=staging_ref,
                published_ref=published_ref,
                started_at=started_at,
                completed_at=completed_at,
            )
