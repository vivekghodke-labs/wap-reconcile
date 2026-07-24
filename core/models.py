"""
Core data models for the WAP reconciliation framework.

Design rules enforced here (do not relax without a documented reason):

1. All models are immutable (frozen dataclasses). A RunResult or
   AssertionResult is evidence — evidence that can be mutated after
   the fact is not evidence. This directly supports the article's
   thesis: "resolved" must be a claim backed by durable proof, not a
   mutable flag.
2. No model reaches into the database or filesystem. Persistence is a
   concern for staging.py / the audit log writer, not for the models.
3. Every model carries a UTC timestamp captured at construction time,
   not left to the caller to backfill later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import uuid4

from core.enums import AssertionStatus, RunStatus, Severity


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class AssertionResult:
    """
    Outcome of a single assertion executed against staged data and a
    reference source.

    `evidence` must be enough for a human reviewer to understand *why*
    the assertion passed or failed without re-running anything — e.g.
    the actual vs. expected values, not just a boolean.
    """

    assertion_name: str
    status: AssertionStatus
    severity: Severity
    message: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    checked_at: datetime = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        if not self.assertion_name:
            raise ValueError("assertion_name must not be empty")
        if self.status is AssertionStatus.FAILED and not self.message:
            raise ValueError(
                f"AssertionResult for '{self.assertion_name}' is FAILED but has no "
                "message. A failure without an explanation is not reviewable."
            )

    @property
    def passed(self) -> bool:
        return self.status is AssertionStatus.PASSED


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    """
    Aggregate view over all AssertionResults produced by a single run.

    This is the object persisted to wap_audit_log. It is the durable
    answer to "how do you know this was resolved correctly?"
    """

    run_id: str
    results: Sequence[AssertionResult]
    generated_at: datetime = field(default_factory=_utc_now)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed_results(self) -> Sequence[AssertionResult]:
        return tuple(r for r in self.results if not r.passed)

    @property
    def highest_failed_severity(self) -> Severity | None:
        """
        Used by the review queue to decide routing priority. Returns None
        if nothing failed.
        """
        order = {Severity.CRITICAL: 0, Severity.WARN: 1, Severity.INFO: 2}
        failed = self.failed_results
        if not failed:
            return None
        return min((r.severity for r in failed), key=lambda s: order[s])


@dataclass(frozen=True, slots=True)
class RunResult:
    """
    Terminal outcome of one ReconciliationPipeline.run() invocation.

    Callers (Airflow operator, CLI, service layer) should only ever
    branch on `status` — never re-derive pass/fail from `report`
    themselves, to keep the decision logic in one place
    (publisher.py / review_queue.py).
    """

    run_id: str
    status: RunStatus
    report: ReconciliationReport
    staging_ref: str
    published_ref: str | None = None
    error: str | None = None
    started_at: datetime = field(default_factory=_utc_now)
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.status is RunStatus.PUBLISHED and self.published_ref is None:
            raise ValueError(
                "RunResult status is PUBLISHED but published_ref is None. "
                "A publish claim requires a pointer to what was published."
            )
        if self.status is RunStatus.ERRORED and not self.error:
            raise ValueError(
                "RunResult status is ERRORED but no error message was captured."
            )


def new_run_id() -> str:
    """Centralized so run_id generation strategy can change in one place."""
    return str(uuid4())