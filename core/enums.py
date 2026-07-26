"""
Shared enums for the WAP reconciliation framework.

Kept in a separate module (rather than inlined in models.py) so that
adapters (e.g. Airflow operator) can import status/severity types without
pulling in the full model surface.
"""

from enum import Enum


class AssertionStatus(str, Enum):
    """Outcome of a single assertion check."""

    PASSED = "passed"
    FAILED = "failed"
    ERRORED = (
        "errored"  # assertion itself could not execute (e.g. reference unavailable)
    )


class RunStatus(str, Enum):
    """Terminal state of a full reconciliation run."""

    PUBLISHED = "published"  # all assertions passed, staging promoted
    ROUTED_TO_REVIEW = "routed_to_review"  # one or more assertions failed
    ERRORED = "errored"  # pipeline-level failure


class Severity(str, Enum):
    """
    Severity of an assertion, independent of pass/fail.

    Used by the review queue to prioritize triage — a CRITICAL failure
    (e.g. FX reconciliation) should not sit in the same queue lane as a
    WARN-level distribution drift.
    """

    CRITICAL = "critical"
    WARN = "warn"
    INFO = "info"
