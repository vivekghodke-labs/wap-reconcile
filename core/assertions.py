"""
Assertion contract.

An Assertion is the unit of "verification" in the detection /
remediation / verification split described in the article. It must
compare staged data against an independent reference — never against
itself — or it degenerates into a structural/freshness check that
cannot catch semantic drift.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from core.enums import Severity
from core.models import AssertionResult


class Assertion(ABC):
    """
    Base class for all pluggable assertions.

    Subclasses must NOT mutate `staged` or `reference`. An assertion
    that mutates its inputs cannot be safely re-run for audit replay,
    which defeats the purpose of the audit log.

    `staged` and `reference` are intentionally typed as `Any` at this
    layer — the core framework does not mandate pandas, Polars, a list
    of dicts, or a DB cursor. Concrete assertions (e.g. in
    assertions_library/) declare and enforce the concrete type they need.
    """

    #: Overridden by subclasses. Used in AssertionResult.assertion_name
    #: and in review-queue routing/labeling.
    name: str = "unnamed_assertion"

    #: Overridden by subclasses. Determines review-queue priority on failure.
    severity: Severity = Severity.WARN

    @abstractmethod
    def check(self, staged: Any, reference: Any) -> AssertionResult:
        """
        Execute the check and return a result.

        Implementations must not raise for expected failure conditions
        (e.g. "values don't reconcile") — that is a normal FAILED
        result, not an exception. Exceptions should be reserved for
        genuinely unexpected errors (e.g. malformed input shape), which
        the pipeline will catch and convert to AssertionStatus.ERRORED.
        """
        raise NotImplementedError

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return f"<{self.__class__.__name__} name={self.name!r} severity={self.severity.value}>"
