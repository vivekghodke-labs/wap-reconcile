"""
NumericDistributionCheck — statistical distribution assertion on a
single numeric field across staged vs. reference datasets.

DOCUMENTED LIMITATION (from the article, verbatim):
    "A distribution check confirms a number is statistically plausible,
    not that it's correct. [...] a meaning-preserving swap, like two
    status codes trading places at similar frequency, moves neither the
    shape nor the schema, and slips past all three."

This assertion is deliberately included with that limitation documented
in its docstring because it catches a real and common failure class:
unit swaps, scale errors, and population drift — all of which move the
aggregate statistics this assertion watches. It does NOT catch:
- Meaning-preserving swaps (status codes swapping at similar frequency)
- Semantic drift where the distribution is coincidentally preserved

Teams must use this in combination with FxRateReconciliation (or a
domain-specific value-level assertion) for complete coverage. This
assertion is the "cheap layer"; FxRateReconciliation is the "expensive
layer" the article argues most teams skip.

What it catches:
- Mean drift: staged mean deviates from reference mean by > tolerance
- Std-dev drift: staged std-dev deviates from reference std-dev by > tolerance
- Insufficient sample size in staged (configurable min_sample_size)
- Non-numeric field values

Oil & Gas applicability:
- Daily production volumes (bbl/day): mean drift flags production anomalies
- Wellhead pressure readings: std-dev drift flags sensor instability
- Gas-oil ratios: both mean and std-dev drift flag separator issues
- Royalty rate distributions: mean drift flags rate table corruption

Design:
- `staged` and `reference` are both dict with a `values` key holding
  list[float | int]. This is the most portable contract — it doesn't
  assume pandas, numpy, or any particular columnar format.
- Mean and std-dev tolerances are expressed as a fraction of the
  reference value (relative tolerance), not absolute, so the same
  assertion works for both small rates (0.79) and large volumes (500000).
- numpy is intentionally NOT used. stdlib math is sufficient for the
  sample sizes typical in WAP assertion checks, and adding numpy as a
  core dependency would make the framework heavier than it needs to be
  for a reference implementation.
"""

from __future__ import annotations

import math
from typing import Any

from core.assertions import Assertion
from core.enums import AssertionStatus, Severity
from core.models import AssertionResult

_VALUES_KEY = "values"


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _stddev(values: list[float]) -> float:
    """Population std-dev (not sample). Consistent with reference calculation."""
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    variance = sum((x - mu) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


class NumericDistributionCheck(Assertion):
    """
    Checks that the mean and standard deviation of a numeric field in
    staged data remain within configurable relative tolerance bands
    compared to the reference.

    LIMITATION: catches shape/scale drift only. Does not catch
    meaning-preserving swaps. See module docstring.

    Args:
        field:           Key in both staged and reference payloads that
                         holds list[float | int] of values to analyse.
                         Default: "values".
        max_mean_drift:  Maximum allowed relative drift in mean, expressed
                         as a fraction (e.g. 0.05 = 5%). Default: 0.05.
        max_stddev_drift: Maximum allowed relative drift in std-dev,
                         expressed as a fraction. Default: 0.10 (10%).
                         Looser than mean because std-dev is naturally
                         more volatile over small windows.
        min_sample_size: Minimum number of values required in staged for
                         the assertion to be meaningful. Default: 10.
                         Below this, FAILED is returned (not ERRORED)
                         because an insufficient sample is a data-quality
                         issue, not a framework error.

    Usage:
        assertion = NumericDistributionCheck(
            field="production_bbl",
            max_mean_drift=0.05,
            max_stddev_drift=0.10,
        )
        result = assertion.check(
            staged={"production_bbl": [520, 518, 515, ...]},
            reference={"production_bbl": [510, 512, 508, ...]},
        )
    """

    name: str = "numeric_distribution_check"
    severity: Severity = Severity.WARN  # Shape drift is a warning, not a block

    def __init__(
        self,
        *,
        field: str = _VALUES_KEY,
        max_mean_drift: float = 0.05,
        max_stddev_drift: float = 0.10,
        min_sample_size: int = 10,
    ) -> None:
        if max_mean_drift < 0:
            raise ValueError("max_mean_drift must be >= 0")
        if max_stddev_drift < 0:
            raise ValueError("max_stddev_drift must be >= 0")
        if min_sample_size < 1:
            raise ValueError("min_sample_size must be >= 1")

        self._field = field
        self._max_mean_drift = max_mean_drift
        self._max_stddev_drift = max_stddev_drift
        self._min_sample_size = min_sample_size

    # ------------------------------------------------------------------
    # Assertion interface
    # ------------------------------------------------------------------

    def check(self, staged: Any, reference: Any) -> AssertionResult:
        """
        Execute distribution check on the configured field.

        Args:
            staged:    dict with self._field → list[float | int]
            reference: dict with self._field → list[float | int]

        Returns:
            AssertionResult PASSED or FAILED with statistics in evidence.
        """
        extraction = self._extract_values(staged, reference)
        if isinstance(extraction, AssertionResult):
            return extraction

        staged_vals, reference_vals = extraction

        # Sample size guard
        if len(staged_vals) < self._min_sample_size:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"Staged sample size {len(staged_vals)} is below "
                    f"minimum required {self._min_sample_size}. "
                    "Distribution check is not meaningful on this sample."
                ),
                evidence={
                    "staged_sample_size": len(staged_vals),
                    "min_sample_size": self._min_sample_size,
                    "field": self._field,
                },
            )

        staged_mean = _mean(staged_vals)
        reference_mean = _mean(reference_vals)
        staged_stddev = _stddev(staged_vals)
        reference_stddev = _stddev(reference_vals)

        failures: list[str] = []
        evidence: dict[str, Any] = {
            "field": self._field,
            "staged_sample_size": len(staged_vals),
            "reference_sample_size": len(reference_vals),
            "staged_mean": round(staged_mean, 6),
            "reference_mean": round(reference_mean, 6),
            "staged_stddev": round(staged_stddev, 6),
            "reference_stddev": round(reference_stddev, 6),
            "max_mean_drift": self._max_mean_drift,
            "max_stddev_drift": self._max_stddev_drift,
        }

        # Mean drift check
        mean_drift = self._relative_drift(staged_mean, reference_mean)
        evidence["mean_drift"] = (
            round(mean_drift, 6) if mean_drift is not None else None
        )

        if mean_drift is None:
            # reference_mean is 0 — relative drift undefined
            if abs(staged_mean) > 1e-10:
                failures.append(
                    f"reference mean is 0 but staged mean is {staged_mean:.6f}"
                )
        elif mean_drift > self._max_mean_drift:
            failures.append(
                f"mean drift {mean_drift:.4%} exceeds threshold {self._max_mean_drift:.4%} "
                f"(staged={staged_mean:.6f}, reference={reference_mean:.6f})"
            )

        # Std-dev drift check
        stddev_drift = self._relative_drift(staged_stddev, reference_stddev)
        evidence["stddev_drift"] = (
            round(stddev_drift, 6) if stddev_drift is not None else None
        )

        if stddev_drift is None:
            if staged_stddev > 1e-10:
                failures.append(
                    f"reference std-dev is 0 but staged std-dev is {staged_stddev:.6f}"
                )
        elif stddev_drift > self._max_stddev_drift:
            failures.append(
                f"std-dev drift {stddev_drift:.4%} exceeds threshold {self._max_stddev_drift:.4%} "
                f"(staged={staged_stddev:.6f}, reference={reference_stddev:.6f})"
            )

        if not failures:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.PASSED,
                severity=self.severity,
                message=(
                    f"Distribution of '{self._field}' is within tolerance. "
                    f"mean_drift={evidence['mean_drift']:.4%}, "
                    f"stddev_drift={evidence['stddev_drift']:.4%}."
                    if evidence.get("mean_drift") is not None
                    and evidence.get("stddev_drift") is not None
                    else f"Distribution of '{self._field}' is within tolerance."
                ),
                evidence=evidence,
            )

        evidence["failures"] = failures
        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.FAILED,
            severity=self.severity,
            message=(
                f"Distribution check failed for field '{self._field}': "
                + "; ".join(failures)
                + ". NOTE: this assertion catches shape/scale drift only — "
                "see module docstring for documented limitations."
            ),
            evidence=evidence,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_values(
        self, staged: Any, reference: Any
    ) -> tuple[list[float], list[float]] | AssertionResult:
        """
        Extract and validate numeric value lists from both payloads.
        Returns (staged_vals, reference_vals) or a FAILED AssertionResult.
        """
        field = self._field

        for label, payload in (("staged", staged), ("reference", reference)):
            if not isinstance(payload, dict):
                return AssertionResult(
                    assertion_name=self.name,
                    status=AssertionStatus.FAILED,
                    severity=self.severity,
                    message=f"{label} payload must be a dict, got {type(payload).__name__}.",
                    evidence={"source": label, "type": type(payload).__name__},
                )
            if field not in payload:
                return AssertionResult(
                    assertion_name=self.name,
                    status=AssertionStatus.FAILED,
                    severity=self.severity,
                    message=f"{label} payload missing required key '{field}'.",
                    evidence={"source": label, "missing_key": field},
                )
            values = payload[field]
            if not isinstance(values, list) or not values:
                return AssertionResult(
                    assertion_name=self.name,
                    status=AssertionStatus.FAILED,
                    severity=self.severity,
                    message=(
                        f"{label}['{field}'] must be a non-empty list, "
                        f"got {type(values).__name__}."
                    ),
                    evidence={"source": label, "field": field},
                )
            non_numeric = [v for v in values if not isinstance(v, (int, float))]
            if non_numeric:
                return AssertionResult(
                    assertion_name=self.name,
                    status=AssertionStatus.FAILED,
                    severity=self.severity,
                    message=(
                        f"{label}['{field}'] contains {len(non_numeric)} non-numeric "
                        f"value(s): {non_numeric[:5]!r}{'...' if len(non_numeric) > 5 else ''}."
                    ),
                    evidence={
                        "source": label,
                        "non_numeric_count": len(non_numeric),
                        "sample": non_numeric[:5],
                    },
                )

        staged_vals = [float(v) for v in staged[field]]
        reference_vals = [float(v) for v in reference[field]]
        return staged_vals, reference_vals

    @staticmethod
    def _relative_drift(staged_val: float, reference_val: float) -> float | None:
        """
        Compute relative drift = |staged - reference| / |reference|.
        Returns None if reference_val is 0 (caller handles undefined case).
        """
        if abs(reference_val) < 1e-10:
            return None
        return abs(staged_val - reference_val) / abs(reference_val)
