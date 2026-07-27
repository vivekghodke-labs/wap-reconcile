"""
RowCountDelta — assertion that catches volume anomalies between staged
and reference datasets.

Failure classes caught:
- Silent truncation (partial load — 10k rows expected, 200 arrived)
- Runaway duplication (fan-out join bug — 10k rows became 500k)
- Empty load (zero rows staged — the most common silent failure)
- Threshold breach in either direction (configurable)

Design decisions:
- Supports both absolute delta (±N rows) and percentage delta (±N%).
  Percentage is appropriate when dataset size varies day-to-day (e.g.
  well production records — active wells fluctuate). Absolute is
  appropriate for fixed-cardinality reference tables (currency pairs,
  field codes).
- `staged` and `reference` payloads are expected to carry a `row_count`
  key (int). This is the most portable contract — any upstream system
  can emit a count without exposing raw data.
- Zero staged rows always fail regardless of threshold, because a zero-
  row publish is never a valid "all assertions passed" scenario for
  operational data. This default is overrideable via `allow_empty`.

Oil & Gas applicability:
- Daily well production records: percentage threshold (production volumes
  vary; a 50% single-day swing is anomalous but possible during workovers).
- Royalty rate lookup tables: absolute threshold (fixed-cardinality,
  any row count change is suspicious).
- Equipment sensor readings: absolute threshold with tight bound (sensor
  dropout vs expected 86400 readings/day).
"""

from __future__ import annotations

from typing import Any

from core.assertions import Assertion
from core.enums import AssertionStatus, Severity
from core.models import AssertionResult

_ROW_COUNT_KEY = "row_count"


class RowCountDelta(Assertion):
    """
    Validates that the staged row count does not deviate from the
    reference row count beyond a configurable threshold.

    Args:
        max_absolute_delta: Maximum allowed absolute row count difference.
                            Mutually exclusive with max_pct_delta.
        max_pct_delta:      Maximum allowed percentage difference (0–100).
                            Mutually exclusive with max_absolute_delta.
        allow_empty:        If False (default), staged row_count == 0
                            always fails, regardless of threshold.
        row_count_key:      Key in the payload dict that holds the count.
                            Default: "row_count". Override for payloads
                            that use a different field name.

    Exactly one of max_absolute_delta or max_pct_delta must be supplied.

    Usage:
        # Percentage mode — for variable-cardinality datasets
        assertion = RowCountDelta(max_pct_delta=10.0)

        # Absolute mode — for fixed-cardinality reference tables
        assertion = RowCountDelta(max_absolute_delta=5)

        result = assertion.check(
            staged={"row_count": 9850},
            reference={"row_count": 10000},
        )
    """

    name: str = "row_count_delta"
    severity: Severity = Severity.WARN

    def __init__(
        self,
        *,
        max_absolute_delta: int | None = None,
        max_pct_delta: float | None = None,
        allow_empty: bool = False,
        row_count_key: str = _ROW_COUNT_KEY,
    ) -> None:
        if (max_absolute_delta is None) == (max_pct_delta is None):
            raise ValueError("Exactly one of max_absolute_delta or max_pct_delta must be provided.")
        if max_absolute_delta is not None and max_absolute_delta < 0:
            raise ValueError("max_absolute_delta must be >= 0")
        if max_pct_delta is not None and not (0.0 <= max_pct_delta <= 100.0):
            raise ValueError("max_pct_delta must be between 0.0 and 100.0")

        self._max_absolute_delta = max_absolute_delta
        self._max_pct_delta = max_pct_delta
        self._allow_empty = allow_empty
        self._row_count_key = row_count_key

    # ------------------------------------------------------------------
    # Assertion interface
    # ------------------------------------------------------------------

    def check(self, staged: Any, reference: Any) -> AssertionResult:
        """
        Compare row counts from staged and reference payloads.

        Args:
            staged:    dict containing self._row_count_key → int
            reference: dict containing self._row_count_key → int

        Returns:
            AssertionResult PASSED or FAILED with full evidence.
        """
        extraction_error = self._extract_counts(staged, reference)
        if isinstance(extraction_error, AssertionResult):
            return extraction_error

        staged_count, reference_count = extraction_error

        # Zero-row guard — catches empty loads before threshold check
        if staged_count == 0 and not self._allow_empty:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    "Staged row count is 0. Empty loads are not permitted "
                    "(allow_empty=False). If this is intentional, set "
                    "allow_empty=True explicitly."
                ),
                evidence={
                    "staged_count": 0,
                    "reference_count": reference_count,
                    "allow_empty": False,
                },
            )

        absolute_delta = abs(staged_count - reference_count)

        if self._max_absolute_delta is not None:
            return self._check_absolute(staged_count, reference_count, absolute_delta)

        return self._check_percentage(staged_count, reference_count, absolute_delta)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_counts(self, staged: Any, reference: Any) -> tuple[int, int] | AssertionResult:
        """
        Extract integer row counts from both payloads.
        Returns (staged_count, reference_count) or a FAILED AssertionResult.
        """
        key = self._row_count_key

        if not isinstance(staged, dict) or key not in staged:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"staged payload missing required key '{key}'. "
                    f"Got keys: {list(staged.keys()) if isinstance(staged, dict) else type(staged).__name__}"
                ),
                evidence={"missing_key": key, "source": "staged"},
            )

        if not isinstance(reference, dict) or key not in reference:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"reference payload missing required key '{key}'. "
                    f"Got keys: {list(reference.keys()) if isinstance(reference, dict) else type(reference).__name__}"
                ),
                evidence={"missing_key": key, "source": "reference"},
            )

        staged_val = staged[key]
        ref_val = reference[key]

        if not isinstance(staged_val, int) or not isinstance(ref_val, int):
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"'{key}' must be int in both payloads. "
                    f"staged={type(staged_val).__name__}, reference={type(ref_val).__name__}"
                ),
                evidence={
                    "staged_type": type(staged_val).__name__,
                    "reference_type": type(ref_val).__name__,
                },
            )

        return staged_val, ref_val

    def _check_absolute(
        self, staged_count: int, reference_count: int, absolute_delta: int
    ) -> AssertionResult:
        threshold = self._max_absolute_delta
        if absolute_delta <= threshold:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.PASSED,
                severity=self.severity,
                message=(
                    f"Row count delta {absolute_delta} is within absolute threshold {threshold}."
                ),
                evidence={
                    "staged_count": staged_count,
                    "reference_count": reference_count,
                    "absolute_delta": absolute_delta,
                    "threshold": threshold,
                    "mode": "absolute",
                },
            )
        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.FAILED,
            severity=self.severity,
            message=(
                f"Row count delta {absolute_delta} exceeds absolute "
                f"threshold {threshold}. "
                f"staged={staged_count}, reference={reference_count}."
            ),
            evidence={
                "staged_count": staged_count,
                "reference_count": reference_count,
                "absolute_delta": absolute_delta,
                "threshold": threshold,
                "mode": "absolute",
            },
        )

    def _check_percentage(
        self, staged_count: int, reference_count: int, absolute_delta: int
    ) -> AssertionResult:
        threshold = self._max_pct_delta

        # Guard against zero reference (avoid division by zero)
        if reference_count == 0:
            if staged_count == 0:
                return AssertionResult(
                    assertion_name=self.name,
                    status=AssertionStatus.PASSED,
                    severity=self.severity,
                    message="Both staged and reference row counts are 0.",
                    evidence={
                        "staged_count": 0,
                        "reference_count": 0,
                        "mode": "percentage",
                    },
                )
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"reference row count is 0 but staged count is {staged_count}. "
                    "Percentage delta is undefined; treating as failure."
                ),
                evidence={
                    "staged_count": staged_count,
                    "reference_count": 0,
                    "mode": "percentage",
                },
            )

        pct_delta = (absolute_delta / reference_count) * 100.0
        pct_delta_rounded = round(pct_delta, 4)

        if pct_delta <= threshold:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.PASSED,
                severity=self.severity,
                message=(
                    f"Row count percentage delta {pct_delta_rounded}% is within "
                    f"threshold {threshold}%."
                ),
                evidence={
                    "staged_count": staged_count,
                    "reference_count": reference_count,
                    "absolute_delta": absolute_delta,
                    "pct_delta": pct_delta_rounded,
                    "threshold_pct": threshold,
                    "mode": "percentage",
                },
            )

        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.FAILED,
            severity=self.severity,
            message=(
                f"Row count percentage delta {pct_delta_rounded}% exceeds "
                f"threshold {threshold}%. "
                f"staged={staged_count}, reference={reference_count}."
            ),
            evidence={
                "staged_count": staged_count,
                "reference_count": reference_count,
                "absolute_delta": absolute_delta,
                "pct_delta": pct_delta_rounded,
                "threshold_pct": threshold,
                "mode": "percentage",
            },
        )
