"""
FxRateReconciliation — assertion that directly codifies the article's bug.

The failure scenario (verbatim from the article):
    "An upstream job re-ran and re-stamped a last_updated column without
    actually refreshing the underlying values. Every downstream consumer
    trusted a timestamp that was lying by omission."

This assertion catches that by comparing staged FX rate values
field-by-field against an independent reference source (prior published
snapshot, upstream API response, or ledger table). A re-stamped
last_updated with stale values will produce a FAILED result with
a precise evidence dict naming every mismatched pair.

Design decisions:
- Tolerance is configurable (default 0.0001, ~1 pip for FX). Callers in
  different domains (oil pricing, equity rates) override to their
  business-defined acceptable drift.
- Keys present in reference but absent in staged → FAILED (missing rates).
- Keys present in staged but absent in reference → FAILED (unexpected
  rates — could indicate a currency substitution or schema drift).
- Both `staged` and `reference` must be flat dicts of {str: float|int}.
  Nested payloads must be unwrapped by the caller before passing.
  This keeps the assertion logic single-responsibility.
- Evidence dict contains every mismatched pair so a human reviewer
  can understand the failure without re-running anything.

Oil & Gas note: The same pattern applies to commodity pricing feeds
(WTI/Brent spot, Henry Hub gas price), royalty rate tables, and
well production unit conversions. Swap the dataset_key; the assertion
logic is identical.
"""

from __future__ import annotations

from typing import Any

from core.assertions import Assertion
from core.enums import AssertionStatus, Severity
from core.models import AssertionResult


class FxRateReconciliation(Assertion):
    """
    Compares staged FX rates against a reference source, field-by-field.

    Catches:
    - Stale values behind a fresh timestamp (the article's exact scenario)
    - Missing currency pairs (partial load)
    - Extra currency pairs (unexpected injection / currency substitution)
    - Rate drift beyond tolerance (precision loss, unit swap)

    Args:
        tolerance: Maximum absolute difference allowed per rate pair.
                   Default: 0.0001 (~1 pip). Set to 0.0 for exact match.
        required_keys: If provided, these keys MUST be present in staged
                       and reference. Absence raises FAILED regardless of
                       tolerance. Use for currencies that are non-negotiable
                       for downstream consumers (e.g. USD_GBP for a UK
                       financial pipeline).

    Usage:
        assertion = FxRateReconciliation(tolerance=0.0001, required_keys=["USD_GBP"])
        result = assertion.check(staged={"USD_GBP": 0.79}, reference={"USD_GBP": 0.79})
    """

    name: str = "fx_rate_reconciliation"
    severity: Severity = Severity.CRITICAL  # FX mismatch is never a WARN

    def __init__(
        self,
        tolerance: float = 0.0001,
        required_keys: list[str] | None = None,
    ) -> None:
        if tolerance < 0:
            raise ValueError(f"tolerance must be >= 0, got {tolerance}")
        self._tolerance = tolerance
        self._required_keys: frozenset[str] = frozenset(required_keys or [])

    # ------------------------------------------------------------------
    # Assertion interface
    # ------------------------------------------------------------------

    def check(self, staged: Any, reference: Any) -> AssertionResult:
        """
        Execute field-by-field rate reconciliation.

        Args:
            staged:    dict[str, float] — payload from StagingWriter.read()
            reference: dict[str, float] — payload from ReferenceSource.resolve()

        Returns:
            AssertionResult with status PASSED or FAILED.
            On FAILED: evidence contains all mismatched, missing, and
            unexpected pairs so a reviewer has complete information.
        """
        input_error = self._validate_inputs(staged, reference)
        if input_error:
            return input_error

        mismatches: dict[str, dict] = {}

        # 1. Define the keys first
        staged_keys = set(staged.keys())
        reference_keys = set(reference.keys())

        # 2. Build the lists directly from the sets (this replaces the empty lists and the loops)
        missing_in_staged: list[str] = list(reference_keys - staged_keys)
        extra_in_staged: list[str] = list(staged_keys - reference_keys)

        # Keys present in both — check value drift
        for key in staged_keys & reference_keys:
            s_val = staged[key]
            r_val = reference[key]

            if not isinstance(s_val, (int, float)) or not isinstance(
                r_val, (int, float)
            ):
                mismatches[key] = {
                    "staged": s_val,
                    "reference": r_val,
                    "reason": "non-numeric value",
                }
                continue

            delta = abs(float(s_val) - float(r_val))
            if delta > self._tolerance:
                mismatches[key] = {
                    "staged": float(s_val),
                    "reference": float(r_val),
                    "delta": round(delta, 10),
                    "tolerance": self._tolerance,
                }

        # Required keys missing from staged — escalate regardless
        required_missing = [k for k in self._required_keys if k not in staged_keys]

        has_failures = bool(mismatches or missing_in_staged or extra_in_staged)

        if not has_failures:
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.PASSED,
                severity=self.severity,
                message=(
                    f"All {len(staged_keys)} rate pairs reconcile within "
                    f"tolerance={self._tolerance}."
                ),
                evidence={
                    "pairs_checked": len(staged_keys),
                    "tolerance": self._tolerance,
                },
            )

        # Build a human-readable, reviewer-actionable failure message
        parts: list[str] = []
        if mismatches:
            parts.append(
                f"{len(mismatches)} pair(s) exceed tolerance={self._tolerance}"
            )
        if missing_in_staged:
            parts.append(f"{len(missing_in_staged)} pair(s) missing from staged")
        if extra_in_staged:
            parts.append(f"{len(extra_in_staged)} unexpected pair(s) in staged")
        if required_missing:
            parts.append(f"required keys absent: {required_missing}")

        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.FAILED,
            severity=self.severity,
            message="; ".join(parts),
            evidence={
                "mismatches": mismatches,
                "missing_in_staged": missing_in_staged,
                "extra_in_staged": extra_in_staged,
                "required_missing": required_missing,
                "tolerance": self._tolerance,
                "pairs_checked": len(staged_keys & reference_keys),
            },
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_inputs(self, staged: Any, reference: Any) -> AssertionResult | None:
        """
        Return a FAILED result if inputs are not flat dicts, None otherwise.

        Exceptions are reserved for genuinely unexpected errors per the
        Assertion contract. Wrong input shape is an expected failure class.
        """
        if not isinstance(staged, dict):
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"staged payload must be a dict, got {type(staged).__name__}. "
                    "Unwrap nested payloads before passing to this assertion."
                ),
                evidence={"staged_type": type(staged).__name__},
            )
        if not isinstance(reference, dict):
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.FAILED,
                severity=self.severity,
                message=(
                    f"reference payload must be a dict, got {type(reference).__name__}."
                ),
                evidence={"reference_type": type(reference).__name__},
            )
        return None
