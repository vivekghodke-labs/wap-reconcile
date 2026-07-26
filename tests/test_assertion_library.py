"""
Day 4 unit tests: assertion library.

Pure unit tests — no DB, no I/O. Each assertion has:
  - A PASSING fixture: proves the assertion does not over-fire on valid data.
  - A DELIBERATELY BROKEN fixture: proves the assertion catches its claimed
    failure class. A test that never fails doesn't prove the assertion works.

Test philosophy (enforced by the Day 4 plan):
    "Each assertion: unit tests with both a passing and a deliberately-broken
    fixture (to prove it actually catches the failure class it claims to)"

The broken fixtures are designed from the article's failure taxonomy:
  - FxRateReconciliation: stale values behind a fresh timestamp
  - RowCountDelta: silent truncation, empty load
  - NumericDistributionCheck: scale error (unit swap), mean drift

No mocking of core contracts — assertions are pure functions (staged, reference)
→ AssertionResult. There is nothing to mock.
"""

from __future__ import annotations

import pytest

from assertions_library.distribution_check import NumericDistributionCheck
from assertions_library.fx_reconciliation import FxRateReconciliation
from assertions_library.row_count_delta import RowCountDelta
from core.enums import AssertionStatus, Severity

# =============================================================================
# FxRateReconciliation
# =============================================================================


class TestFxRateReconciliationPassPath:
    """All rates reconcile within tolerance — assertion must PASS."""

    def test_identical_rates_pass(self):
        a = FxRateReconciliation(tolerance=0.0001)
        result = a.check(
            staged={"USD_GBP": 0.79, "USD_EUR": 0.92},
            reference={"USD_GBP": 0.79, "USD_EUR": 0.92},
        )
        assert result.passed
        assert result.status is AssertionStatus.PASSED

    def test_rates_within_tolerance_pass(self):
        """Drift of exactly the tolerance boundary must still pass."""
        a = FxRateReconciliation(tolerance=0.0001)
        result = a.check(
            staged={"USD_GBP": 0.7901},  # delta = 0.0001 exactly
            reference={"USD_GBP": 0.7900},
        )
        assert result.passed

    def test_evidence_contains_pairs_checked(self):
        a = FxRateReconciliation()
        result = a.check(
            staged={"USD_GBP": 0.79, "USD_EUR": 0.92, "USD_JPY": 145.0},
            reference={"USD_GBP": 0.79, "USD_EUR": 0.92, "USD_JPY": 145.0},
        )
        assert result.evidence["pairs_checked"] == 3

    def test_severity_is_critical(self):
        a = FxRateReconciliation()
        result = a.check(
            staged={"USD_GBP": 0.79},
            reference={"USD_GBP": 0.79},
        )
        assert result.severity is Severity.CRITICAL

    def test_required_keys_present_passes(self):
        a = FxRateReconciliation(required_keys=["USD_GBP"])
        result = a.check(
            staged={"USD_GBP": 0.79},
            reference={"USD_GBP": 0.79},
        )
        assert result.passed


class TestFxRateReconciliationFailPath:
    """
    Deliberately broken fixtures — each proves the assertion catches
    its claimed failure class.
    """

    def test_stale_values_behind_fresh_timestamp_are_caught(self):
        """
        THE ARTICLE'S EXACT BUG:
        Upstream job re-stamped last_updated without refreshing values.
        Staged rates are yesterday's; reference has today's.
        Assertion must FAIL with evidence naming the mismatched pairs.
        """
        a = FxRateReconciliation(tolerance=0.0001)
        result = a.check(
            staged={
                "USD_GBP": 0.75,  # yesterday's rate — stale
                "USD_EUR": 0.88,  # yesterday's rate — stale
            },
            reference={
                "USD_GBP": 0.79,  # today's actual rate
                "USD_EUR": 0.92,  # today's actual rate
            },
        )
        assert not result.passed
        assert result.status is AssertionStatus.FAILED
        assert "USD_GBP" in result.evidence["mismatches"]
        assert "USD_EUR" in result.evidence["mismatches"]
        # Reviewer can see exact delta without re-running
        assert result.evidence["mismatches"]["USD_GBP"]["delta"] == pytest.approx(
            0.04, abs=1e-9
        )

    def test_missing_currency_pair_is_caught(self):
        """Partial load: staged is missing USD_EUR entirely."""
        a = FxRateReconciliation()
        result = a.check(
            staged={"USD_GBP": 0.79},
            reference={"USD_GBP": 0.79, "USD_EUR": 0.92},
        )
        assert not result.passed
        assert "USD_EUR" in result.evidence["missing_in_staged"]

    def test_unexpected_currency_pair_is_caught(self):
        """
        Currency substitution: staged has USD_CHF which reference doesn't.
        Could indicate a currency field was silently repurposed.
        """
        a = FxRateReconciliation()
        result = a.check(
            staged={"USD_GBP": 0.79, "USD_CHF": 0.91},  # CHF is unexpected
            reference={"USD_GBP": 0.79},
        )
        assert not result.passed
        assert "USD_CHF" in result.evidence["extra_in_staged"]

    def test_required_key_missing_from_staged_fails(self):
        """USD_GBP is non-negotiable for this pipeline."""
        a = FxRateReconciliation(required_keys=["USD_GBP"])
        result = a.check(
            staged={"USD_EUR": 0.92},
            reference={"USD_GBP": 0.79, "USD_EUR": 0.92},
        )
        assert not result.passed
        assert "USD_GBP" in result.evidence["required_missing"]

    def test_non_numeric_value_in_staged_fails(self):
        """Rate was serialized as string — type corruption."""
        a = FxRateReconciliation()
        result = a.check(
            staged={"USD_GBP": "0.79"},  # string, not float
            reference={"USD_GBP": 0.79},
        )
        assert not result.passed
        assert "non-numeric" in result.evidence["mismatches"]["USD_GBP"]["reason"]

    def test_wrong_input_type_staged_fails_gracefully(self):
        """staged is a list — not a flat dict."""
        a = FxRateReconciliation()
        result = a.check(staged=[0.79, 0.92], reference={"USD_GBP": 0.79})
        assert not result.passed
        assert result.status is AssertionStatus.FAILED

    def test_failure_message_is_human_readable(self):
        """Evidence and message must allow a human reviewer to understand the failure."""
        a = FxRateReconciliation(tolerance=0.0)
        result = a.check(
            staged={"USD_GBP": 0.75},
            reference={"USD_GBP": 0.79},
        )
        assert result.message  # non-empty
        assert result.evidence  # non-empty dict
        # Message must name the failure class, not just say "failed"
        assert "tolerance" in result.message or "exceed" in result.message


class TestFxRateReconciliationConstruction:
    def test_negative_tolerance_rejected(self):
        with pytest.raises(ValueError, match="tolerance"):
            FxRateReconciliation(tolerance=-0.01)

    def test_zero_tolerance_accepted(self):
        a = FxRateReconciliation(tolerance=0.0)
        assert a is not None

    def test_name_and_severity_class_attributes(self):
        assert FxRateReconciliation.name == "fx_rate_reconciliation"
        assert FxRateReconciliation.severity is Severity.CRITICAL


# =============================================================================
# RowCountDelta
# =============================================================================


class TestRowCountDeltaPassPath:
    def test_identical_counts_pass_absolute_mode(self):
        a = RowCountDelta(max_absolute_delta=0)
        result = a.check(
            staged={"row_count": 10000},
            reference={"row_count": 10000},
        )
        assert result.passed

    def test_within_absolute_threshold_passes(self):
        a = RowCountDelta(max_absolute_delta=100)
        result = a.check(
            staged={"row_count": 9950},
            reference={"row_count": 10000},
        )
        assert result.passed
        assert result.evidence["absolute_delta"] == 50

    def test_within_pct_threshold_passes(self):
        a = RowCountDelta(max_pct_delta=5.0)
        result = a.check(
            staged={"row_count": 9600},
            reference={"row_count": 10000},
        )
        assert result.passed
        assert result.evidence["pct_delta"] == pytest.approx(4.0, abs=0.01)

    def test_evidence_contains_mode(self):
        a = RowCountDelta(max_absolute_delta=100)
        result = a.check(staged={"row_count": 100}, reference={"row_count": 100})
        assert result.evidence["mode"] == "absolute"


class TestRowCountDeltaFailPath:
    def test_silent_truncation_is_caught(self):
        """
        Classic partial load: 10k rows expected, 200 arrived.
        This is unambiguously catastrophic data loss.
        """
        a = RowCountDelta(max_pct_delta=5.0)
        result = a.check(
            staged={"row_count": 200},  # 98% drop
            reference={"row_count": 10000},
        )
        assert not result.passed
        assert result.evidence["pct_delta"] > 5.0
        assert result.status is AssertionStatus.FAILED

    def test_empty_load_is_caught_by_default(self):
        """
        Zero staged rows must always fail (allow_empty=False by default).
        An empty publish is never a valid outcome for operational data.
        """
        a = RowCountDelta(max_absolute_delta=1000)
        result = a.check(
            staged={"row_count": 0},
            reference={"row_count": 10000},
        )
        assert not result.passed
        assert "0" in result.message or "empty" in result.message.lower()

    def test_empty_load_passes_when_allow_empty_true(self):
        """allow_empty=True is an explicit override — downstream consumers must handle it."""
        a = RowCountDelta(max_absolute_delta=1000, allow_empty=True)
        result = a.check(
            staged={"row_count": 0},
            reference={"row_count": 500},
        )
        # Delta is 500, within 1000 — passes
        assert result.passed

    def test_runaway_duplication_caught(self):
        """Fan-out join bug: 10k rows became 500k."""
        a = RowCountDelta(max_pct_delta=10.0)
        result = a.check(
            staged={"row_count": 500_000},
            reference={"row_count": 10_000},
        )
        assert not result.passed

    def test_absolute_threshold_breach_caught(self):
        a = RowCountDelta(max_absolute_delta=5)
        result = a.check(
            staged={"row_count": 9990},
            reference={"row_count": 10000},
        )
        assert not result.passed
        assert result.evidence["absolute_delta"] == 10

    def test_missing_row_count_key_in_staged_fails(self):
        a = RowCountDelta(max_absolute_delta=100)
        result = a.check(staged={"records": 100}, reference={"row_count": 100})
        assert not result.passed
        assert "row_count" in result.message

    def test_missing_row_count_key_in_reference_fails(self):
        a = RowCountDelta(max_absolute_delta=100)
        result = a.check(staged={"row_count": 100}, reference={"records": 100})
        assert not result.passed

    def test_non_integer_count_fails(self):
        """row_count must be an integer, not a float serialized from JSON."""
        a = RowCountDelta(max_absolute_delta=100)
        result = a.check(
            staged={"row_count": 100.0},  # float — common JSON deserialisation issue
            reference={"row_count": 100},
        )
        assert not result.passed
        assert "int" in result.message

    def test_custom_row_count_key_is_respected(self):
        """Payloads that use a different field name for the count."""
        a = RowCountDelta(max_absolute_delta=5, row_count_key="total_records")
        result = a.check(
            staged={"total_records": 100},
            reference={"total_records": 100},
        )
        assert result.passed


class TestRowCountDeltaConstruction:
    def test_both_modes_specified_raises(self):
        with pytest.raises(ValueError, match="Exactly one"):
            RowCountDelta(max_absolute_delta=10, max_pct_delta=5.0)

    def test_neither_mode_specified_raises(self):
        with pytest.raises(ValueError, match="Exactly one"):
            RowCountDelta()

    def test_negative_absolute_delta_raises(self):
        with pytest.raises(ValueError, match=">= 0"):
            RowCountDelta(max_absolute_delta=-1)

    def test_pct_delta_over_100_raises(self):
        with pytest.raises(ValueError, match="100"):
            RowCountDelta(max_pct_delta=150.0)

    def test_name_and_severity_class_attributes(self):
        assert RowCountDelta.name == "row_count_delta"
        assert RowCountDelta.severity is Severity.WARN


# =============================================================================
# NumericDistributionCheck
# =============================================================================

# Fixtures — 20 values each so min_sample_size=10 default is met

_STABLE_REFERENCE = [100.0 + i * 0.5 for i in range(20)]  # mean≈104.75, stable

_WITHIN_TOLERANCE_STAGED = [v + 0.1 for v in _STABLE_REFERENCE]  # tiny upward shift

_MEAN_DRIFTED_STAGED = [
    v * 2.0 for v in _STABLE_REFERENCE
]  # 100% mean drift (scale error)

_UNIT_SWAP_STAGED = [v * 1000.0 for v in _STABLE_REFERENCE]  # bbl→MCF unit swap


class TestNumericDistributionCheckPassPath:
    def test_stable_distribution_passes(self):
        a = NumericDistributionCheck(field="production_bbl", max_mean_drift=0.05)
        result = a.check(
            staged={"production_bbl": _WITHIN_TOLERANCE_STAGED},
            reference={"production_bbl": _STABLE_REFERENCE},
        )
        assert result.passed
        assert result.status is AssertionStatus.PASSED

    def test_evidence_contains_statistics(self):
        a = NumericDistributionCheck()
        result = a.check(
            staged={"values": _WITHIN_TOLERANCE_STAGED},
            reference={"values": _STABLE_REFERENCE},
        )
        assert "staged_mean" in result.evidence
        assert "reference_mean" in result.evidence
        assert "staged_stddev" in result.evidence
        assert "reference_stddev" in result.evidence

    def test_severity_is_warn(self):
        """Distribution drift is a warning — it doesn't block publish by itself."""
        a = NumericDistributionCheck()
        result = a.check(
            staged={"values": _WITHIN_TOLERANCE_STAGED},
            reference={"values": _STABLE_REFERENCE},
        )
        assert result.severity is Severity.WARN

    def test_identical_distributions_pass(self):
        a = NumericDistributionCheck(max_mean_drift=0.0, max_stddev_drift=0.0)
        result = a.check(
            staged={"values": list(_STABLE_REFERENCE)},
            reference={"values": list(_STABLE_REFERENCE)},
        )
        assert result.passed

    def test_integer_values_are_accepted(self):
        """JSONB round-trips ints as ints — assertion must handle both."""
        a = NumericDistributionCheck(min_sample_size=5)
        vals = [100, 102, 98, 101, 99, 103, 97, 100, 101, 99]
        result = a.check(staged={"values": vals}, reference={"values": vals})
        assert result.passed


class TestNumericDistributionCheckFailPath:
    def test_scale_error_unit_swap_is_caught(self):
        """
        Catch: bbl → MCF unit swap causes 1000x mean inflation.
        The article: "swap a currency field's unit and the mean and variance
        shift with it, and a distribution check has a real shot at flagging it."
        """
        a = NumericDistributionCheck(field="production_bbl", max_mean_drift=0.05)
        result = a.check(
            staged={"production_bbl": _UNIT_SWAP_STAGED},  # 1000x scale
            reference={"production_bbl": _STABLE_REFERENCE},
        )
        assert not result.passed
        assert result.status is AssertionStatus.FAILED
        # Evidence must name the failing statistic
        assert result.evidence.get("mean_drift") is not None
        assert result.evidence["mean_drift"] > 0.05

    def test_mean_drift_exceeding_threshold_fails(self):
        """100% mean drift (doubled values) must fail with a 5% threshold."""
        a = NumericDistributionCheck(max_mean_drift=0.05)
        result = a.check(
            staged={"values": _MEAN_DRIFTED_STAGED},
            reference={"values": _STABLE_REFERENCE},
        )
        assert not result.passed
        assert "mean drift" in result.message

    def test_insufficient_sample_size_fails(self):
        """
        Too few staged values — distribution check is not meaningful.
        Better to surface this explicitly than silently pass on 3 data points.
        """
        a = NumericDistributionCheck(min_sample_size=10)
        result = a.check(
            staged={"values": [100.0, 101.0, 99.0]},  # only 3 values
            reference={"values": _STABLE_REFERENCE},
        )
        assert not result.passed
        assert "sample size" in result.message
        assert result.evidence["staged_sample_size"] == 3

    def test_non_numeric_values_in_staged_fails(self):
        """
        Sensor reading serialized as string — data corruption caught before math.
        """
        a = NumericDistributionCheck()
        result = a.check(
            staged={"values": [100.0, "N/A", 99.0] + _STABLE_REFERENCE},
            reference={"values": _STABLE_REFERENCE},
        )
        assert not result.passed
        assert "non-numeric" in result.message

    def test_missing_field_in_staged_fails(self):
        a = NumericDistributionCheck(field="production_bbl")
        result = a.check(
            staged={"pressure_psi": [100.0] * 20},  # wrong field
            reference={"production_bbl": _STABLE_REFERENCE},
        )
        assert not result.passed
        assert "production_bbl" in result.message

    def test_missing_field_in_reference_fails(self):
        a = NumericDistributionCheck(field="production_bbl")
        result = a.check(
            staged={"production_bbl": _STABLE_REFERENCE},
            reference={"pressure_psi": [100.0] * 20},
        )
        assert not result.passed

    def test_empty_staged_list_fails(self):
        a = NumericDistributionCheck()
        result = a.check(
            staged={"values": []},
            reference={"values": _STABLE_REFERENCE},
        )
        assert not result.passed

    def test_failure_message_documents_limitation(self):
        """
        The failure message must contain the documented limitation so
        a reviewer knows this check doesn't catch meaning-preserving swaps.
        """
        a = NumericDistributionCheck(max_mean_drift=0.05)
        result = a.check(
            staged={"values": _MEAN_DRIFTED_STAGED},
            reference={"values": _STABLE_REFERENCE},
        )
        assert not result.passed
        # The article's caveat must appear in the message
        assert (
            "limitation" in result.message.lower() or "shape" in result.message.lower()
        )


class TestNumericDistributionCheckConstruction:
    def test_negative_mean_drift_raises(self):
        with pytest.raises(ValueError, match="max_mean_drift"):
            NumericDistributionCheck(max_mean_drift=-0.1)

    def test_negative_stddev_drift_raises(self):
        with pytest.raises(ValueError, match="max_stddev_drift"):
            NumericDistributionCheck(max_stddev_drift=-0.1)

    def test_zero_min_sample_size_raises(self):
        with pytest.raises(ValueError, match="min_sample_size"):
            NumericDistributionCheck(min_sample_size=0)

    def test_name_and_severity_class_attributes(self):
        assert NumericDistributionCheck.name == "numeric_distribution_check"
        assert NumericDistributionCheck.severity is Severity.WARN


# =============================================================================
# Cross-cutting: all assertions honour the core contract
# =============================================================================


class TestAssertionContract:
    """
    Every assertion in the library must honour the Assertion ABC contract.
    These tests guard against regressions where a subclass accidentally
    violates a contract that the framework depends on.
    """

    _assertions = (
        FxRateReconciliation(),
        RowCountDelta(max_absolute_delta=100),
        NumericDistributionCheck(min_sample_size=5),
    )

    @pytest.mark.parametrize("assertion", _assertions)
    def test_check_always_returns_assertion_result(self, assertion):
        """
        check() must always return AssertionResult — never raise for
        expected failure conditions (wrong input shape, missing keys).
        """
        from core.models import AssertionResult

        result = assertion.check(staged=None, reference=None)
        assert isinstance(result, AssertionResult)

    @pytest.mark.parametrize("assertion", _assertions)
    def test_check_result_has_non_empty_message_on_failure(self, assertion):
        """Failed results must have an actionable message."""
        result = assertion.check(staged=None, reference=None)
        if not result.passed:
            assert result.message, (
                f"{assertion.name}: FAILED result has empty message — not reviewable."
            )

    @pytest.mark.parametrize("assertion", _assertions)
    def test_check_result_assertion_name_matches_class(self, assertion):
        result = assertion.check(staged=None, reference=None)
        assert result.assertion_name == assertion.name
