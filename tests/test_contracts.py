"""
Day 1 tests: confirm the three pluggable interfaces (Assertion,
ReferenceSource, StagingWriter) are true ABCs — instantiating them
directly must fail. This locks the contract-only scope of Day 1 in
place; concrete implementations arrive in later days.
"""

import pytest

from core.assertions import Assertion
from core.reference_source import ReferenceSource
from core.staging import StagingWriter


def test_assertion_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        Assertion()  # type: ignore[abstract]


def test_reference_source_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        ReferenceSource()  # type: ignore[abstract]


def test_staging_writer_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        StagingWriter()  # type: ignore[abstract]


def test_assertion_subclass_must_implement_check():
    class IncompleteAssertion(Assertion):
        pass

    with pytest.raises(TypeError):
        IncompleteAssertion()  # type: ignore[abstract]


def test_minimal_concrete_assertion_is_instantiable():
    from core.enums import AssertionStatus, Severity
    from core.models import AssertionResult

    class MinimalAssertion(Assertion):
        name = "minimal"
        severity = Severity.INFO

        def check(self, staged, reference):
            return AssertionResult(
                assertion_name=self.name,
                status=AssertionStatus.PASSED,
                severity=self.severity,
                message="ok",
            )

    a = MinimalAssertion()
    result = a.check(staged=None, reference=None)
    assert result.passed is True