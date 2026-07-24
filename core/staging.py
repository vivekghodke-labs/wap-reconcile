"""
StagingWriter contract.

The WAP pattern's core guarantee is: data written here is never
trusted by consumers until Publisher.promote() succeeds. Any
implementation of this interface must physically isolate staged data
from published data (e.g. a separate schema/table/prefix) — the
isolation must be structural, not a naming convention a query can
accidentally bypass.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StagingWriteError(Exception):
    """Raised when a write to the staging area fails."""


class StagingWriter(ABC):
    """
    Base class for all pluggable staging writers.

    `key` identifies the logical dataset (matches the `key` used by
    ReferenceSource.resolve for the same dataset). `data` is opaque to
    this interface — a concrete writer declares what shape it accepts.
    """

    @abstractmethod
    def write(self, key: str, data: Any) -> str:
        """
        Write `data` to the staging area under `key`.

        Returns a `staging_ref` — an opaque, stable pointer (e.g. a
        table name + run_id, or a URI) that downstream steps
        (assertions, publisher) use to read back exactly what was
        staged. Must raise StagingWriteError on failure, never return
        a partial/empty ref.
        """
        raise NotImplementedError

    @abstractmethod
    def read(self, staging_ref: str) -> Any:
        """
        Read back data previously written, by its staging_ref.

        Must return the data in the same shape it was written in —
        assertions and the publisher both depend on this round-trip
        fidelity.
        """
        raise NotImplementedError