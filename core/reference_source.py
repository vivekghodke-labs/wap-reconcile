"""
ReferenceSource contract.

This is the abstraction for "an independent source of truth" that the
article insists verification must be checked against — a prior
snapshot, an upstream system-of-record, or a downstream ledger. The
framework core must never assume which one a user has; that decision
belongs to the concrete implementation supplied at pipeline construction.

Concrete implementations (SnapshotReferenceSource, ApiReferenceSource,
LedgerReferenceSource, ...) live outside core/ so the core package has
zero dependency on any specific database or transport.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ReferenceResolutionError(Exception):
    """
    Raised when a reference source cannot produce data for a given key
    (e.g. no prior snapshot exists, upstream API unreachable).

    This is deliberately a distinct exception type — the pipeline must
    be able to tell "reference unavailable" apart from "assertion
    failed" and route them differently (ERRORED vs. ROUTED_TO_REVIEW).
    """


class ReferenceSource(ABC):
    """
    Base class for all pluggable reference sources.

    `key` identifies which logical dataset/table/entity is being
    reconciled (e.g. "fx_rates", "well_production_daily"). Its meaning
    is defined by the concrete implementation, not by this interface.
    """

    @abstractmethod
    def resolve(self, key: str) -> Any:
        """
        Return the independent reference data for `key`.

        Must raise ReferenceResolutionError (not return None or an
        empty result silently) if the reference cannot be produced.
        A silent empty reference is how meaning-preserving bugs slip
        through assertions that don't defensively check for it.
        """
        raise NotImplementedError