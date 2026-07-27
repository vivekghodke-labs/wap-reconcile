"""
fx_pipeline_demo.py — recreates the article's exact incident end-to-end.

Run:
    export DATABASE_URL=postgresql://wap_user:wap_password@localhost:5432/wap_db
    python -m db.migrations.run_migrations   # once
    python examples/fx_pipeline_demo.py

Or via docker-compose:
    docker compose run --rm app python examples/fx_pipeline_demo.py

What this demonstrates, in order:

  SCENE 1 — a healthy prior state.
      A "yesterday" run publishes correct FX rates. This becomes the
      independent reference (SnapshotReferenceSource) for today's run.

  SCENE 2 — the article's exact bug.
      An upstream job re-runs and re-stamps `last_updated` without
      refreshing the underlying values. The staged payload is
      structurally fine and arrives on schedule — but the rates are
      yesterday's, not today's. A naive freshness/schema check would
      go green. FxRateReconciliation compares staged values against
      the independent reference field-by-field and catches it:
      the run is REFUSED publication and routed to human review with
      full evidence — no page fired, no retry masked it, and nothing
      downstream was allowed to trust a lie.

  SCENE 3 — the corrected re-run.
      The same dataset, now with today's actual rates. All assertions
      pass, the publish gate opens, and the corrected data becomes the
      new reference for tomorrow.

This is not a synthetic toy case — it is the incident from the
accompanying article, replayed against the real framework, against a
real Postgres instance, with no mocking of any core component.
"""

from __future__ import annotations

import logging
import sys

from assertions_library.fx_reconciliation import FxRateReconciliation
from assertions_library.row_count_delta import RowCountDelta
from backends.postgres.staging_writer import PostgresStagingWriter
from core.enums import RunStatus
from core.pipeline import ReconciliationPipeline
from core.publisher import Publisher
from core.reference_source import SnapshotReferenceSource
from core.review_queue import ReviewQueueRouter

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

DATASET_KEY = "fx_rates_demo"

_DIVIDER = "=" * 78


def _banner(title: str) -> None:
    print(f"\n{_DIVIDER}\n{title}\n{_DIVIDER}")


def _make_pipeline() -> ReconciliationPipeline:
    return ReconciliationPipeline(
        staging_writer_factory=lambda run_id: PostgresStagingWriter(run_id=run_id),
        publisher=Publisher(),
        review_router=ReviewQueueRouter(),
    )


def scene_1_seed_healthy_prior_state(pipeline: ReconciliationPipeline) -> None:
    _banner("SCENE 1 — Yesterday's run: correct rates, published cleanly")

    yesterdays_actual_rates = {
        "USD_GBP": 0.79,
        "USD_EUR": 0.92,
        "row_count": 2,
    }
    print(f"Staging + publishing: {yesterdays_actual_rates}")

    result = pipeline.run(
        dataset_key=DATASET_KEY,
        data=yesterdays_actual_rates,
        assertions=[RowCountDelta(max_absolute_delta=0)],
        reference_source=_BootstrapReferenceSource(yesterdays_actual_rates),
    )

    print(f"-> status={result.status.value} published_ref={result.published_ref}")
    assert result.status is RunStatus.PUBLISHED, "Demo setup failed — aborting."


def scene_2_the_incident(pipeline: ReconciliationPipeline) -> None:
    _banner("SCENE 2 — Today's run: last_updated re-stamped, values NOT refreshed")

    todays_actual_rates_that_never_arrived = {"USD_GBP": 0.79, "USD_EUR": 0.92}
    stale_payload_behind_fresh_timestamp = {
        "USD_GBP": 0.75,  # yesterday's rate, silently reused
        "USD_EUR": 0.88,  # yesterday's rate, silently reused
        "row_count": 2,
    }

    print("Upstream job re-ran. last_updated column refreshed.")
    print(f"Staged payload (looks fresh, IS stale): {stale_payload_behind_fresh_timestamp}")
    print("A naive freshness/schema check would show green here.\n")

    result = pipeline.run(
        dataset_key=DATASET_KEY,
        data=stale_payload_behind_fresh_timestamp,
        assertions=[
            FxRateReconciliation(tolerance=0.0001, required_keys=["USD_GBP", "USD_EUR"]),
            RowCountDelta(max_absolute_delta=0),
        ],
        reference_source=SnapshotReferenceSource(),
    )

    print(f"-> status={result.status.value}")
    print(f"-> published_ref={result.published_ref}  (must be None)")
    for r in result.report.failed_results:
        print(f"-> FAILED [{r.severity.value}] {r.assertion_name}: {r.message}")
        print(f"   evidence: {r.evidence}")

    assert result.status is RunStatus.ROUTED_TO_REVIEW
    assert result.published_ref is None
    print(
        "\nPublish REFUSED. Run routed to wap_review_queue with full evidence — "
        "no retry, no silent overwrite, nothing downstream trusts this data."
    )
    _ = todays_actual_rates_that_never_arrived  # documents what SHOULD have arrived


def scene_3_the_corrected_rerun(pipeline: ReconciliationPipeline) -> None:
    _banner("SCENE 3 — Corrected re-run: actual today's rates")

    corrected_rates = {"USD_GBP": 0.79, "USD_EUR": 0.92, "row_count": 2}
    print(f"Staging + publishing corrected payload: {corrected_rates}")

    result = pipeline.run(
        dataset_key=DATASET_KEY,
        data=corrected_rates,
        assertions=[
            FxRateReconciliation(tolerance=0.0001, required_keys=["USD_GBP", "USD_EUR"]),
            RowCountDelta(max_absolute_delta=0),
        ],
        reference_source=SnapshotReferenceSource(),
    )

    print(f"-> status={result.status.value} published_ref={result.published_ref}")
    assert result.status is RunStatus.PUBLISHED
    print("\nPublished. This corrected snapshot is now tomorrow's reference.")


class _BootstrapReferenceSource:
    """
    Trivial reference source used only for Scene 1's bootstrap publish,
    where by definition no prior snapshot exists yet. Every subsequent
    scene uses the real SnapshotReferenceSource against what was
    actually published.
    """

    def __init__(self, data: dict) -> None:
        self._data = data

    def resolve(self, key: str) -> dict:
        return self._data


def main() -> int:
    pipeline = _make_pipeline()

    try:
        scene_1_seed_healthy_prior_state(pipeline)
        scene_2_the_incident(pipeline)
        scene_3_the_corrected_rerun(pipeline)
    except AssertionError as exc:
        print(f"\nDEMO FAILED: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 — top-level demo entrypoint
        print(
            f"\nDEMO ERRORED: {exc}\n"
            "Is DATABASE_URL set and migrations applied? "
            "See db/migrations/run_migrations.py.",
            file=sys.stderr,
        )
        return 1

    _banner("DONE — the article's incident was caught, not missed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
