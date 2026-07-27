# wap-reconcile

A Write-Audit-Publish (WAP) reconciliation framework for catching **semantic** data
drift — the kind that a schema check or freshness check cannot see.

## The problem this solves

A pipeline can be structurally healthy and still be wrong. A job re-runs, re-stamps a
`last_updated` column, but never actually refreshes the underlying values. No schema
changed. No row count dropped. The dashboard is green. Six weeks later finance
discovers the FX table has been quietly serving yesterday's rates as today's.

Retries and schema-drift alerts are not built to catch this — they verify structure
and freshness, not meaning. This framework adds the missing layer: **verification
against an independent reference**, gated behind a staging step, before anything is
trusted downstream.

## How it works — Write, Audit, Publish

```
stage data  →  run assertions against an independent reference  →  pass? publish : route to review
```

- **Staging** — data lands in `staging.*`, never trusted by consumers.
- **Audit** — pluggable `Assertion`s compare staged data field-by-field against a
  `ReferenceSource` (a prior snapshot, an upstream system, a ledger table — anything).
- **Publish** — only promoted to `published.*` if every assertion passes. Every run,
  pass or fail, writes a durable `wap_audit_log` entry. Failures route to
  `wap_review_queue` for human sign-off — never silently retried, never silently
  dropped.

```
┌──────────┐    ┌────────────┐    ┌──────────────┐    ┌───────────┐
│  Stage   │──▶ │  Reference │──▶ │  Assertions  │──▶ │  Publish  │──▶ published.*
│  writer  │    │  source    │    │  (pluggable) │    │  gate     │
└──────────┘    └────────────┘    └──────┬───────┘    └─────┬─────┘
                                         │  fail            │ pass
                                         ▼                  ▼
                                  wap_review_queue      wap_audit_log
                                  (human sign-off)      (every run, always)
```

Core has **zero orchestrator dependency** — it's a standalone Python library.
Airflow is an optional, thin adapter (`adapters/airflow_operator.py`), not a
requirement.

## Quickstart

```bash
cp .env.example .env        # fill in a Postgres password
docker compose up migrate   # start Postgres, apply schema
docker compose run --rm app python examples/fx_pipeline_demo.py
```

The demo replays the incident above end-to-end: seeds a correct prior snapshot,
stages stale rates behind a fresh run (the bug), shows the publish gate refuse it
with full evidence, then re-runs with corrected data and publishes.

```python
from core.pipeline import ReconciliationPipeline
from core.publisher import Publisher
from core.review_queue import ReviewQueueRouter
from core.reference_source import SnapshotReferenceSource
from backends.postgres.staging_writer import PostgresStagingWriter
from assertions_library.fx_reconciliation import FxRateReconciliation

pipeline = ReconciliationPipeline(
    staging_writer_factory=lambda run_id: PostgresStagingWriter(run_id=run_id),
    publisher=Publisher(),
    review_router=ReviewQueueRouter(),
)

result = pipeline.run(
    dataset_key="fx_rates",
    data={"USD_GBP": 0.79, "USD_EUR": 0.92},
    assertions=[FxRateReconciliation(tolerance=0.0001)],
    reference_source=SnapshotReferenceSource(),
)

# result.status is one of: PUBLISHED / ROUTED_TO_REVIEW / ERRORED
```

### Airflow

```python
from adapters.airflow_operator import WAPReconciliationOperator

WAPReconciliationOperator(
    task_id="reconcile_fx_rates",
    dataset_key="fx_rates",
    data=lambda ctx: ctx["ti"].xcom_pull(task_ids="extract_fx_rates"),
    assertions=[FxRateReconciliation(tolerance=0.0001)],
    reference_source=SnapshotReferenceSource(),
)
```

`ROUTED_TO_REVIEW` fails the task by default (nothing downstream can silently
consume unpublished data) — set `fail_on_review_route=False` for a soft path that
branches on the pushed XCom `status` instead.

## Project layout

```
core/                   Contracts + orchestrator — zero I/O framework dependencies
  assertions.py          Assertion ABC
  staging.py             StagingWriter ABC
  reference_source.py    ReferenceSource ABC + SnapshotReferenceSource
  publisher.py           Publish gate (staging → published, all-or-nothing)
  review_queue.py        Failed-run routing for human review
  pipeline.py            ReconciliationPipeline — ties it all together
  models.py               RunResult / AssertionResult / ReconciliationReport
assertions_library/     Pluggable assertions (FX reconciliation, row-count delta,
                        numeric distribution check — the article's exact bug is
                        assertions_library/fx_reconciliation.py)
backends/postgres/      Postgres-backed StagingWriter + connection pool
adapters/               Optional orchestrator adapters (Airflow — thin, isolated)
db/migrations/          Schema: staging.*, published.*, audit log, review queue
examples/               Runnable end-to-end demo
tests/                  pytest — unit + integration (Postgres via docker-compose)
```

## Writing your own assertion

```python
from core.assertions import Assertion
from core.enums import AssertionStatus, Severity
from core.models import AssertionResult


class MyAssertion(Assertion):
    name = "my_assertion"
    severity = Severity.CRITICAL

    def check(self, staged, reference) -> AssertionResult:
        # compare staged against reference; never mutate either
        ...
        return AssertionResult(
            assertion_name=self.name,
            status=AssertionStatus.PASSED,
            severity=self.severity,
            message="ok",
        )
```

Known limitation, by design: distribution/statistical checks
(`NumericDistributionCheck`) catch scale/shape drift but **cannot** catch a
meaning-preserving swap (two status codes trading places at similar frequency).
That residual gap is why field-level reconciliation against ground truth
(`FxRateReconciliation`-style assertions) exists as a separate, complementary tool —
see the module docstrings for the full reasoning.

## Development

```bash
pip install -r requirements.txt -r requirements-dev.txt
docker compose up migrate            # apply schema to local Postgres
pytest tests/ -v                     # core suite
# optional Airflow adapter tests:
pip install -r requirements-airflow.txt
pytest tests/test_airflow_operator.py -v
```

`ruff check .` and `mypy core/ backends/ adapters/` run in CI (see
`.github/workflows/ci.yml`) — lint → type-check → test → build → publish-on-tag.

## License

Apache-2.0