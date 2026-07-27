"""
WAPReconciliationOperator — thin Airflow operator wrapping
ReconciliationPipeline.

This is the ONLY file in the repository permitted to import `airflow`.
core/ has zero orchestrator dependency by design (WAP Reconciliation
Framework decisions, point 6) — this adapter is what makes the
framework "pluggable into real pipelines" without core/ ever knowing
Airflow exists. Hardcoding Airflow into core/ would violate that
no-lock-in requirement, and would make the framework untestable
without a scheduler.

Design:
- `dataset_key`, `data`, `assertions`, `reference_source` may each be a
  literal value OR a callable(context) -> value. This lets a DAG author
  pull `data` from an upstream XCom (`lambda ctx: ctx["ti"].xcom_pull(...)`)
  without this operator, or core/, needing to import XCom machinery.
- staging_writer_factory defaults to a lazily-constructed Postgres-backed
  factory — `psycopg2`/`backends.postgres` is only imported if the DAG
  author doesn't inject a different backend's StagingWriter.
- Failure semantics are explicit, never implicit:
    RunStatus.ERRORED           -> always raises WAPReconciliationError.
                                    A genuine pipeline fault — the task
                                    fails, Airflow retries/alerting fire.
    RunStatus.ROUTED_TO_REVIEW  -> raises WAPReviewRequiredException by
                                    default (fail_on_review_route=True).
                                    Failed assertions mean nothing was
                                    published; a downstream task must
                                    not be able to silently consume
                                    unpublished data. Set
                                    fail_on_review_route=False for DAGs
                                    that want a soft path: the task
                                    succeeds and downstream logic
                                    branches on the pushed XCom `status`.
    RunStatus.PUBLISHED         -> task succeeds.
- Every outcome — including failures, BEFORE any exception is raised —
  pushes run_id/status/staging_ref/published_ref/error to XCom, so the
  Airflow UI and logs always carry the run's evidence. A task that "just
  failed" with no context is exactly the silent-failure mode this
  framework exists to prevent.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from core.enums import RunStatus
from core.models import RunResult
from core.pipeline import ReconciliationPipeline
from core.staging import StagingWriter

# An operator argument may be supplied directly, or as a callable
# resolved against the Airflow task context at execute() time. This is
# a documented alias, not an enforced type — see _resolve().
Resolvable = Any


class WAPReconciliationError(AirflowException):
    """
    Raised when a reconciliation run terminates as RunStatus.ERRORED.

    Distinct from WAPReviewRequiredException so DAG authors can catch
    and alert on them separately — a pipeline fault (staging/reference
    unavailable, publish gate malfunction) is a different operational
    concern than a business-rule review requirement.
    """


class WAPReviewRequiredException(AirflowException):
    """
    Raised when a run is RunStatus.ROUTED_TO_REVIEW and
    fail_on_review_route=True (the default).

    A run that failed its assertions has NOT been published. This
    exception makes that unpublishable state a hard task failure by
    default, so nothing downstream can accidentally treat unreviewed
    data as trustworthy.
    """


def _resolve(value: Resolvable, context: Any) -> Any:
    """
    Resolve an operator argument that may be a literal or a callable.

    Callables receive the Airflow task context dict, the same context
    passed to `execute()`. This is how dataset payloads sourced from
    upstream XComs, or assertions/reference sources chosen dynamically,
    reach the pipeline without this operator needing to know anything
    about XCom, task instances, or Jinja templating.
    """
    if callable(value):
        return value(context)
    return value


def _default_staging_writer_factory() -> Callable[[str], StagingWriter]:
    """
    Lazily build the default Postgres-backed staging writer factory.

    Imported inside the function, not at module top, so that DAGs
    supplying their own StagingWriter never require psycopg2 to be
    installed in the Airflow worker environment just to import this
    operator.
    """
    from backends.postgres.staging_writer import PostgresStagingWriter

    return lambda run_id: PostgresStagingWriter(run_id=run_id)


class WAPReconciliationOperator(BaseOperator):
    """
    Airflow operator that executes one WAP reconciliation run per task
    instance.

    Args:
        dataset_key:            Logical dataset name, or
                                 callable(context) -> str.
        data:                   Payload to stage, or callable(context) ->
                                 Any, e.g.
                                 `lambda ctx: ctx["ti"].xcom_pull(task_ids="extract")`.
        assertions:             Sequence[Assertion], or
                                 callable(context) -> Sequence[Assertion].
        reference_source:       ReferenceSource instance, or
                                 callable(context) -> ReferenceSource.
        staging_writer_factory: Callable[[str], StagingWriter]. Defaults
                                 to a lazily-constructed Postgres-backed
                                 factory.
        publisher:              Optional Publisher override (injectable
                                 for tests / alternate backends).
        review_router:          Optional ReviewQueueRouter override.
        fail_on_review_route:   If True (default), ROUTED_TO_REVIEW
                                 raises WAPReviewRequiredException — the
                                 task fails. If False, the task succeeds
                                 and the run's status/evidence are
                                 pushed to XCom for downstream branching.

    XCom pushed on every outcome (success or failure), keyed under this
    task's task_id: run_id, status, staging_ref, published_ref, error.

    Usage:
        WAPReconciliationOperator(
            task_id="reconcile_fx_rates",
            dataset_key="fx_rates",
            data=lambda ctx: ctx["ti"].xcom_pull(task_ids="extract_fx_rates"),
            assertions=[FxRateReconciliation(tolerance=0.0001)],
            reference_source=SnapshotReferenceSource(),
        )
    """

    # Dynamic values are resolved via callables (see _resolve), not
    # Jinja templating — no Jinja-templatable fields on this operator.
    template_fields: tuple[str, ...] = ()

    def __init__(
        self,
        *,
        dataset_key: Resolvable,
        data: Resolvable,
        assertions: Resolvable,
        reference_source: Resolvable,
        staging_writer_factory: Callable[[str], StagingWriter] | None = None,
        publisher: Any | None = None,
        review_router: Any | None = None,
        fail_on_review_route: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._dataset_key = dataset_key
        self._data = data
        self._assertions = assertions
        self._reference_source = reference_source
        self._staging_writer_factory = staging_writer_factory
        self._publisher = publisher
        self._review_router = review_router
        self._fail_on_review_route = fail_on_review_route

    # ------------------------------------------------------------------
    # Airflow interface
    # ------------------------------------------------------------------

    def execute(self, context: "Context") -> dict:
        """
        Resolve arguments, run the pipeline once, push evidence to XCom,
        and translate the terminal RunStatus into Airflow task
        success/failure per the class docstring's failure semantics.

        Returns:
            A plain, XCom-serializable dict summarizing the run
            (auto-pushed under key "return_value" by Airflow itself).
        """
        dataset_key = _resolve(self._dataset_key, context)
        data = _resolve(self._data, context)
        assertions = _resolve(self._assertions, context)
        reference_source = _resolve(self._reference_source, context)

        factory = self._staging_writer_factory or _default_staging_writer_factory()

        pipeline_kwargs: dict[str, Any] = {"staging_writer_factory": factory}
        if self._publisher is not None:
            pipeline_kwargs["publisher"] = self._publisher
        if self._review_router is not None:
            pipeline_kwargs["review_router"] = self._review_router

        pipeline = ReconciliationPipeline(**pipeline_kwargs)

        result = pipeline.run(
            dataset_key=dataset_key,
            data=data,
            assertions=assertions,
            reference_source=reference_source,
        )

        self._push_evidence(context, result)

        if result.status is RunStatus.ERRORED:
            raise WAPReconciliationError(
                f"Reconciliation run {result.run_id} for dataset_key="
                f"'{dataset_key}' ERRORED: {result.error}"
            )

        if result.status is RunStatus.ROUTED_TO_REVIEW and self._fail_on_review_route:
            failed_names = [r.assertion_name for r in result.report.failed_results]
            raise WAPReviewRequiredException(
                f"Reconciliation run {result.run_id} for dataset_key="
                f"'{dataset_key}' failed assertion(s) {failed_names} and was "
                "routed to human review — publish was refused. Set "
                "fail_on_review_route=False to allow the task to succeed "
                "and branch on status instead."
            )

        return self._to_summary(result)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_summary(result: RunResult) -> dict:
        return {
            "run_id": result.run_id,
            "status": result.status.value,
            "staging_ref": result.staging_ref,
            "published_ref": result.published_ref,
            "error": result.error,
        }

    def _push_evidence(self, context: Any, result: RunResult) -> None:
        """
        Push run evidence to XCom regardless of outcome, BEFORE any
        exception is raised. A task that fails must still leave a
        traceable run_id/status in the Airflow UI — failure with no
        evidence trail is the exact silent-failure mode this framework
        exists to prevent.
        """
        ti = context.get("ti") or context.get("task_instance")
        if ti is None:
            # No task instance available (e.g. a bare unit-test context).
            # Evidence is still available via execute()'s return value.
            return
        for key, value in self._to_summary(result).items():
            ti.xcom_push(key=key, value=value)
