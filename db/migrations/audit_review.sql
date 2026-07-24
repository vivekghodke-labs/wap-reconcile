-- Migration 002: audit log + review queue
--
-- wap_audit_log is the durable evidence trail the article argues for:
-- every run's assertion results, regardless of outcome, are recorded
-- here BEFORE any publish decision is finalized. It is append-only —
-- no UPDATE path is provided in the ORM layer, and this table has no
-- business reason to ever be updated.

CREATE TABLE IF NOT EXISTS wap_audit_log (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL,
    dataset_key   TEXT NOT NULL,
    status        TEXT NOT NULL,           -- RunStatus value
    report        JSONB NOT NULL,          -- serialized ReconciliationReport
    staging_ref   TEXT NOT NULL,
    published_ref TEXT,
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_dataset_key
    ON wap_audit_log (dataset_key, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_log_run_id
    ON wap_audit_log (run_id);

-- Review queue: only failed/errored runs land here. A human (or a
-- downstream triage tool) resolves an entry by setting resolved_at +
-- resolution — the framework never auto-clears these.
CREATE TABLE IF NOT EXISTS wap_review_queue (
    id             BIGSERIAL PRIMARY KEY,
    run_id         UUID NOT NULL UNIQUE,
    dataset_key    TEXT NOT NULL,
    severity       TEXT NOT NULL,          -- highest failed Severity
    report         JSONB NOT NULL,
    staging_ref    TEXT NOT NULL,
    queued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ,
    resolution     TEXT,                   -- e.g. 'approved_publish', 'rejected', 'false_positive'
    resolved_by    TEXT
);

CREATE INDEX IF NOT EXISTS idx_review_queue_unresolved
    ON wap_review_queue (dataset_key, queued_at)
    WHERE resolved_at IS NULL;