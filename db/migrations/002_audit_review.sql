-- Migration 002: audit log + review queue
--
-- wap_audit_log is the durable evidence trail the article argues for:
-- every run's assertion results, regardless of outcome, are recorded
-- here BEFORE any publish decision is finalized. It is append-only —
-- no UPDATE path is provided in the ORM layer, and this table has no
-- business reason to ever be updated.
--
-- This migration is idempotent (IF NOT EXISTS throughout).

CREATE TABLE IF NOT EXISTS wap_audit_log (
    id            BIGSERIAL   PRIMARY KEY,
    run_id        UUID        NOT NULL,
    dataset_key   TEXT        NOT NULL,
    status        TEXT        NOT NULL,      -- RunStatus value
    report        JSONB       NOT NULL,      -- serialized ReconciliationReport
    staging_ref   TEXT        NOT NULL,
    published_ref TEXT,                      -- NULL when not published
    error         TEXT,                      -- populated on ERRORED runs
    started_at    TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Primary access pattern: "show me all runs for this dataset, newest first"
CREATE INDEX IF NOT EXISTS idx_audit_log_dataset_key
    ON wap_audit_log (dataset_key, recorded_at DESC);

-- Cross-reference pattern: "find the audit entry for run X"
CREATE INDEX IF NOT EXISTS idx_audit_log_run_id
    ON wap_audit_log (run_id);

-- Status filter: "show me all errored runs across all datasets"
CREATE INDEX IF NOT EXISTS idx_audit_log_status
    ON wap_audit_log (status, recorded_at DESC);

-- Review queue: only failed/errored runs land here. A human (or a
-- downstream triage tool) resolves an entry by setting resolved_at +
-- resolution. The framework never auto-clears these — resolution is
-- always a human (or explicitly delegated automated) decision.
--
-- resolution values (open set — callers may extend):
--   'approved_publish'  — human reviewed, approved promoting staging
--   'rejected'          — data confirmed bad, pipeline re-run required
--   'false_positive'    — assertion fired incorrectly, suppressed
CREATE TABLE IF NOT EXISTS wap_review_queue (
    id             BIGSERIAL   PRIMARY KEY,
    run_id         UUID        NOT NULL UNIQUE,
    dataset_key    TEXT        NOT NULL,
    severity       TEXT        NOT NULL,     -- highest failed Severity value
    report         JSONB       NOT NULL,
    staging_ref    TEXT        NOT NULL,
    queued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ,
    resolution     TEXT,
    resolved_by    TEXT
);

-- Primary triage pattern: "show me all unresolved items for this dataset"
CREATE INDEX IF NOT EXISTS idx_review_queue_unresolved
    ON wap_review_queue (dataset_key, queued_at)
    WHERE resolved_at IS NULL;

-- Severity-based triage: "show me all unresolved CRITICAL items"
CREATE INDEX IF NOT EXISTS idx_review_queue_severity_unresolved
    ON wap_review_queue (severity, queued_at)
    WHERE resolved_at IS NULL;