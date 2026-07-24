-- Migration 001: staging + published schemas
--
-- Design note: staging and published are physically separate schemas,
-- not a status column on one table. A status-column design lets a
-- careless query (`SELECT * FROM data`) read unpublished rows by
-- accident. Separate schemas make that a permission/grant error
-- instead of a silent correctness bug.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS published;

-- Generic staged-record store. Concrete pipelines land arbitrary JSON
-- payloads here; the framework does not assume a fixed business
-- schema (fx_rates vs. well_production_daily have different shapes).
CREATE TABLE IF NOT EXISTS staging.records (
    staging_ref   TEXT PRIMARY KEY,
    dataset_key   TEXT NOT NULL,
    run_id        UUID NOT NULL,
    payload       JSONB NOT NULL,
    written_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_staging_records_dataset_key
    ON staging.records (dataset_key, written_at DESC);

-- Published mirror. A row only ever gets here via Publisher.promote(),
-- never via direct insert from a pipeline.
CREATE TABLE IF NOT EXISTS published.records (
    published_ref TEXT PRIMARY KEY,
    dataset_key   TEXT NOT NULL,
    run_id        UUID NOT NULL,
    staging_ref   TEXT NOT NULL,
    payload       JSONB NOT NULL,
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_published_records_dataset_key
    ON published.records (dataset_key, published_at DESC);

-- Used by SnapshotReferenceSource: "the reference for today's run is
-- whatever was published last for this dataset_key". This is what
-- makes the previous-snapshot reference source work without any
-- external system.
CREATE OR REPLACE VIEW published.latest_by_dataset AS
SELECT DISTINCT ON (dataset_key)
    dataset_key, published_ref, payload, published_at
FROM published.records
ORDER BY dataset_key, published_at DESC;