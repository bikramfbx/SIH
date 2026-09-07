-- Hardening migration (idempotent).
-- Adds worker heartbeat telemetry, additional query indexes, and optional
-- land-cover evidence seams. Safe on existing databases; also reflected in
-- database/init.sql for fresh setups.

-- ---------------------------------------------------------------------------
-- Worker heartbeat: one row per worker instance, updated every cycle so the
-- /api/health endpoint can report whether the scheduled pipeline is alive.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS worker_status (
    worker_id          TEXT PRIMARY KEY,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cycle_count        BIGINT NOT NULL DEFAULT 0,
    last_cycle_status  TEXT,          -- 'success' | 'failed'
    last_error         TEXT,
    last_inserted      INTEGER NOT NULL DEFAULT 0,
    version            TEXT
);

-- ---------------------------------------------------------------------------
-- Query indexes.
--   * per-source filtering (global-capable NRT query paths)
--   * composite (source, acq_date) for time-window NRT reads
--   * nearest-facility lookups start from the enrichment side
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_hotspots_source
    ON hotspots (source);
CREATE INDEX IF NOT EXISTS idx_hotspots_source_acq_date
    ON hotspots (source, acq_date DESC);
CREATE INDEX IF NOT EXISTS idx_hotspot_enrichment_facility_id
    ON hotspot_enrichment (nearest_facility_id)
    WHERE nearest_facility_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Optional land-cover evidence seam. Populated only when a land-cover provider
-- is configured (LAND_COVER=1 and a working source); otherwise stays NULL and
-- every downstream consumer (enrichment, classifier, API) treats it as
-- "no data". Never on the critical path.
-- ---------------------------------------------------------------------------
ALTER TABLE hotspot_enrichment
    ADD COLUMN IF NOT EXISTS landcover_class     TEXT,
    ADD COLUMN IF NOT EXISTS landcover_fracs     JSONB,
    ADD COLUMN IF NOT EXISTS landcover_note      TEXT;