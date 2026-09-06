-- Final-backend hardening migration (idempotent).
-- Adds deep temporal features, vision-model integration seams, and the
-- ingestion/pipeline telemetry table. Safe to run on an existing database;
-- also reflected in database/init.sql for fresh setups.

-- ---------------------------------------------------------------------------
-- Temporal + vision fields on the enrichment table. Raw FIRMS rows are never
-- modified; every derived value lives here.
-- ---------------------------------------------------------------------------
ALTER TABLE hotspot_enrichment
    ADD COLUMN IF NOT EXISTS detections_24h                INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS detections_7d                 INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS detections_30d                INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS days_active_30d               INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS mean_frp_7d                   REAL,
    ADD COLUMN IF NOT EXISTS mean_frp_30d                  REAL,
    ADD COLUMN IF NOT EXISTS max_frp_30d                   REAL,
    ADD COLUMN IF NOT EXISTS current_frp_vs_historical_mean REAL,
    ADD COLUMN IF NOT EXISTS nighttime_detection_fraction  REAL,
    ADD COLUMN IF NOT EXISTS vision_industrial_prob        REAL,
    ADD COLUMN IF NOT EXISTS vision_fire_prob              REAL,
    ADD COLUMN IF NOT EXISTS vision_flare_prob             REAL,
    ADD COLUMN IF NOT EXISTS vision_facility_type          TEXT,
    ADD COLUMN IF NOT EXISTS vision_model_version          TEXT;

CREATE INDEX IF NOT EXISTS idx_hotspot_enrichment_enriched_at
    ON hotspot_enrichment (enriched_at DESC);

-- ---------------------------------------------------------------------------
-- Pipeline telemetry: one row per FIRMS fetch/insert attempt so the health
-- endpoint can report last runs, sources used, and error state without
-- touching secrets.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_run_log (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT NOT NULL DEFAULT 'firms',      -- 'firms' | 'backfill'
    source      TEXT NOT NULL,
    bbox        TEXT,
    date_from   DATE,
    date_to     DATE,
    days        INTEGER,
    returned    INTEGER NOT NULL DEFAULT 0,
    inserted    INTEGER NOT NULL DEFAULT 0,
    duplicates  INTEGER NOT NULL DEFAULT 0,
    invalid     INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'success',    -- 'success' | 'failed'
    error       TEXT,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_run_log_finished
    ON ingestion_run_log (finished_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_run_log_kind_status
    ON ingestion_run_log (kind, status);