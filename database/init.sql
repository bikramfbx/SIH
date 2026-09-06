CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE hotspots (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT,
    satellite       TEXT,
    instrument      TEXT,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    acq_date        DATE NOT NULL,
    acq_time        SMALLINT NOT NULL,
    frp             REAL,
    bright_ti4      REAL,
    bright_ti5      REAL,
    scan            REAL,
    track           REAL,
    confidence      TEXT,
    daynight        CHAR(1),
    version         TEXT,
    location        GEOGRAPHY(Point, 4326) NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_hotspots_location ON hotspots USING GIST (location);
CREATE INDEX idx_hotspots_acq_date ON hotspots (acq_date);

CREATE UNIQUE INDEX idx_hotspots_no_duplicates ON hotspots (
    source,
    satellite,
    ROUND(CAST(latitude AS numeric), 4),
    ROUND(CAST(longitude AS numeric), 4),
    acq_date,
    acq_time
);

CREATE TABLE industrial_facilities (
    id              BIGSERIAL PRIMARY KEY,
    osm_type        TEXT NOT NULL,
    osm_id          BIGINT NOT NULL,
    name            TEXT,
    facility_type   TEXT NOT NULL,
    tags            JSONB NOT NULL DEFAULT '{}'::jsonb,
    source          TEXT NOT NULL DEFAULT 'osm/overpass',
    geometry        GEOMETRY(Geometry, 4326),
    location        GEOGRAPHY(Point, 4326),
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_facilities_osm_identity ON industrial_facilities (osm_type, osm_id);
CREATE INDEX idx_facilities_geometry ON industrial_facilities USING GIST (geometry);
CREATE INDEX idx_facilities_location ON industrial_facilities USING GIST (location);
CREATE INDEX idx_facilities_facility_type ON industrial_facilities (facility_type);

-- Facility-context query cache (local Overpass queries around hotspots).
CREATE TABLE osm_query_cache (
    id            BIGSERIAL PRIMARY KEY,
    center_lat    DOUBLE PRECISION NOT NULL,
    center_lon    DOUBLE PRECISION NOT NULL,
    radius_m      NUMERIC NOT NULL,
    facility_count INT NOT NULL DEFAULT 0,
    queried_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_osm_query_cache_cover
    ON osm_query_cache
    USING GIST (CAST(ST_SetSRID(ST_MakePoint(center_lon, center_lat), 4326) AS geography));

-- Enrichment + classification, stored separately from the raw hotspot rows.
CREATE TABLE hotspot_enrichment (
    hotspot_id            BIGINT PRIMARY KEY REFERENCES hotspots (id) ON DELETE CASCADE,
    nearest_facility_id   BIGINT,
    nearest_facility_name TEXT,
    nearest_facility_type TEXT,
    distance_m            DOUBLE PRECISION,
    within_facility       BOOLEAN NOT NULL DEFAULT FALSE,
    nearby_detections_7d  INTEGER NOT NULL DEFAULT 0,
    persistence_score     REAL NOT NULL DEFAULT 0,
    frp_vs_nearby         REAL,
    class                 TEXT,
    reasons               JSONB NOT NULL DEFAULT '[]'::jsonb,
    enriched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_hotspot_enrichment_class ON hotspot_enrichment (class);

-- Classification stats helper: per-class counts.
CREATE OR REPLACE FUNCTION enrichment_class_counts()
RETURNS TABLE (class TEXT, count BIGINT) AS $$
    SELECT COALESCE(e.class, 'unclassified') AS class, COUNT(*) AS count
    FROM hotspots h
    LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
    GROUP BY 1
    ORDER BY 2 DESC
$$ LANGUAGE sql STABLE;

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