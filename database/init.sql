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

