-- MVP migration (idempotent): facility-context query cache + hotspot enrichment.
-- Safe to run on an existing database; also reflected in database/init.sql
-- for fresh setups.

-- Cache of Overpass area queries already performed around a hotspot location,
-- so the same neighbourhood is never re-queried on a rerun.
CREATE TABLE IF NOT EXISTS osm_query_cache (
    id            BIGSERIAL PRIMARY KEY,
    center_lat    DOUBLE PRECISION NOT NULL,
    center_lon    DOUBLE PRECISION NOT NULL,
    radius_m      NUMERIC NOT NULL,
    facility_count INT NOT NULL DEFAULT 0,
    queried_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_osm_query_cache_cover
    ON osm_query_cache
    USING GIST (CAST(ST_SetSRID(ST_MakePoint(center_lon, center_lat), 4326) AS geography));

-- Enrichment + classification, stored separately from the raw hotspot rows.
CREATE TABLE IF NOT EXISTS hotspot_enrichment (
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

CREATE INDEX IF NOT EXISTS idx_hotspot_enrichment_class ON hotspot_enrichment (class);

-- Classification stats helper: per-class counts. Refresh via enrichment runs;
-- created once here for the stats endpoint.
CREATE OR REPLACE FUNCTION enrichment_class_counts()
RETURNS TABLE (class TEXT, count BIGINT) AS $$
    SELECT COALESCE(e.class, 'unclassified') AS class, COUNT(*) AS count
    FROM hotspots h
    LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
    GROUP BY 1
    ORDER BY 2 DESC
$$ LANGUAGE sql STABLE;