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
