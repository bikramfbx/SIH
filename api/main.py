"""SIH MVP API.

Serves classified hotspot GeoJSON, per-hotspot detail, and aggregate stats.
Also serves the Leaflet frontend from ``frontend/`` at the site root.

Run (from repo root):
    uvicorn api.main:app --host 0.0.0.0 --port 8000
"""

import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import psycopg

from . import db

app = FastAPI(title="SIH Thermal Anomaly MVP", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GEOJSON_SQL = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', COALESCE(json_agg(
        json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(h.location::geometry)::json,
            'properties', json_build_object(
                'id', h.id,
                'source', h.source,
                'satellite', h.satellite,
                'instrument', h.instrument,
                'acq_date', to_char(h.acq_date, 'YYYY-MM-DD'),
                'acq_time', h.acq_time,
                'latitude', h.latitude,
                'longitude', h.longitude,
                'frp', h.frp,
                'confidence', h.confidence,
                'daynight', h.daynight,
                'class', COALESCE(e.class, 'unclassified'),
                'reasons', e.reasons,
                'nearest_facility_name', e.nearest_facility_name,
                'nearest_facility_type', e.nearest_facility_type,
                'distance_m', e.distance_m,
                'within_facility', COALESCE(e.within_facility, FALSE),
                'nearby_detections_7d', COALESCE(e.nearby_detections_7d, 0),
                'persistence_score', COALESCE(e.persistence_score, 0),
                'frp_vs_nearby', e.frp_vs_nearby
            )
        ) ORDER BY e.class, h.acq_date DESC
    ), '[]'::json)
) AS fc
FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE %(classes)s::text[] IS NULL
   OR COALESCE(e.class, 'unclassified') = ANY(%(classes)s::text[])
"""

DETAIL_SQL = """
SELECT json_build_object(
    'id', h.id,
    'source', h.source,
    'satellite', h.satellite,
    'instrument', h.instrument,
    'acq_date', to_char(h.acq_date, 'YYYY-MM-DD'),
    'acq_time', h.acq_time,
    'latitude', h.latitude,
    'longitude', h.longitude,
    'frp', h.frp,
    'bright_ti4', h.bright_ti4,
    'bright_ti5', h.bright_ti5,
    'scan', h.scan,
    'track', h.track,
    'confidence', h.confidence,
    'daynight', h.daynight,
    'version', h.version,
    'geometry', ST_AsGeoJSON(h.location::geometry)::json,
    'enrichment', json_build_object(
        'class', COALESCE(e.class, 'unclassified'),
        'reasons', e.reasons,
        'nearest_facility_id', e.nearest_facility_id,
        'nearest_facility_name', e.nearest_facility_name,
        'nearest_facility_type', e.nearest_facility_type,
        'distance_m', e.distance_m,
        'within_facility', COALESCE(e.within_facility, FALSE),
        'nearby_detections_7d', COALESCE(e.nearby_detections_7d, 0),
        'persistence_score', COALESCE(e.persistence_score, 0),
        'frp_vs_nearby', e.frp_vs_nearby,
        'enriched_at', e.enriched_at
    )
) AS detail
FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE h.id = %(hotspot_id)s
"""

STATS_SQL = """
SELECT json_build_object(
    'total_hotspots', (SELECT count(*) FROM hotspots),
    'total_facilities', (SELECT count(*) FROM industrial_facilities),
    'date_range', (
        SELECT json_build_object(
            'min', min(acq_date), 'max', max(acq_date),
            'days', count(DISTINCT acq_date)
        )
        FROM hotspots
    ),
    'by_class', (
        SELECT json_object_agg(
            t.class, t.count
            ORDER BY t.count DESC
        )
        FROM enrichment_class_counts() t
    )
) AS stats
"""


@app.get("/api/health")
def health():
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok", "database": "connected"}
    except psycopg.Error as e:
        raise HTTPException(status_code=503, detail=f"database error: {e}")


@app.get("/api/hotspots.geojson")
def hotspots_geojson(
    class_: list[str] | None = Query(
        None,
        alias="class",
        description="Filter by classification; may be repeated.",
    ),
):
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(GEOJSON_SQL, {"classes": class_})
                return cur.fetchone()[0]
    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/hotspots/{hotspot_id}")
def hotspot_detail(hotspot_id: int):
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(DETAIL_SQL, {"hotspot_id": hotspot_id})
                row = cur.fetchone()
    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    if row is None or row[0] is None:
        raise HTTPException(status_code=404, detail="hotspot not found")
    return row[0]


@app.get("/api/stats")
def stats():
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(STATS_SQL)
                return cur.fetchone()[0]
    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail=str(e))


# Serve the Leaflet frontend as the site root. Must be mounted last so the
# /api routes above take precedence.
_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")