"""SIH thermal-anomaly API (final backend).

Serves classified hotspot GeoJSON (with GIS + temporal + vision evidence),
per-hotspot detail, aggregate stats, and pipeline health. Also serves the
Leaflet frontend from ``frontend/`` at the site root.

Run (from repo root):
    uvicorn api.main:app --host 0.0.0.0 --port 8000
"""

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import psycopg

from . import db

API_VERSION = "2.0.0"
app = FastAPI(title="SIH Thermal Anomaly API", version=API_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GEOJSON_FEATURES = """
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
            'acq_datetime', to_char(
                h.acq_date + ((h.acq_time / 100) * INTERVAL '1 hour')
                          + (mod(h.acq_time, 100) * INTERVAL '1 minute'),
                'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
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
            'detections_24h', COALESCE(e.detections_24h, 0),
            'detections_7d', COALESCE(e.detections_7d, 0),
            'detections_30d', COALESCE(e.detections_30d, 0),
            'days_active_30d', COALESCE(e.days_active_30d, 0),
            'mean_frp_7d', e.mean_frp_7d,
            'mean_frp_30d', e.mean_frp_30d,
            'max_frp_30d', e.max_frp_30d,
            'current_frp_vs_historical_mean', e.current_frp_vs_historical_mean,
            'nighttime_detection_fraction', e.nighttime_detection_fraction,
            'persistence_score', COALESCE(e.persistence_score, 0),
            'frp_vs_nearby', e.frp_vs_nearby,
            'vision_industrial_prob', e.vision_industrial_prob,
            'vision_fire_prob', e.vision_fire_prob,
            'vision_flare_prob', e.vision_flare_prob,
            'vision_facility_type', e.vision_facility_type,
            'vision_model_version', e.vision_model_version,
            'enriched_at', e.enriched_at
        )
    )
"""

GEOJSON_COUNT_SQL = """
SELECT COUNT(*) FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE {where}
"""

GEOJSON_SQL = """
SELECT COALESCE(json_agg(feat ORDER BY feat->'properties'->>'class',
                         feat->'properties'->>'acq_datetime' DESC), '[]'::json)
FROM (
    SELECT {features} AS feat
    FROM hotspots h
    LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
    WHERE {where}
    ORDER BY e.class NULLS LAST, h.acq_date DESC, h.acq_time DESC
    LIMIT %(limit)s
) s
"""

DETAIL_SQL = """
SELECT json_build_object(
    'id', h.id,
    'source', h.source,
    'satellite', h.satellite,
    'instrument', h.instrument,
    'acq_date', to_char(h.acq_date, 'YYYY-MM-DD'),
    'acq_time', h.acq_time,
    'acq_datetime', to_char(
        h.acq_date + ((h.acq_time / 100) * INTERVAL '1 hour')
                  + (mod(h.acq_time, 100) * INTERVAL '1 minute'),
        'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
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
        'detections_24h', COALESCE(e.detections_24h, 0),
        'detections_7d', COALESCE(e.detections_7d, 0),
        'detections_30d', COALESCE(e.detections_30d, 0),
        'days_active_30d', COALESCE(e.days_active_30d, 0),
        'mean_frp_7d', e.mean_frp_7d,
        'mean_frp_30d', e.mean_frp_30d,
        'max_frp_30d', e.max_frp_30d,
        'current_frp_vs_historical_mean', e.current_frp_vs_historical_mean,
        'nighttime_detection_fraction', e.nighttime_detection_fraction,
        'persistence_score', COALESCE(e.persistence_score, 0),
        'frp_vs_nearby', e.frp_vs_nearby,
        'vision_industrial_prob', e.vision_industrial_prob,
        'vision_fire_prob', e.vision_fire_prob,
        'vision_flare_prob', e.vision_flare_prob,
        'vision_facility_type', e.vision_facility_type,
        'vision_model_version', e.vision_model_version,
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
            'min', to_char(min(acq_date), 'YYYY-MM-DD'),
            'max', to_char(max(acq_date), 'YYYY-MM-DD'),
            'days', count(DISTINCT acq_date)
        )
        FROM hotspots
    ),
    'sources', (
        SELECT COALESCE(json_agg(DISTINCT source ORDER BY source), '[]'::json)
        FROM hotspots WHERE source IS NOT NULL
    ),
    'by_class', (
        SELECT json_object_agg(t.class, t.count ORDER BY t.count DESC)
        FROM enrichment_class_counts() t
    ),
    'last_ingestion', (
        SELECT json_build_object(
            'finished_at', finished_at, 'kind', kind, 'source', source,
            'returned', returned, 'inserted', inserted, 'status', status
        )
        FROM ingestion_run_log
        ORDER BY finished_at DESC LIMIT 1
    )
) AS stats
"""

HEALTH_SQL = """
SELECT json_build_object(
    'last_successful_ingestion', (
        SELECT json_build_object(
            'finished_at', finished_at, 'kind', kind, 'source', source,
            'bbox', bbox, 'days', days,
            'returned', returned, 'inserted', inserted,
            'duplicates', duplicates, 'invalid', invalid
        )
        FROM ingestion_run_log WHERE status = 'success'
        ORDER BY finished_at DESC LIMIT 1
    ),
    'last_run', (
        SELECT json_build_object(
            'finished_at', finished_at, 'kind', kind, 'source', source,
            'returned', returned, 'inserted', inserted,
            'duplicates', duplicates, 'invalid', invalid,
            'status', status, 'error', error
        )
        FROM ingestion_run_log
        ORDER BY finished_at DESC LIMIT 1
    ),
    'sources_used', (
        SELECT COALESCE(json_agg(DISTINCT source ORDER BY source), '[]'::json)
        FROM ingestion_run_log
    ),
    'per_source', (
        SELECT COALESCE(json_object_agg(sub.source, sub.last_run ORDER BY sub.source), '{}'::json)
        FROM (
            SELECT DISTINCT ON (source) source,
                json_build_object(
                    'status', status, 'kind', kind,
                    'finished_at', finished_at,
                    'returned', returned, 'inserted', inserted,
                    'error', error
                ) AS last_run
            FROM ingestion_run_log
            ORDER BY source, finished_at DESC
        ) sub
    ),
    'inserted_24h', (
        SELECT COALESCE(SUM(inserted), 0)
        FROM ingestion_run_log
        WHERE status = 'success' AND finished_at >= NOW() - INTERVAL '24 hours'
    ),
    'last_enrichment_at', (SELECT MAX(enriched_at) FROM hotspot_enrichment),
    'classified_count', (
        SELECT COUNT(*) FROM hotspot_enrichment WHERE class IS NOT NULL
    ),
    'total_hotspots', (SELECT COUNT(*) FROM hotspots),
    'total_facilities', (SELECT COUNT(*) FROM industrial_facilities),
    'worker', (
        SELECT json_build_object(
            'configured', COUNT(*) > 0,
            'alive', COUNT(*) > 0
                AND MAX(last_heartbeat_at) > NOW() - INTERVAL '45 minutes',
            'worker_id', MAX(worker_id),
            'last_heartbeat_at', MAX(last_heartbeat_at),
            'cycle_count', MAX(cycle_count),
            'last_cycle_status', MAX(last_cycle_status),
            'last_error', MAX(last_error),
            'version', MAX(version)
        )
        FROM worker_status
    ),
    'vision', (
        SELECT json_build_object(
            'configured', FALSE,
            'model_version', MAX(vision_model_version),
            'inferred_count', COUNT(vision_model_version)
        )
        FROM hotspot_enrichment
    ),
    'landcover', (
        SELECT json_build_object(
            'configured', BOOL_OR(landcover_class IS NOT NULL),
            'enriched_count', COUNT(landcover_class)
        )
        FROM hotspot_enrichment
    )
) AS health
"""


def _db_ok():
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except psycopg.Error:
        return False


def _build_hotspot_filters(class_, bbox, date_from, date_to, sources):
    """Return (filters_sql, params) for the GEOJSON queries."""
    conds = []
    params = {}

    if class_:
        conds.append("COALESCE(e.class, 'unclassified') = ANY(%(classes)s::text[])")
        params["classes"] = class_
    if sources:
        conds.append("h.source = ANY(%(sources)s::text[])")
        params["sources"] = sources
    if date_from:
        conds.append("h.acq_date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        conds.append("h.acq_date <= %(date_to)s")
        params["date_to"] = date_to
    if bbox:
        w, s, e, n = bbox
        conds.append(
            "ST_Intersects(h.location, "
            "CAST(ST_MakeEnvelope(%(bbox_w)s, %(bbox_s)s, %(bbox_e)s, %(bbox_n)s, 4326) "
            "AS geography))"
        )
        params.update(bbox_w=w, bbox_s=s, bbox_e=e, bbox_n=n)

    where = " AND ".join(conds) if conds else "TRUE"
    return where, params


def _parse_bbox(value):
    parts = [p.strip() for p in value.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be west,south,east,north")
    w, s, e, n = (float(p) for p in parts)
    if not (-180 <= w <= 180 and -180 <= e <= 180
            and -90 <= s <= 90 and -90 <= n <= 90):
        raise ValueError("bbox out of range")
    return w, s, e, n


def _parse_date(value, name):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a date in YYYY-MM-DD format")


@app.get("/api/health")
def health():
    db_up = _db_ok()
    if not db_up:
        raise HTTPException(status_code=503, detail="database unavailable")

    info = {"status": "ok", "database": "connected", "api_version": API_VERSION}
    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(HEALTH_SQL)
                row = cur.fetchone()
        info.update(row[0])
    except psycopg.errors.UndefinedTable:
        # Migration not applied yet: still report API + DB up.
        info["warning"] = "migration not applied (ingestion_run_log missing)"
        return info

    last = info.get("last_run") or {}
    if last.get("status") == "failed":
        try:
            when = datetime.fromisoformat(last["finished_at"].replace("Z", "+00:00"))
            if not isinstance(when, datetime):
                raise ValueError
        except (ValueError, TypeError):
            when = None
        if when and when >= datetime.now(timezone.utc) - timedelta(hours=24):
            info["status"] = "degraded"
            info["pipeline_error"] = last.get("error")
    return info


@app.get("/api/hotspots.geojson")
def hotspots_geojson(
    class_: list[str] | None = Query(None, alias="class",
                                     description="Classification; repeatable."),
    bbox: str | None = Query(None, description="west,south,east,north viewport."),
    date_from: str | None = Query(None, description="Include acq_date >= YYYY-MM-DD."),
    date_to: str | None = Query(None, description="Include acq_date <= YYYY-MM-DD."),
    source: list[str] | None = Query(None, description="FIRMS source; repeatable."),
    limit: int = Query(50000, ge=1, le=200000,
                       description="Max features returned per request."),
):
    try:
        box = _parse_bbox(bbox) if bbox else None
        d_from = _parse_date(date_from, "date_from") if date_from else None
        d_to = _parse_date(date_to, "date_to") if date_to else None
        if d_from and d_to and d_from > d_to:
            raise ValueError("date_from must not be after date_to")
        filters, params = _build_hotspot_filters(class_, box, d_from, d_to, source)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    try:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(GEOJSON_COUNT_SQL.format(where=filters), params)
                total = cur.fetchone()[0]
                cur.execute(
                    GEOJSON_SQL.format(where=filters, features=GEOJSON_FEATURES),
                    {**params, "limit": limit},
                )
                features = cur.fetchone()[0]
    except psycopg.Error as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features),
        "TotalFeatures": total,
        "numberMatched": total,
        "truncated": total > limit,
    }


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