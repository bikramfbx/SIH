"""Hotspot enrichment + classification (final backend).

Computes, for every hotspot (or a chosen subset), enrichment metrics stored
SEPARATELY from the raw hotspot row (see ``hotspot_enrichment``):

GIS context
    * nearest industrial facility (id, name, type) and distance in meters
    * whether the hotspot lies inside a facility polygon

Temporal / historical features (spatial matching tolerance = ``PERSIST_RADIUS_M``)
    * detections_24h / _7d / _30d   counts near this location before now
    * days_active_30d               distinct acquisition dates in 30 d
    * mean_frp_7d / mean_frp_30d    mean FRP of those neighbours
    * max_frp_30d                   hottest neighbour in 30 d
    * current_frp_vs_historical_mean  this FRP / 30-d mean (baseline ratio)
    * nighttime_detection_fraction  share of recent (30 d) detections at night,
      counting the current detection itself
    * persistence_score             normalized 7-d recurrence in [0,1]

Then classifies with the transparent rule-based classifier (``classifier.py``)
and stores ``class`` + ``reasons``.

Idempotent: re-running recomputes and upserts in place; no duplicates.
Only the requested hotspot ids are updated, so the scheduler can enrich just
the new detections from each ingest cycle.

ENV (all optional)
    PERSIST_RADIUS_M          matching tolerance in meters (default 1000)
    PERSIST_WINDOW_DAYS       7-d recurrence window (default 7)
    PERSIST_DENOM             detections that yield persistence 1.0 (default 5)
"""

import json
import os
import sys

from dotenv import load_dotenv
import psycopg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import classifier  # noqa: E402
import land_cover  # noqa: E402

DEFAULT_PERSIST_RADIUS_M = 1000
DEFAULT_PERSIST_WINDOW_DAYS = 7
DEFAULT_PERSIST_DENOM = classifier.NEARBY_COUNT_FOR_PERSISTENCE


def _ids_array(hotspot_ids):
    """Render a bigint[] array literal for a psycopg parameter cast."""
    if not hotspot_ids:
        return None
    return "{" + ",".join(str(int(i)) for i in hotspot_ids) + "}"


ENRICH_SQL = """
WITH hh AS (
    SELECT h.*,
           h.acq_date + ((h.acq_time / 100) * INTERVAL '1 hour')
           + (mod(h.acq_time, 100) * INTERVAL '1 minute') AS dt
    FROM hotspots h
    WHERE %(ids)s::bigint[] IS NULL
       OR h.id = ANY(%(ids)s::bigint[])
)
INSERT INTO hotspot_enrichment (
    hotspot_id,
    nearest_facility_id, nearest_facility_name, nearest_facility_type,
    distance_m, within_facility,
    detections_24h, detections_7d, detections_30d, days_active_30d,
    mean_frp_7d, mean_frp_30d, max_frp_30d,
    current_frp_vs_historical_mean,
    nighttime_detection_fraction,
    persistence_score, frp_vs_nearby,
    enriched_at
)
SELECT
    h.id,
    n.fid, n.fname, n.ftype,
    ROUND(n.dist_m::numeric, 1)::double precision,
    COALESCE(n.inside, FALSE),
    t.d24, t.d7, t.d30, t.active30,
    ROUND(t.mean7::numeric, 2)::real,
    ROUND(t.mean30::numeric, 2)::real,
    ROUND(t.max30::numeric, 2)::real,
    ROUND((h.frp / GREATEST(t.mean30, 0.1))::numeric, 2)::real,
    ROUND(t.nightfrac::numeric, 3)::real,
    ROUND(LEAST(1.0, t.d7::numeric / %(persist_denom)s)::numeric, 2)::real,
    ROUND((h.frp / GREATEST(t.mean7, 0.1))::numeric, 2)::real,
    NOW()
FROM hh h
LEFT JOIN LATERAL (
    SELECT f.id AS fid, f.name AS fname, f.facility_type AS ftype,
           ST_Distance(h.location, CAST(f.geometry AS geography)) AS dist_m,
           ST_Covers(f.geometry, CAST(h.location AS geometry)) AS inside
    FROM industrial_facilities f
    ORDER BY ST_Distance(h.location, CAST(f.geometry AS geography))
    LIMIT 1
) n ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE o.dt >= h.dt - INTERVAL '24 hours')::int AS d24,
        COUNT(*) FILTER (WHERE o.dt >= h.dt - INTERVAL '1 day' * %(window_days)s)::int AS d7,
        COUNT(*) FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days')::int AS d30,
        COUNT(DISTINCT o.acq_date)
            FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days')::int AS active30,
        COALESCE(AVG(o.frp) FILTER (WHERE o.dt >= h.dt - INTERVAL '7 days'), 0)::real AS mean7,
        COALESCE(AVG(o.frp) FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days'), 0)::real AS mean30,
        COALESCE(MAX(o.frp) FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days'), 0)::real AS max30,
        CASE WHEN COUNT(*) FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days') +
                  1 = 0 THEN NULL
             ELSE (COUNT(*) FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days' AND o.night)
                   + CASE WHEN (h.daynight = 'N') THEN 1 ELSE 0 END)::real
                  / (COUNT(*) FILTER (WHERE o.dt >= h.dt - INTERVAL '30 days') + 1)::real
        END AS nightfrac
    FROM (
        SELECT o.id,
               o.acq_date + ((o.acq_time / 100) * INTERVAL '1 hour')
                         + (mod(o.acq_time, 100) * INTERVAL '1 minute') AS dt,
               o.acq_date, o.frp, (o.daynight = 'N') AS night
        FROM hotspots o
        WHERE o.id <> h.id
          AND o.acq_date >= h.acq_date - INTERVAL '30 days'
          AND ST_DWithin(h.location, o.location, %(persist_radius_m)s)
    ) o
) t ON TRUE
ON CONFLICT (hotspot_id) DO UPDATE SET
    nearest_facility_id   = EXCLUDED.nearest_facility_id,
    nearest_facility_name = EXCLUDED.nearest_facility_name,
    nearest_facility_type = EXCLUDED.nearest_facility_type,
    distance_m            = EXCLUDED.distance_m,
    within_facility       = EXCLUDED.within_facility,
    detections_24h        = EXCLUDED.detections_24h,
    detections_7d         = EXCLUDED.detections_7d,
    detections_30d        = EXCLUDED.detections_30d,
    days_active_30d       = EXCLUDED.days_active_30d,
    mean_frp_7d           = EXCLUDED.mean_frp_7d,
    mean_frp_30d          = EXCLUDED.mean_frp_30d,
    max_frp_30d           = EXCLUDED.max_frp_30d,
    current_frp_vs_historical_mean = EXCLUDED.current_frp_vs_historical_mean,
    nighttime_detection_fraction   = EXCLUDED.nighttime_detection_fraction,
    persistence_score     = EXCLUDED.persistence_score,
    frp_vs_nearby         = EXCLUDED.frp_vs_nearby,
    enriched_at           = NOW()
"""

FETCH_SQL = """
SELECT h.id, h.frp, h.daynight,
       e.nearest_facility_id, e.nearest_facility_name, e.nearest_facility_type,
       e.distance_m, e.within_facility,
       e.detections_24h, e.detections_7d, e.detections_30d,
       e.days_active_30d, e.mean_frp_7d, e.mean_frp_30d, e.max_frp_30d,
       e.current_frp_vs_historical_mean, e.nighttime_detection_fraction,
       e.persistence_score, e.frp_vs_nearby,
       e.vision_industrial_prob, e.vision_fire_prob, e.vision_flare_prob,
       e.vision_facility_type, e.vision_model_version
FROM hotspots h
JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE %(ids)s::bigint[] IS NULL
   OR h.id = ANY(%(ids)s::bigint[])
ORDER BY h.id
"""

UPDATE_SQL = """
UPDATE hotspot_enrichment
SET class = %(class)s, reasons = %(reasons)s::jsonb
WHERE hotspot_id = %(hotspot_id)s
"""


def enrich(conn, conn_info=None, hotspot_ids=None, persist_radius=None,
           window_days=None, persist_denom=None, classify=True):
    """Enrich (and classify) hotspots. ``conn`` is a psycopg connection.

    When ``hotspot_ids`` is given, only those hotspots are recomputed --
    this is what the scheduler uses to enrich just the new detections of a
    cycle. Returns a dict summary.
    """
    persist_radius = persist_radius or float(
        os.getenv("PERSIST_RADIUS_M", DEFAULT_PERSIST_RADIUS_M))
    window_days = int(window_days or os.getenv(
        "PERSIST_WINDOW_DAYS", DEFAULT_PERSIST_WINDOW_DAYS))
    persist_denom = float(persist_denom or os.getenv(
        "PERSIST_DENOM", DEFAULT_PERSIST_DENOM))

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")

    ids_param = _ids_array(hotspot_ids)
    with conn.cursor() as cur:
        cur.execute(ENRICH_SQL, {
            "ids": ids_param,
            "persist_radius_m": persist_radius,
            "window_days": window_days,
            "persist_denom": persist_denom,
        })
        enriched = cur.rowcount

    class_counts = {}
    if classify:
        with conn.cursor() as cur:
            cur.execute(FETCH_SQL, {"ids": ids_param})
            updates = []
            for r in cur.fetchall():
                hotspot = {
                    "id": r[0], "frp": r[1], "daynight": r[2],
                }
                enrichment = {
                    "nearest_facility_id": r[3],
                    "nearest_facility_name": r[4],
                    "nearest_facility_type": r[5],
                    "distance_m": r[6],
                    "within_facility": r[7],
                    "detections_24h": r[8],
                    "detections_7d": r[9],
                    "detections_30d": r[10],
                    "days_active_30d": r[11],
                    "mean_frp_7d": r[12],
                    "mean_frp_30d": r[13],
                    "max_frp_30d": r[14],
                    "current_frp_vs_historical_mean": r[15],
                    "nighttime_detection_fraction": r[16],
                    "persistence_score": r[17],
                    "frp_vs_nearby": r[18],
                    "vision_industrial_prob": r[19],
                    "vision_fire_prob": r[20],
                    "vision_flare_prob": r[21],
                    "vision_facility_type": r[22],
                    "vision_model_version": r[23],
                }
                label, reasons = classifier.classify(hotspot, enrichment)
                updates.append({
                    "hotspot_id": r[0],
                    "class": label,
                    "reasons": json.dumps(reasons),
                })
                class_counts[label] = class_counts.get(label, 0) + 1
            if updates:
                cur.executemany(UPDATE_SQL, updates)

    class_counts.update(land_cover.persist_if_enabled(conn, ids_param))
    return {"enriched": enriched, "class_counts": class_counts}


def main():
    load_dotenv()

    ids_arg = None
    if len(sys.argv) > 1:
        raw = sys.argv[1:]
        if "--all" not in raw:
            ids_arg = [int(x) for x in raw if x.isdigit()]
        else:
            ids_arg = None
    scope = "subset" if ids_arg else "all"
    print(f"Enriching hotspots ({scope})...")

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    with psycopg.connect(**conn_info) as conn:
        summary = enrich(conn, conn_info=conn_info, hotspot_ids=ids_arg)
        conn.commit()

    counts = summary["class_counts"]
    print(f"Enriched rows: {summary['enriched']}")
    print(f"Hotspots classified: {sum(counts.values())}")
    for label in sorted(counts):
        print(f"  {label:>28}: {counts[label]}")


if __name__ == "__main__":
    main()