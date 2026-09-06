"""Hotspot enrichment + classification (MVP).

Computes, for every hotspot, a set of enrichment metrics stored SEPARATELY
from the raw hotspot row (see ``hotspot_enrichment``):

  * nearest industrial facility (id, name, type) and distance in meters
  * whether the hotspot lies inside a facility polygon
  * number of detections near the same location in the recent window,
    a persistence score in [0,1], and the hotspot FRP vs the mean FRP of
    nearby detections

Then classifies each hotspot with the transparent rule-based classifier
(``classifier.py``) and stores ``class`` + ``reasons``.

Idempotent: re-running recomputes and upserts in place; no duplicates.

ENV (all optional)
    PERSIST_RADIUS_M      radius for "same location" clustering (default 1000)
    PERSIST_WINDOW_DAYS   lookback window (default 7)
    PERSIST_DENOM         detections count that yields persistence 1.0 (default 5)
"""

import json
import os
import sys

from dotenv import load_dotenv
import psycopg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import classifier  # noqa: E402

DEFAULT_PERSIST_RADIUS_M = 1000
DEFAULT_PERSIST_WINDOW_DAYS = 7
DEFAULT_PERSIST_DENOM = classifier.NEARBY_COUNT_FOR_PERSISTENCE

ENRICH_SQL = """
INSERT INTO hotspot_enrichment (
    hotspot_id,
    nearest_facility_id, nearest_facility_name, nearest_facility_type,
    distance_m, within_facility,
    nearby_detections_7d, persistence_score, frp_vs_nearby,
    enriched_at
)
SELECT
    h.id,
    n.fid,
    n.fname,
    n.ftype,
    ROUND(n.dist_m::numeric, 1)::double precision,
    COALESCE(
        (SELECT EXISTS (
            SELECT 1 FROM industrial_facilities ff
            WHERE ST_Covers(ff.geometry, CAST(h.location AS geometry))
        )),
        FALSE
    ),
    cnt.nearby,
    ROUND(LEAST(1.0, cnt.nearby::numeric / %(persist_denom)s)::numeric, 2)::real,
    ROUND((h.frp / GREATEST(cnt.avg_frp, 0.1))::numeric, 2)::real,
    NOW()
FROM hotspots h
LEFT JOIN LATERAL (
    SELECT f.id AS fid, f.name AS fname, f.facility_type AS ftype,
           ST_Distance(h.location, CAST(f.geometry AS geography)) AS dist_m
    FROM industrial_facilities f
    ORDER BY ST_Distance(h.location, CAST(f.geometry AS geography))
    LIMIT 1
) n ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS nearby,
           COALESCE(AVG(o.frp), 0)::real AS avg_frp
    FROM hotspots o
    WHERE o.id <> h.id
      AND o.acq_date BETWEEN h.acq_date - (%(window_days)s || ' days')::interval
                         AND h.acq_date
      AND ST_DWithin(h.location, o.location, %(persist_radius_m)s)
) cnt ON TRUE
ON CONFLICT (hotspot_id) DO UPDATE SET
    nearest_facility_id   = EXCLUDED.nearest_facility_id,
    nearest_facility_name = EXCLUDED.nearest_facility_name,
    nearest_facility_type = EXCLUDED.nearest_facility_type,
    distance_m            = EXCLUDED.distance_m,
    within_facility       = EXCLUDED.within_facility,
    nearby_detections_7d  = EXCLUDED.nearby_detections_7d,
    persistence_score     = EXCLUDED.persistence_score,
    frp_vs_nearby         = EXCLUDED.frp_vs_nearby,
    enriched_at           = NOW()
"""

FETCH_SQL = """
SELECT h.id, h.frp,
       e.nearest_facility_id, e.nearest_facility_name, e.nearest_facility_type,
       e.distance_m, e.within_facility,
       e.nearby_detections_7d, e.persistence_score, e.frp_vs_nearby
FROM hotspots h
JOIN hotspot_enrichment e ON e.hotspot_id = h.id
ORDER BY h.id
"""

UPDATE_SQL = """
UPDATE hotspot_enrichment
SET class = %(class)s, reasons = %(reasons)s::jsonb
WHERE hotspot_id = %(hotspot_id)s
"""


def main():
    load_dotenv()

    persist_radius = float(os.getenv("PERSIST_RADIUS_M", DEFAULT_PERSIST_RADIUS_M))
    window_days = int(os.getenv("PERSIST_WINDOW_DAYS", DEFAULT_PERSIST_WINDOW_DAYS))
    persist_denom = float(os.getenv("PERSIST_DENOM", DEFAULT_PERSIST_DENOM))

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    with psycopg.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            print(
                f"Enriching hotspots "
                f"(persist_radius={persist_radius:.0f}m, "
                f"window={window_days}d, persistence 1.0 at "
                f"{persist_denom:g} nearby detections)"
            )
            cur.execute(
                ENRICH_SQL,
                {
                    "persist_radius_m": persist_radius,
                    "window_days": window_days,
                    "persist_denom": persist_denom,
                },
            )
            print(f"Enriched rows: {cur.rowcount}")

        # Classify every enrichment row.
        class_counts = {}
        with conn.cursor() as cur:
            cur.execute(FETCH_SQL)
            rows = cur.fetchall()

            for r in rows:
                hotspot = {"id": r[0], "frp": r[1]}
                enrichment = {
                    "nearest_facility_id": r[2],
                    "nearest_facility_name": r[3],
                    "nearest_facility_type": r[4],
                    "distance_m": r[5],
                    "within_facility": r[6],
                    "nearby_detections_7d": r[7],
                    "persistence_score": r[8],
                    "frp_vs_nearby": r[9],
                }
                label, reasons = classifier.classify(hotspot, enrichment)
                cur.execute(
                    UPDATE_SQL,
                    {
                        "hotspot_id": r[0],
                        "class": label,
                        "reasons": json.dumps(reasons),
                    },
                )
                class_counts[label] = class_counts.get(label, 0) + 1

        conn.commit()

    print("\n--- Enrichment + Classification Summary ---")
    print(f"Hotspots classified: {sum(class_counts.values())}")
    for label in sorted(class_counts):
        print(f"  {label:>28}: {class_counts[label]}")


if __name__ == "__main__":
    main()