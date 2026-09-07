"""Fast gated enrichment (local compute + batch upsert).

Computes enrichment metrics + class for industrial-context candidate rows
WITHOUT the per-row Postgres LATERALs that make the server-side enrich path
O(N*M) over 381k rows on a pooled connection.

Plan
----
1.  Candidate IDs come from a fast set-based SQL probe (EXISTS / ST_DWithin,
    index-backed — same shape as the prune dry-run, which ran in ~1 min).
2.  All hotspots are pulled into Python once (plain SELECT, no LATERAL); an
    in-memory 0.01-deg grid gives the temporal persistence neighbours.
3.  Nearest-facility distance / inside-polygon computed locally per candidate
    with a haversine + ray-casting implementation (no geo lib dependency).
4.  ``classifier.classify`` assigns a label + reasons.
5.  Batch-upsert via ``jsonb_to_recordset ... ON CONFLICT (hotspot_id)``.
"""
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
import psycopg

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import classifier  # noqa: E402

BUFFER_M = float(os.getenv("PRUNE_FAR_FROM_FACILITY_M", "5000"))
PERSIST_RADIUS_M = float(os.getenv("PERSIST_RADIUS_M", "1000"))
PERSIST_DENOM = float(classifier.NEARBY_COUNT_FOR_PERSISTENCE)
KEEP_CLASSES = ("industrial_fire", "persistent_industrial_source", "gas_flare")

CELL_STEP = 0.01
EARTH_R = 6371000.0
KM_PER_DEG = 111320.0


def _fmt(n):
    return f"{n:,}"


def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * EARTH_R * math.asin(min(1.0, math.sqrt(a)))


def cell_key(lat, lon):
    return (round(float(lat), 2), round(float(lon), 2))


def cells_near(cell, cells_radius=1):
    lat, lon = cell
    step = CELL_STEP
    for dlat in range(-cells_radius, cells_radius + 1):
        for dlon in range(-cells_radius, cells_radius + 1):
            yield (round(lat + dlat * step, 2), round(lon + dlon * step, 2))


def parse_polygons(wkt):
    """Parse WKT into ring lists [(lon,lat), ...] (accepts MULTIPOLYGON)."""
    if not wkt:
        return []
    rings = []
    for m in re.finditer(r"\(\(([^)]+)\)\)", wkt):
        ring = []
        for token in m.group(1).split(","):
            parts = token.strip().replace("(", "").replace(")", "").split()
            if len(parts) >= 2:
                try:
                    ring.append((float(parts[0]), float(parts[1])))
                except ValueError:
                    pass
        if ring:
            rings.append(ring)
    return rings


def point_in_rings(lat, lon, rings):
    inside = False
    for ring in rings:
        n = len(ring)
        for i in range(n):
            x1, y1 = ring[i]
            x2, y2 = ring[(i + 1) % n]
            if ((y1 > lat) != (y2 > lat)) and (
                    lon < (x2 - x1) * (lat - y1) / ((y2 - y1) or 1e-12) + x1):
                inside = not inside
    return inside


def point_to_ring_m(lat, lon, ring):
    best = float("inf")
    mper_lon = KM_PER_DEG * (math.cos(math.radians(lat)) or 1.0)
    for i in range(len(ring)):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % len(ring)]
        dx, dy = x2 - x1, y2 - y1
        seg2 = dx * dx + dy * dy
        t = 0.0 if seg2 == 0 else max(
            0.0, min(1.0, ((lon - x1) * dx + (lat - y1) * dy) / seg2))
        px, py = x1 + t * dx, y1 + t * dy
        dlon = min(abs(lon - px), abs(lon - px - 360), abs(lon - px + 360))
        best = min(best, math.hypot(dlon * mper_lon, abs(lat - py) * KM_PER_DEG))
    return best


def conn_info():
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "sslmode": os.getenv("POSTGRES_SSLMODE", "prefer"),
        "prepare_threshold": None,
    }


CANDIDATE_SQL = r"""
SELECT h.id
FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE (e.class IS NULL OR NOT (e.class = ANY(%(keep)s::text[])))
  AND (
    COALESCE(e.vision_industrial_prob > 0.6, FALSE)
    OR EXISTS (
      SELECT 1 FROM industrial_facilities f
      WHERE f.geometry IS NOT NULL
        AND f.geometry && CAST(h.location AS geometry)
        AND ST_Covers(f.geometry, CAST(h.location AS geometry))
    )
    OR EXISTS (
      SELECT 1 FROM industrial_facilities f
      WHERE f.location IS NOT NULL
        AND ST_DWithin(h.location, f.location, %(buf)s)
    )
  )
AND h.id > %(after)s AND h.id <= %(upto)s
"""


def candidate_ids(conn):
    keep_arr = "{" + ",".join(KEEP_CLASSES) + "}"
    ids = []
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute("SELECT MIN(id), MAX(id) FROM hotspots")
        lo, hi = cur.fetchone()
        if lo is None:
            return ids
        after = lo - 1
        while after < hi:
            upto = min(after + 50000, hi)
            cur.execute(
                CANDIDATE_SQL,
                {"keep": keep_arr, "buf": BUFFER_M, "after": after, "upto": upto},
            )
            ids.extend(r[0] for r in cur.fetchall())
            after = upto
    return ids


def load_hotspots(conn):
    hs = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, frp, daynight, acq_date, acq_time, "
            "       ST_Y(location::geometry)::float8, "
            "       ST_X(location::geometry)::float8 "
            "FROM hotspots")
        for id_, frp, daynight, acq_date, acq_time, lat, lon in cur.fetchall():
            dt = None
            if acq_date is not None and acq_time is not None:
                dt = datetime.combine(
                    acq_date, datetime.min.time().replace(
                        hour=int(acq_time) // 100, minute=int(acq_time) % 100))
            hs[id_] = {
                "id": id_, "frp": frp, "daynight": daynight,
                "acq_date": acq_date, "dt": dt, "lat": lat, "lon": lon,
            }
    return hs


def load_facilities(conn):
    fac = []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, facility_type, "
            "       ST_Y(location::geometry)::float8, "
            "       ST_X(location::geometry)::float8, "
            "       ST_AsText(geometry) "
            "FROM industrial_facilities WHERE location IS NOT NULL")
        for id_, name, ftype, lat, lon, wkt in cur.fetchall():
            fac.append({"id": id_, "name": name, "type": ftype,
                        "lat": lat, "lon": lon, "rings": parse_polygons(wkt)})
    return fac


def temporal_metrics(h, hs, grid):
    rec = []
    for c in cells_near(cell_key(h["lat"], h["lon"])):
        for o in grid.get(c, ()):
            if o["id"] == h["id"] or o["dt"] is None or h["dt"] is None:
                continue
            if haversine_m(h["lat"], h["lon"], o["lat"], o["lon"]) <= PERSIST_RADIUS_M:
                rec.append(o)

    dt = h["dt"]
    days30 = dt - timedelta(days=30)
    days7 = dt - timedelta(days=7)
    days1 = dt - timedelta(hours=24)

    d24 = d7 = d30 = night30 = 0
    frps7, frps30 = [], []
    active_dates = set()
    for o in rec:
        if o["dt"] >= days1:
            d24 += 1
        if o["dt"] >= days7:
            d7 += 1
            if o["frp"] is not None:
                frps7.append(o["frp"])
        if o["dt"] >= days30:
            d30 += 1
            if o["frp"] is not None:
                frps30.append(o["frp"])
            active_dates.add(o["acq_date"])
            if o["daynight"] == "N":
                night30 += 1

    mean7 = sum(frps7) / len(frps7) if frps7 else 0.0
    mean30 = sum(frps30) / len(frps30) if frps30 else 0.0
    max30 = max(frps30) if frps30 else 0.0
    denom = d30 + 1
    nightfrac = (night30 + (1 if h["daynight"] == "N" else 0)) / denom
    return {
        "d24": d24, "d7": d7, "d30": d30, "active30": len(active_dates),
        "mean7": mean7, "mean30": mean30, "max30": max30,
        "nightfrac": nightfrac, "persistence": min(1.0, d7 / PERSIST_DENOM),
    }


def facility_context(h, fac_by_cell, width=None):
    if width is None:
        width = max(1, min(int(math.ceil(
            BUFFER_M / (KM_PER_DEG * CELL_STEP))), 6))
    best = None
    inside = False
    for c in cells_near(cell_key(h["lat"], h["lon"]), width):
        for f in fac_by_cell.get(c, ()):
            if f["rings"]:
                if point_in_rings(h["lat"], h["lon"], f["rings"]):
                    inside = True
                    d = 0.0
                else:
                    d = min(point_to_ring_m(h["lat"], h["lon"], r)
                            for r in f["rings"])
            else:
                d = haversine_m(h["lat"], h["lon"], f["lat"], f["lon"])
            if inside or d <= BUFFER_M:
                if best is None or d < best[0]:
                    best = (d, f)
    return best, inside


def main():
    with psycopg.connect(**conn_info()) as conn:
        ids = candidate_ids(conn)
        print(f"Candidates to enrich: {_fmt(len(ids))}")
        if not ids:
            print("Nothing to do.")
            return

        hs = load_hotspots(conn)
        print(f"Hotspots loaded in memory: {_fmt(len(hs))}")
        grid = {}
        for i, h in enumerate(hs.values()):
            grid.setdefault(cell_key(h["lat"], h["lon"]), []).append(h)

        fac = load_facilities(conn)
        print(f"Facilities loaded: {_fmt(len(fac))}")
        fac_by_cell = {}
        for f in fac:
            fac_by_cell.setdefault(cell_key(f["lat"], f["lon"]), []).append(f)

        updates = []
        class_counts = {}
        done = 0
        for hid in ids:
            h = hs.get(hid)
            if h is None:
                continue
            tm = temporal_metrics(h, hs, grid)
            best, inside = facility_context(h, fac_by_cell)
            if best:
                dist, f = best
                within = bool(inside or dist <= 0)
            else:
                dist, f, within = None, None, False

            hs_dict = {"frp": h["frp"], "daynight": h["daynight"]}
            en = {
                "nearest_facility_id": f["id"] if f else None,
                "nearest_facility_name": f["name"] if f else None,
                "nearest_facility_type": f["type"] if f else None,
                "distance_m": round(dist, 1) if dist is not None else None,
                "within_facility": within,
                "detections_24h": tm["d24"],
                "detections_7d": tm["d7"],
                "detections_30d": tm["d30"],
                "days_active_30d": tm["active30"],
                "mean_frp_7d": round(tm["mean7"], 2),
                "mean_frp_30d": round(tm["mean30"], 2),
                "max_frp_30d": round(tm["max30"], 2),
                "current_frp_vs_historical_mean":
                    round(h["frp"] / max(tm["mean30"], 0.1), 2)
                    if h["frp"] is not None else None,
                "nighttime_detection_fraction": round(tm["nightfrac"], 3),
                "persistence_score": round(tm["persistence"], 2),
                "frp_vs_nearby":
                    round(h["frp"] / max(tm["mean7"], 0.1), 2)
                    if h["frp"] is not None else None,
            }
            label, reasons = classifier.classify(hs_dict, en)
            en.update({"hotspot_id": hid, "class": label, "reasons": reasons})
            updates.append(en)
            class_counts[label] = class_counts.get(label, 0) + 1
            done += 1
            if done % 2000 == 0:
                print(f"  classified {_fmt(done)}/{_fmt(len(ids))}")

        print(f"Classified {_fmt(len(updates))} rows; "
              f"upserting in chunks ...")
        UPSERT = """
        INSERT INTO hotspot_enrichment (
            hotspot_id, nearest_facility_id, nearest_facility_name,
            nearest_facility_type, distance_m, within_facility,
            detections_24h, detections_7d, detections_30d, days_active_30d,
            mean_frp_7d, mean_frp_30d, max_frp_30d,
            current_frp_vs_historical_mean, nighttime_detection_fraction,
            persistence_score, frp_vs_nearby, class, reasons, enriched_at
        )
        SELECT w.hotspot_id, w.nearest_facility_id, w.nearest_facility_name,
               w.nearest_facility_type, w.distance_m, w.within_facility,
               w.detections_24h, w.detections_7d, w.detections_30d,
               w.days_active_30d, w.mean_frp_7d, w.mean_frp_30d, w.max_frp_30d,
               w.current_frp_vs_historical_mean,
               w.nighttime_detection_fraction, w.persistence_score,
               w.frp_vs_nearby, w.class, w.reasons, NOW()
        FROM jsonb_to_recordset(%(rows)s::jsonb) AS w(
            hotspot_id bigint,
            nearest_facility_id bigint,
            nearest_facility_name text,
            nearest_facility_type text,
            distance_m double precision,
            within_facility boolean,
            detections_24h int, detections_7d int, detections_30d int,
            days_active_30d int,
            mean_frp_7d real, mean_frp_30d real, max_frp_30d real,
            current_frp_vs_historical_mean real,
            nighttime_detection_fraction real,
            persistence_score real, frp_vs_nearby real,
            class text, reasons jsonb
        )
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
            class                 = EXCLUDED.class,
            reasons               = EXCLUDED.reasons,
            enriched_at           = NOW()
        """
        with conn.cursor() as cur:
            for start in range(0, len(updates), 5000):
                chunk = updates[start:start + 5000]
                cur.execute(UPSERT, {"rows": json.dumps(chunk)})
                affected = cur.rowcount
                conn.commit()
                print(f"  upserted {_fmt(affected)} (chunk at {start})")

        print("Class counts (newly processed):")
        for k, v in sorted(class_counts.items()):
            print(f"  {k:>28}: {_fmt(v)}")


if __name__ == "__main__":
    main()