"""Facility context ingestion (MVP Phase 3.5).

For each FIRMS hotspot (sequential, conservative), determine whether its
neighbourhood has already been queried (see ``osm_query_cache``). If not,
run a small Overpass query in a configurable radius around the hotspot and
upsert any industrial facilities/areas found into ``industrial_facilities``,
deduplicating globally on (osm_type, osm_id).

This keeps the demo footprint small: we only fetch industrial context around
actual thermal anomalies instead of every facility nationwide. The schema and
upsert are the same ones used by the nationwide ``ingest_osm.py`` path, so
switching to a global/Geofabrik extract later requires no schema change.

Reuses query construction, Overpass retry/fallback, geometry parsing and
facility normalization from ``ingest_osm.py``.

ENV (all optional)
    OSM_BBOX                       unused here; radius-based
    OSM_CONTEXT_RADIUS_M            radius around each hotspot in meters (default 7000)
    OSM_CONTEXT_MAX_RETRIES         per-hotspot Overpass attempts (default 2)
    OSM_CONTEXT_MAX_FAILURES        hotspots to give up on before aborting (default 10)
    OSM_CONTEXT_MIN_INTERVAL_S      pause between hotspot queries (default 1.0)
    OSM_CONTEXT_CACHE_OVERRIDE_C    radius of a prior cache centre that covers a
                                    hotspot (the hotspot is skipped when it is
                                    within this distance of an already-queried
                                    centre); default = 0 uses the cache radius
"""

import math
import os
import sys
import time

from dotenv import load_dotenv
import psycopg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ingest_osm  # noqa: E402  (reuses query/cache/upsert/normalize logic)

DEFAULT_CONTEXT_RADIUS_M = 7000
DEFAULT_MAX_RETRIES = 2
DEFAULT_MAX_FAILURES = 10
DEFAULT_MIN_INTERVAL_S = 2.0
DEFAULT_CACHE_OVERRIDE_RADIUS_M = 0
DEFAULT_HTTP_TIMEOUT = 25  # bound hung-mirror wait; these are tiny bbox queries

# Rough meters-per-degree at the equator; longitude stretched by cos(lat).
M_PER_DEG_LAT = 111320.0


def radius_to_bbox(center_lat, center_lon, radius_m):
    """Return an Overpass-ready bbox ``(south, west, north, east)`` string."""
    lat_delta = radius_m / M_PER_DEG_LAT
    cos_lat = max(0.2, math.cos(math.radians(center_lat)))
    lon_delta = radius_m / (M_PER_DEG_LAT * cos_lat)
    south = center_lat - lat_delta
    north = center_lat + lat_delta
    west = center_lon - lon_delta
    east = center_lon + lon_delta
    return f"{south:.6f},{west:.6f},{north:.6f},{east:.6f}"


def area_covered(conn, center_lat, center_lon, radius_m):
    """True if an existing cache entry already covers this hotspot centre."""
    if radius_m <= 0:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM osm_query_cache
            WHERE ST_DWithin(
                CAST(ST_SetSRID(ST_MakePoint(%s, %s), 4326) AS geography),
                CAST(ST_SetSRID(ST_MakePoint(center_lon, center_lat), 4326) AS geography),
                GREATEST(radius_m, %s)
            )
            LIMIT 1
            """,
            (center_lon, center_lat, radius_m),
        )
        return cur.fetchone() is not None


def record_query(conn, center_lat, center_lon, radius_m, facility_count):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO osm_query_cache (center_lat, center_lon, radius_m, facility_count)
            VALUES (%s, %s, %s, %s)
            """,
            (center_lat, center_lon, radius_m, facility_count),
        )


def fetch_context_for_hotspot(conn, hot, radius_m, max_retries, http_timeout):
    """Run a small Overpass query around *hot*; upsert found facilities.

    Returns (n_queries, n_returned, n_inserted, n_updated). Raises
    RuntimeError when Overpass is unreachable for this hotspot.
    """
    south, west, north, east = (float(v) for v in radius_to_bbox(
        hot["latitude"], hot["longitude"], radius_m
    ).split(","))
    query = ingest_osm.build_query(f"{west},{south},{east},{north}")
    data = ingest_osm.query_overpass(
        query, retries=max_retries, timeout=http_timeout
    )
    elems = data.get("elements", [])

    facilities = []
    for elem in elems:
        if not ingest_osm.valid_osm(elem):
            continue
        tags = elem.get("tags", {})
        wkt = ingest_osm.geom_wkt(elem)
        if not wkt:
            continue
        facilities.append(
            {
                "osm_type": elem["type"],
                "osm_id": elem["id"],
                "name": tags.get("name"),
                "facility_type": ingest_osm.normalize_facility_type(tags),
                "tags": tags,
                "wkt": wkt,
                "lon": elem.get("lon", 0.0),
                "lat": elem.get("lat", 0.0),
            }
        )

    _, inserted, updated, _ = ingest_osm.upsert_facilities(conn, facilities)
    return 1, len(elems), inserted, updated


def _load_context_config():
    return {
        "radius_m": float(os.getenv("OSM_CONTEXT_RADIUS_M", DEFAULT_CONTEXT_RADIUS_M)),
        "min_interval": float(os.getenv("OSM_CONTEXT_MIN_INTERVAL_S", DEFAULT_MIN_INTERVAL_S)),
        "max_retries": int(os.getenv("OSM_CONTEXT_MAX_RETRIES", DEFAULT_MAX_RETRIES)),
        "max_failures": int(os.getenv("OSM_CONTEXT_MAX_FAILURES", DEFAULT_MAX_FAILURES)),
        "cache_override": float(os.getenv(
            "OSM_CONTEXT_CACHE_OVERRIDE_C", DEFAULT_CACHE_OVERRIDE_RADIUS_M)),
        "http_timeout": float(os.getenv("OSM_HTTP_TIMEOUT", DEFAULT_HTTP_TIMEOUT)),
    }


def process_hotspot_context(conn, hotspots, cfg):
    """Fetch context for hotspots not already covered by the query cache.

    Sequential + conservative (over Overpass's public API); skips cached
    neighbourhoods; stops when ``max_failures`` hotspots fail consecutively.
    Never raises: per-hotspot failures are counted and logged.
    Returns a summary dict.
    """
    radius_m = cfg["radius_m"]
    min_interval = cfg["min_interval"]
    max_retries = cfg["max_retries"]
    max_failures = cfg["max_failures"]
    cache_override = cfg["cache_override"]
    http_timeout = cfg["http_timeout"]
    cover_dist = cache_override if cache_override > 0 else radius_m

    n_queries = n_skipped_cached = n_failed = n_returned = n_inserted = n_updated = 0
    total = len(hotspots)
    for i, (hid, lat, lon) in enumerate(hotspots, 1):
        if area_covered(conn, lat, lon, cover_dist):
            n_skipped_cached += 1
            continue
        try:
            q, ret, ins, upd = fetch_context_for_hotspot(
                conn, {"latitude": lat, "longitude": lon},
                radius_m, max_retries, http_timeout)
            record_query(conn, lat, lon, radius_m, 0)
            conn.commit()
        except RuntimeError as e:
            conn.rollback()
            n_failed += 1
            print(f"[{i}/{total}] hotspot {hid}: Overpass failed ({e}); "
                  f"skipping (consecutive failures {n_failed})", file=sys.stderr)
            if n_failed >= max_failures:
                print(f"WARNING: {max_failures} hotspots failed in a row; "
                      f"stopping context refresh.", file=sys.stderr)
                break
            time.sleep(min_interval)
            continue
        except Exception:
            conn.rollback()
            raise
        n_queries += q
        n_returned += ret
        n_inserted += ins
        n_updated += upd
        print(f"[{i}/{total}] hotspot {hid} ({lat:.4f},{lon:.4f}): "
              f"ret={ret:3d} ins={ins:3d} upd={upd:3d}", flush=True)
        time.sleep(min_interval)

    return {
        "hotspots": total,
        "skipped_cached": n_skipped_cached,
        "queries": n_queries,
        "failed": n_failed,
        "returned": n_returned,
        "inserted": n_inserted,
        "updated": n_updated,
    }


def process_new_hotspot_context(conn, conn_info):
    """Opt-in worker hook: fetch OSM context for new hotspots missing coverage.

    Overpass stays OFF the live path: this is only invoked by worker.py when
    WORKER_OSM_CONTEXT=1, and every neighbourhood already in the query cache is
    skipped (no repeated queries). Returns a summary dict.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT h.id, h.latitude, h.longitude
            FROM hotspots h
            WHERE NOT EXISTS (
                SELECT 1 FROM osm_query_cache c
                WHERE ST_DWithin(
                    h.location,
                    CAST(ST_SetSRID(ST_MakePoint(c.center_lon, c.center_lat), 4326)
                         AS geography),
                    %s
                )
            )
            ORDER BY h.ingested_at DESC
            LIMIT %s
        """, (32000, 50))
        hotspots = cur.fetchall()
    if not hotspots:
        return {"hotspots": 0}
    return process_hotspot_context(conn, hotspots, _load_context_config())


def main():
    load_dotenv()
    cfg = _load_context_config()

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    with psycopg.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, latitude, longitude FROM hotspots ORDER BY acq_date, id"
            )
            hotspots = cur.fetchall()

        print(f"Hotspots to process: {len(hotspots)} "
              f"(radius={cfg['radius_m']:.0f}m, "
              f"max_retries={cfg['max_retries']})")

        summary = process_hotspot_context(conn, hotspots, cfg)

    print("\n--- Facility Context Summary ---")
    print(f"Hotspots in scope:          {summary['hotspots']}")
    print(f"Hotspots skipped (cached):  {summary['skipped_cached']}")
    print(f"Overpass queries run:       {summary['queries']}")
    print(f"Hotspots failed:            {summary['failed']}")
    print(f"OSM objects returned:       {summary['returned']}")
    print(f"Facilities inserted:        {summary['inserted']}")
    print(f"Facilities updated:         {summary['updated']}")


if __name__ == "__main__":
    main()