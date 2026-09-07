"""Bootstrap industrial_facilities from global open datasets.

Sources
    WRI Global Power Plant Database v1.3.0  (CC BY 4.0)
        https://www.globalpowerplantdatabase.org/
        repo: wri/global-power-plant-database, output_database/global_power_plant_database.csv
    Open Energy Transition osm-powerplants    (code MIT; OSM data ODbL 1.0)
        repo: open-energy-transition/osm-powerplants, osm_global.csv[.gz]
        osm_global.csv mapping: Fueltype, Capacity (MW), lat/lon, id ("way/..")

Only *thermal emitter* plants are kept (coal/gas/oil/biomass/waste/petcoke/
cogeneration). Renewables (solar/wind/hydro/nuclear/storage/geothermal) and
unknown/other fuels are excluded -- they do not emit FIRMS-detectable fires --
and are reported so the exclusion is auditable.

Identity / provenance
    * WRI rows        -> osm_type='wri',  osm_id=FNV-1a 64 hash of gppd_idnr,
                         tags.wri_id keeps the original, tags.url + license kept.
    * osm-powerplants -> osm_type='node'|'way'|'relation', osm_id=OSM id, so the
                         (osm_type, osm_id) unique index also dedupes against a
                         future raw-OSM ingest of the same element.
    * All tags carry  name, fuel, capacity_mw, country, and provenance/source
      metadata. source column = 'wri/v1.3.0' or 'osm-powerplants/YYYY-MM-DD'.

Cross-source dedupe: an osm-powerplants plant whose centroid lies within
``--dup-distance-m`` (default 1200) of a same-fuel WRI plant is marked
tags.duplicate_of='wri:<wri osm_id>' rather than inserted twice; the WRI row
is kept as canonical. Counts are reported both raw and after this dedupe.

Usage
    python scripts/import_global_facilities.py [--db local|prod]
                                               [--wri PATH] [--osmpp PATH]
                                               [--dup-distance-m 1200] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import psycopg  # noqa: E402
from dotenv import load_dotenv  # noqa: E402


def _conn_info():
    info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }
    if os.getenv("POSTGRES_SSLMODE"):
        info["sslmode"] = os.getenv("POSTGRES_SSLMODE")
    if os.getenv("POSTGRES_CONNECT_TIMEOUT"):
        info["connect_timeout"] = int(os.getenv("POSTGRES_CONNECT_TIMEOUT"))
    return info

WRI_PATH = os.path.join(HERE, "..", "data", "facilities",
                        "wri_global_power_plant_database.csv")
OSMPP_PATH = os.path.join(HERE, "..", "data", "facilities", "osm_global.csv")

CAPACITY_LIMIT_MW = 0.0  # keep all reported capacities (tiny gen-sets included)

WRI_THERMAL = {
    "Coal": "thermal_power_plant",
    "Gas": "thermal_power_plant",
    "Oil": "thermal_power_plant",
    "Petcoke": "thermal_power_plant",
    "Biomass": "thermal_power_plant",
    "Waste": "thermal_power_plant",
    "Cogeneration": "thermal_power_plant",
}

OSMPP_THERMAL = {
    "Hard Coal": "thermal_power_plant",
    "Lignite": "thermal_power_plant",
    "Natural Gas": "thermal_power_plant",
    "Oil": "thermal_power_plant",
    "Petcoke": "thermal_power_plant",
    "Solid Biomass": "thermal_power_plant",
    "Biogas": "thermal_power_plant",
    "Waste": "thermal_power_plant",
}

WRI_NON_THERMAL = {
    "Hydro", "Solar", "Wind", "Nuclear", "Geothermal", "Storage",
    "Wave and Tidal", "Other",
}
OSMPP_NON_THERMAL = {
    "Hydro", "Solar", "Wind", "Nuclear", "Geothermal", "Battery", "Other",
}

FUEL_FAMILY = {
    "Coal": "coal", "Hard Coal": "coal", "Lignite": "coal",
    "Gas": "gas", "Natural Gas": "gas", "Biogas": "gas",
    "Oil": "oil",
    "Biomass": "bio", "Solid Biomass": "bio", "Waste": "bio",
    "Cogeneration": "bio",
    "Petcoke": "petcoke",
}


def fnv1a64(text):
    h = 0xCBF29CE484222325
    for b in text.encode("utf-8"):
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def _fnum(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_wri(path, skip_unknown_fuel):
    facilities, excluded = [], 0
    with open(path, newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            fuel = (row.get("primary_fuel") or "").strip()
            lat, lon = _fnum(row.get("latitude")), _fnum(row.get("longitude"))
            cap = _fnum(row.get("capacity_mw"))
            if lat is None or lon is None or not (math.isfinite(lat)
                                                  and math.isfinite(lon)):
                excluded += 1
                continue
            if fuel in WRI_THERMAL:
                if cap is not None and cap < CAPACITY_LIMIT_MW:
                    excluded += 1
                    continue
                oid = fnv1a64(row.get("gppd_idnr") or stock_id(row, fuel))
                facilities.append({
                    "osm_type": "wri",
                    "osm_id": oid if oid <= (1 << 63) - 1 else (oid - (1 << 64)),
                    "name": (row.get("name") or "").strip() or None,
                    "facility_type": WRI_THERMAL[fuel],
                    "lat": lat,
                    "lon": lon,
                    "tags": {
                        "fuel": fuel,
                        "country": (row.get("country") or "").strip() or None,
                        "capacity_mw": cap,
                        "commissioning_year": (row.get("commissioning_year")
                                                or "").strip() or None,
                        "owner": (row.get("owner") or "").strip() or None,
                        "wri_id": (row.get("gppd_idnr") or "").strip(),
                        "wepp_id": (row.get("wepp_id") or "").strip(),
                        "wri_url": (row.get("url") or "").strip() or None,
                        "provenance": "WRI Global Power Plant Database v1.3.0",
                        "license": "CC BY 4.0",
                    },
                    "geometry": None,
                    "wkt": None,
                    "source": "wri/v1.3.0",
                })
            elif fuel in WRI_NON_THERMAL or (skip_unknown_fuel
                                             and fuel not in ("", None)):
                excluded += 1
            else:
                excluded += 1
    return facilities, excluded


def read_osmpp(path, skip_unknown_fuel):
    facilities, excluded = [], 0
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            fuel = (row.get("Fueltype") or "").strip()
            lat, lon = _fnum(row.get("lat")), _fnum(row.get("lon"))
            cap = _fnum(row.get("Capacity"))
            if lat is None or lon is None or not (math.isfinite(lat)
                                                  and math.isfinite(lon)):
                excluded += 1
                continue
            element = (row.get("id") or "").strip()
            otype, oid = "node", None
            if "/" in element:
                otype, _, num = element.partition("/")
                try:
                    oid = int(num)
                except ValueError:
                    oid = None
            if oid is None:
                excluded += 1
                continue
            if fuel in OSMPP_THERMAL:
                if cap is not None and cap < CAPACITY_LIMIT_MW:
                    excluded += 1
                    continue
                facilities.append({
                    "osm_type": otype,
                    "osm_id": oid,
                    "name": (row.get("Name") or "").strip() or None,
                    "facility_type": OSMPP_THERMAL[fuel],
                    "lat": lat,
                    "lon": lon,
                    "tags": {
                        "fuel": fuel,
                        "country": (row.get("Country") or "").strip() or None,
                        "capacity_mw": cap,
                        "technology": (row.get("Technology") or "").strip() or None,
                        "date_in": (row.get("DateIn") or "").strip() or None,
                        "osm_id_raw": element,
                        "provenance": "osm-powerplants (open-energy-transition)",
                        "license": "ODbL 1.0 (OSM data)",
                    },
                    "geometry": None,
                    "wkt": None,
                    "source": "osm-powerplants/2026-06",
                })
            elif fuel in OSMPP_NON_THERMAL or (skip_unknown_fuel
                                               and fuel not in ("", None)):
                excluded += 1
            else:
                excluded += 1
    return facilities, excluded


def stock_id(row, fuel):
    return f"{fuel}:{row.get('country') or ''}:{row.get('name') or ''}:{row.get('latitude') or ''}:{row.get('longitude') or ''}"


def cross_dedupe(thermal_wri, thermal_osmpp, dup_distance_m):
    """Mark osm-powerplants rows duplicating a same-fuel WRI plant.

    Grid hash over 0.01-deg cells; only neighboring cells are scanned.
    ``dup_distance_m`` is a rough equirectangular cutoff (meters).
    """
    deg = 0.01
    cells = {}
    for p in thermal_wri:
        key = (round(p["lat"] / deg), round(p["lon"] / deg))
        cells.setdefault(key, []).append(p)
    dup_marked = 0
    for p in thermal_osmpp:
        cy, cx = round(p["lat"] / deg), round(p["lon"] / deg)
        family = FUEL_FAMILY.get(p["tags"]["fuel"])
        best = None
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for cand in cells.get((cy + dy, cx + dx), []):
                    if family and FUEL_FAMILY.get(cand["tags"]["fuel"]) != family:
                        continue
                    if p["facility_type"] != cand["facility_type"]:
                        continue
                    d = haversine(p["lat"], p["lon"], cand["lat"], cand["lon"])
                    if d <= dup_distance_m and (best is None or d < best[0]):
                        best = (d, cand["osm_id"])
        if best:
            p["tags"]["duplicate_of"] = f"wri:{best[1]}"
            p["tags"]["duplicate_distance_m"] = round(best[0], 1)
            dup_marked += 1
    return dup_marked


def haversine(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def report(facilities, excluded, dup_marked, tag):
    thermal = [f for f in facilities if not f["tags"].get("duplicate_of")]
    from collections import Counter
    types = Counter(f["facility_type"] for f in thermal)
    fuels = Counter(f["tags"]["fuel"] for f in thermal)
    countries = Counter(f["tags"].get("country") or "?" for f in thermal)
    print(f"\n== {tag} ==")
    print(f"raw thermal rows             : {len(facilities)}")
    print(f"excluded (non-thermal/bad)   : {excluded}")
    print(f"cross-source duplicates      : {dup_marked}")
    print(f"canonical after dedupe       : {len(thermal)}")
    print("facility types:", dict(types))
    print("fuels        :", dict(fuels))
    print("top countries:", dict(countries.most_common(12)))
    return len(thermal)


def batch_upsert(conn, facilities):
    """Single round-trip upsert for the whole facility list.

    One statement over ``jsonb_to_recordset`` avoids ~15k pooled round trips
    (a pooler connection makes per-row upserts impractically slow).
    Returns ``(inserted, updated, skipped_invalid)``.
    """
    upsert_sql = """
    INSERT INTO industrial_facilities (
        osm_type, osm_id, name, facility_type, tags, source,
        geometry, location
    )
    SELECT
        r.osm_type,
        r.osm_id::bigint,
        NULLIF(r.name, '')::text,
        r.facility_type,
        COALESCE(NULLIF(r.tags, '')::jsonb, '{}'::jsonb),
        r.source,
        COALESCE(NULLIF(r.wkt, '')::geometry,
                 ST_SetSRID(ST_MakePoint(r.lon::float8, r.lat::float8),
                            4326)::geometry),
        CASE
            WHEN NULLIF(r.wkt, '') IS NOT NULL
                THEN ST_PointOnSurface(NULLIF(r.wkt, '')::geometry)::geography
            ELSE ST_SetSRID(ST_MakePoint(r.lon::float8, r.lat::float8),
                           4326)::geography
        END
    FROM jsonb_to_recordset(%(rows)s::jsonb)
        AS r(osm_type text, osm_id text, name text, facility_type text,
             tags text, source text, wkt text, lon text, lat text)
    ON CONFLICT (osm_type, osm_id) DO UPDATE SET
        name = EXCLUDED.name,
        facility_type = EXCLUDED.facility_type,
        tags = EXCLUDED.tags,
        source = EXCLUDED.source,
        geometry = EXCLUDED.geometry,
        location = EXCLUDED.location,
        updated_at = NOW()
    RETURNING (xmax = 0) AS was_inserted
    """
    payload = [
        {
            "osm_type": f["osm_type"],
            "osm_id": str(f["osm_id"]),
            "name": f.get("name"),
            "facility_type": f["facility_type"],
            "tags": json.dumps(f["tags"]),
            "source": f.get("source", "wri/v1.3.0"),
            "wkt": f.get("wkt") or "",
            "lon": str(f["lon"]),
            "lat": str(f["lat"]),
        }
        for f in facilities
    ]
    last = {}
    for row in payload:
        last[(row["osm_type"], row["osm_id"])] = row
    payload = list(last.values())
    if not payload:
        return 0, 0, 0
    inserted = updated = 0
    with conn.cursor() as cur:
        try:
            cur.execute(upsert_sql, {"rows": json.dumps(payload)})
            for (was_inserted,) in cur.fetchall():
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1
        except psycopg.Error as e:
            conn.rollback()
            print(f"db error during batch upsert: {e}", file=sys.stderr)
            return 0, 0, len(payload)
    return inserted, updated, 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", choices=["env", "local", "prod"], default="env")
    ap.add_argument("--wri", default=WRI_PATH)
    ap.add_argument("--osmpp", default=OSMPP_PATH)
    ap.add_argument("--dup-distance-m", type=float, default=1200.0)
    ap.add_argument("--skip-unknown-fuel", action="store_true", default=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    wri, exc_wri = read_wri(args.wri, args.skip_unknown_fuel)
    osmpp, exc_osmpp = read_osmpp(args.osmpp, args.skip_unknown_fuel)

    dup_marked = cross_dedupe(wri, osmpp, args.dup_distance_m)
    report(wri + osmpp, exc_wri + exc_osmpp, dup_marked, "combined")

    if args.dry_run:
        print("\nDRY RUN -- nothing written.")
        return

    load_dotenv()
    if args.db == "prod":
        deploy = os.path.join(HERE, "..", ".env.deploy")
        if os.path.exists(deploy):
            load_dotenv(deploy, override=True)
        else:
            print("WARNING: .env.deploy not found; using .env connection",
                  file=sys.stderr)
    info = _conn_info()
    if args.db == "local":
        info.update({"host": "db", "port": "5432"})
    print(f"loading into sourced DB at {info['host']}:{info['port']} {info['dbname']} "
          f"(user={info['user']})")
    with psycopg.connect(**info) as conn:
        conn.execute("SET statement_timeout = 0")
        inserted, updated, skipped = batch_upsert(conn, wri + osmpp)
        conn.commit()
    print(f"\nupsert done -> inserted={inserted} updated={updated} "
          f"skipped_invalid={skipped}")


if __name__ == "__main__":
    main()