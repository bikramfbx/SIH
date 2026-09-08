"""Phase 3: OSM / Overpass industrial infrastructure ingestion.

Downloads relevant industrial infrastructure from OpenStreetMap via the
Overpass API and stores it in the ``industrial_facilities`` table.

PIPELINE
    OSM / Overpass -> industrial infrastructure -> normalized facility type
    -> PostGIS storage (industrial_facilities)

OSM TAG SET REQUESTED
    The Overpass query unions the following combinations across
    nodes, ways, and relations (see ``SELECTED_TAGS``):
      * landuse=industrial
      * man_made=works
      * power=plant
      * industrial=*          (any subtag: refinery, oil, gas, petrochemical,
                               steel, mine, ...)
      * landuse=quarry

    Supporting tags captured in ``tags`` (raw JSONB) for later use:
      name, operator, product, substance, industrial, landuse, man_made,
      power, plant:source.

    Retail fuel stations (amenity=fuel) are intentionally NOT requested.

NORMALIZATION (see ``normalize_facility_type``)
      power=plant + plant:source in {coal,gas,oil,thermal,nuclear}
                                   -> thermal_power_plant
      industrial=refinery          -> refinery
      industrial in {oil,gas,oil_and_gas,oil_gas}
                                   -> oil_gas_facility
      industrial in {petrochemical,chemical,chemistry}
                                   -> petrochemical
      industrial in {steel,metal,scrap_metal,metallurgical}
                                   -> steel_or_metal
      industrial in {mine,mining} or landuse=quarry -> mining
      product/substance/industrial matches LNG/LPG/gas -> lng_or_storage_terminal
      landuse=industrial           -> industrial_area
      man_made=works               -> general_factory
      power=plant (no recognized source) -> thermal_power_plant
      anything else industrial     -> unknown_industrial

    Raw tags are always preserved in ``tags`` so normalization can be
    improved later without re-downloading source data.

GEOMETRY
    * nodes      -> POINT
    * closed ways/relations -> POLYGON
    * open ways  -> LINESTRING
    A representative ``location`` point is derived with ST_PointOnSurface
    (or the node's own coordinates) for meter-based distance queries later.
    Objects with unusable geometry are skipped rather than stored broken.

IDEMPOTENCY
    Uniqueness is on (osm_type, osm_id). Re-running updates existing rows in
    place (name, facility_type, tags, geometry, updated_at); no duplicates.

NATIONWIDE PRODUCTION NOTE
    A single public Overpass request over the full India-scale default bbox
    (68,6,98,38) is too large and the free public mirrors are frequently
    overloaded. For nationwide production, prefer one of:
      * controlled tiled / regional queries (OSM_CHUNKS / smaller OSM_BBOX), or
      * an India OSM extract (e.g. Geofabrik osm.pbf) loaded into PostGIS,
    rather than relying on a single public Overpass request.
"""

import json
import os
import sys
import time

import psycopg
import requests
from dotenv import load_dotenv

# Mirrors are tried in order until one returns a parseable response.
# `overpass-api.de` is the long-lived reference endpoint and is tried before
# the community mirrors, which are frequently overloaded or hang for seconds
# to minutes. The `DEFAULT_TIMEOUT` bounds per-request HTTP read time so a
# stuck mirror does not stall the whole retry loop.
OVERPASS_URLS = [
    u
    for u in (
        os.getenv("OVERPASS_URL"),
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.private.coffee/api/interpreter",
    )
    if u
]

USER_AGENT = "PyroSphere-Industrial-Ingest/0.1 (+https://github.com/bikramfbx/pyrosphere)"

DEFAULT_BBOX = "68,6,98,38"
DEFAULT_CHUNKS = 1
DEFAULT_TIMEOUT = 60
CONNECT_TIMEOUT = 10
MAX_RETRIES = 3
BACKOFF_BASE = 5

SELECTED_TAGS = [
    'node["landuse"="industrial"]',
    'way["landuse"="industrial"]',
    'relation["landuse"="industrial"]',
    'node["man_made"="works"]',
    'way["man_made"="works"]',
    'relation["man_made"="works"]',
    'node["power"="plant"]',
    'way["power"="plant"]',
    'relation["power"="plant"]',
    'node["industrial"]',
    'way["industrial"]',
    'relation["industrial"]',
    'node["landuse"="quarry"]',
    'way["landuse"="quarry"]',
    'relation["landuse"="quarry"]',
]


def parse_bbox(bbox):
    try:
        west, south, east, north = (float(v) for v in bbox.split(","))
    except (ValueError, AttributeError) as e:
        raise ValueError(
            f"bbox must be 'west,south,east,north', got {bbox!r}"
        ) from e
    if not (-180 <= west <= 180) or not (-180 <= east <= 180):
        raise ValueError(f"west/east must be within [-180, 180], got {bbox!r}")
    if not (-90 <= south <= 90) or not (-90 <= north <= 90):
        raise ValueError(f"south/north must be within [-90, 90], got {bbox!r}")
    if not (west < east):
        raise ValueError(f"west must be < east, got {bbox!r}")
    if not (south < north):
        raise ValueError(f"south must be < north, got {bbox!r}")
    return west, south, east, north


def to_overpass_bbox(bbox):
    west, south, east, north = parse_bbox(bbox)
    return f"{south:.6f},{west:.6f},{north:.6f},{east:.6f}"


def build_query(bbox):
    bbox_str = f"({to_overpass_bbox(bbox)})"
    queries = "".join(f"  {tag}{bbox_str};\n" for tag in SELECTED_TAGS)
    return (
        "[out:json][timeout:60];\n"
        "(\n"
        f"{queries}"
        ");\n"
        "out body geom;\n"
    )


def area_tiles(bbox, chunks):
    west, south, east, north = parse_bbox(bbox)
    if chunks <= 1:
        return [f"{west},{south},{east},{north}"]
    lat_step = (north - south) / chunks
    lon_step = (east - west) / chunks
    tiles = []
    for i in range(chunks):
        for j in range(chunks):
            w = west + j * lon_step
            e = west + (j + 1) * lon_step
            s = south + i * lat_step
            n = south + (i + 1) * lat_step
            tiles.append(f"{w:.6f},{s:.6f},{e:.6f},{n:.6f}")
    return tiles


def query_overpass(query, retries=MAX_RETRIES, timeout=DEFAULT_TIMEOUT):
    last_err = None
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries):
        for url in OVERPASS_URLS:
            try:
                resp = requests.post(
                    url,
                    data={"data": query},
                    timeout=(CONNECT_TIMEOUT, timeout),
                    headers=headers,
                )
                if resp.status_code == 400:
                    body = " ".join(resp.text.split())[:200]
                    raise RuntimeError(
                        f"Overpass returned HTTP 400 (malformed request): {body}"
                    )
                if resp.status_code in (429, 502, 504, 406):
                    body = " ".join(resp.text.split())[:120]
                    print(
                        f"  {url} -> {resp.status_code} ({body}); trying next",
                        file=sys.stderr,
                    )
                    last_err = f"HTTP {resp.status_code}"
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                last_err = e
                print(f"  {url} error ({e}); trying next", file=sys.stderr)
        sleep = BACKOFF_BASE * (2 ** attempt)
        print(f"  all endpoints failed on attempt {attempt+1}; retrying in {sleep}s", file=sys.stderr)
        time.sleep(sleep)
    raise RuntimeError(f"Overpass query failed after {retries} retries: {last_err}")


def coords_to_wkt(coords, closed=False):
    if not coords:
        return None
    pts = "".join(f"{lon:.7f} {lat:.7f}," for lon, lat in coords).rstrip(",")
    return f"LINESTRING({pts})"


def ring_to_polygon_wkt(coords):
    if not coords or len(coords) < 4:
        return None
    first = (coords[0][0], coords[0][1])
    last = (coords[-1][0], coords[-1][1])
    ring = list(coords)
    if first != last:
        ring.append(first)
    pts = "".join(f"{lon:.7f} {lat:.7f}," for lon, lat in ring).rstrip(",")
    return f"POLYGON(({pts}))"


def geom_wkt(elem):
    etype = elem.get("type")
    if etype == "node":
        if "lat" in elem and "lon" in elem:
            return f"POINT({elem['lon']:.7f} {elem['lat']:.7f})"
        return None
    if etype in ("way", "relation"):
        coords = [(e["lon"], e["lat"]) for e in elem.get("geometry", [])]
        if not coords or len(coords) < 2:
            return None
        if coords[0] == coords[-1]:
            return ring_to_polygon_wkt(coords) or coords_to_wkt(coords)
        return coords_to_wkt(coords, closed=False)
    return None


def normalize_facility_type(tags):
    industrial = tags.get("industrial")
    landuse = tags.get("landuse")
    man_made = tags.get("man_made")
    power = tags.get("power")
    plant_source = tags.get("plant:source", "").lower()
    product = tags.get("product", "").lower()
    substance = tags.get("substance", "").lower()

    if (power == "plant") and any(
        s in plant_source for s in ("coal", "gas", "oil", "thermal", "nuclear")
    ):
        return "thermal_power_plant"
    if industrial == "refinery":
        return "refinery"
    if industrial in ("oil", "gas", "oil_and_gas", "oil_gas"):
        return "oil_gas_facility"
    if industrial in ("petrochemical", "chemical", "chemistry"):
        return "petrochemical"
    if industrial in ("steel", "metal", "scrap_metal", "metallurgical"):
        return "steel_or_metal"
    if industrial in ("mine", "mining") or landuse == "quarry":
        return "mining"
    if any(s in ("lng", "lpg", "gas") for s in (kind for kind in (product, substance, industrial) if kind)):
        return "lng_or_storage_terminal"
    if landuse == "industrial":
        return "industrial_area"
    if man_made == "works":
        return "general_factory"
    if power == "plant":
        return "thermal_power_plant"
    if industrial:
        return "unknown_industrial"
    return "unknown_industrial"


def selectors_match(tags):
    """Replicate the Overpass selector list on a raw dict of OSM tags.

    Mirrors SELECTED_TAGS so an offline extract (ingest_osm_extract.py)
    selects exactly the same objects the live Overpass query would.
    """
    industrial = tags.get("industrial")
    landuse = tags.get("landuse")
    man_made = tags.get("man_made")
    power = tags.get("power")
    if industrial:
        return True
    if landuse == "industrial":
        return True
    if man_made == "works":
        return True
    if power == "plant":
        return True
    if landuse == "quarry":
        return True
    return False


def valid_osm(elem):
    return elem.get("type") in ("node", "way", "relation") and isinstance(
        elem.get("id"), int
    )


def upsert_facilities(conn, facilities, source="osm/overpass"):
    upsert_sql = """
    INSERT INTO industrial_facilities (
        osm_type, osm_id, name, facility_type, tags, source,
        geometry, location
    ) VALUES (
        %(osm_type)s, %(osm_id)s, %(name)s, %(facility_type)s, %(tags)s,
        %(source)s,
        NULLIF(%(wkt)s, '')::geometry,
        CASE
            WHEN NULLIF(%(wkt)s, '') IS NOT NULL
                THEN ST_PointOnSurface(NULLIF(%(wkt)s, '')::geometry)::geography
            ELSE ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography
        END
    )
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
    total = len(facilities)
    inserted = 0
    updated = 0
    skipped_invalid = 0

    with conn.cursor() as cur:
        for f in facilities:
            try:
                cur.execute(
                    upsert_sql,
                    {
                        "osm_type": f["osm_type"],
                        "osm_id": f["osm_id"],
                        "name": f["name"],
                        "facility_type": f["facility_type"],
                        "tags": json.dumps(f["tags"]),
                        "source": f.get("source", source),
                        "wkt": f["wkt"],
                        "lon": f["lon"],
                        "lat": f["lat"],
                    },
                )
                row = cur.fetchone()
                was_inserted = True if row is None else row[0]
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1
            except psycopg.Error as e:
                conn.rollback()
                skipped_invalid += 1
                print(
                    f"  db error upserting {f['osm_type']}/{f['osm_id']}: {e}",
                    file=sys.stderr,
                )
    return total, inserted, updated, skipped_invalid


def main():
    load_dotenv()

    bbox = os.getenv("OSM_BBOX", DEFAULT_BBOX)
    chunks = int(os.getenv("OSM_CHUNKS", DEFAULT_CHUNKS))

    try:
        parse_bbox(bbox)
    except ValueError as e:
        print(f"ERROR: invalid OSM_BBOX: {e}", file=sys.stderr)
        sys.exit(1)

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    tiles = area_tiles(bbox, chunks)
    print(f"Querying Overpass: bbox={bbox}, tiles={len(tiles)}")

    total_returned = 0
    total_inserted = 0
    total_updated = 0
    total_invalid = 0

    with psycopg.connect(**conn_info) as conn:
        for tile in tiles:
            query = build_query(tile)
            print(f"\n--- tile {tile} ---")
            try:
                data = query_overpass(query)
            except RuntimeError as e:
                print(f"ERROR: {e}", file=sys.stderr)
                print(
                    "Overpass is unreachable. This is an external-service "
                    "limitation; no tile data was committed for this run.",
                    file=sys.stderr,
                )
                sys.exit(2)
            elems = data.get("elements", [])
            total_returned += len(elems)

            facilities = []
            for elem in elems:
                if not valid_osm(elem):
                    total_invalid += 1
                    continue
                tags = elem.get("tags", {})
                wkt = geom_wkt(elem)
                if not wkt:
                    total_invalid += 1
                    continue
                lon = elem.get("lon", 0.0)
                lat = elem.get("lat", 0.0)
                facilities.append(
                    {
                        "osm_type": elem["type"],
                        "osm_id": elem["id"],
                        "name": tags.get("name"),
                        "facility_type": normalize_facility_type(tags),
                        "tags": tags,
                        "wkt": wkt,
                        "lon": lon,
                        "lat": lat,
                    }
                )

            n, ins, upd, inv = upsert_facilities(conn, facilities)
            total_inserted += ins
            total_updated += upd
            total_invalid += inv
            time.sleep(1)

    print(f"\n--- Ingestion Summary ---")
    print(f"OSM objects returned:  {total_returned}")
    print(f"Facilities inserted:   {total_inserted}")
    print(f"Facilities updated:    {total_updated}")
    print(f"Skipped (invalid):     {total_invalid}")


if __name__ == "__main__":
    main()
