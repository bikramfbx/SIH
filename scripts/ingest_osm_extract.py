"""Offline OSM extract ingestion for industrial facilities.

Reduces dependence on the public Overpass endpoint when running in restricted
networks. Accepts either an OSM XML file (``.osm``/``.xml``) or an OSM PBF
extract (``.osm.pbf``/``.pbf``) and loads the same objects the live Overpass
query selects (see ``ingest_osm.selectors_match``) into ``industrial_facilities``,
reusing ``ingest_osm.upsert_facilities`` so behaviour is identical.

Usage
    python scripts/ingest_osm_extract.py [--source osm/extract] file.osm.pbf
    python scripts/ingest_osm_extract.py [--source osm/extract] file.osm

    Reads POSTGRES_* env vars (same as ingest_osm.py). PostGIS-created tables
    must exist in the target database (init.sql) -- the upsert uses
    ON CONFLICT (osm_type, osm_id), so rerunning the same extract is
    idempotent and only updates changed tags/geometry.

Notes / limitations
    * Relations (multipolygon outer rings) are skipped, matching the PBF
      decoder's documented constraint. Closed ways carry the vast majority of
      industrial polygons in regional extracts; multipolygon-only sites can be
      loaded via the live Overpass path when a public endpoint is reachable.
    * The India-wide PBF (~150 MB) was not downloaded/validated in the build
      sandbox because external OSM hosts were unreachable there. A real
      Geofabrik extract run is a documented follow-up.
"""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET

import psycopg
from dotenv import load_dotenv

from ingest_osm import normalize_facility_type, selectors_match, upsert_facilities
from osm_pbf import build_way_geometry, iter_pbf


def _facility_from(elem, tags, wkt):
    return {
        "osm_type": elem["type"],
        "osm_id": elem["id"],
        "name": tags.get("name"),
        "facility_type": normalize_facility_type(tags),
        "tags": tags,
        "wkt": wkt or "",
        "lon": elem.get("lon", 0.0),
        "lat": elem.get("lat", 0.0),
    }


def _collect_from_pbf(path):
    """Yield facility dicts from a .osm.pbf file."""
    node_coords = {}
    pending_ways = []
    skipped = {"relations": 0, "nodes": 0, "ways": 0}

    for el in iter_pbf(path, stats=skipped):
        tags = el.get("tags", {})
        if el["type"] == "node":
            # Every node's coordinates are needed to resolve way geometry,
            # not just tagged or industrial ones.
            node_coords[el["id"]] = (el["lat"], el["lon"])
            if selectors_match(tags):
                pending_ways.append({"elem": el, "tags": tags})
        elif el["type"] == "way":
            pending_ways.append({"elem": el, "tags": tags})

    for item in pending_ways:
        elem, tags = item["elem"], item["tags"]
        if not selectors_match(tags):
            continue
        if elem["type"] == "way":
            wkt, lat, lon = build_way_geometry(elem, node_coords)
        else:
            wkt, lat, lon = f"POINT({elem['lon']:.7f} {elem['lat']:.7f})", \
                elem["lat"], elem["lon"]
        if wkt is None:
            skipped["ways"] += 1
            continue
        e = dict(elem, lat=lat, lon=lon)
        yield _facility_from(e, tags, wkt)
    print(f"  pbf skipped: {skipped}", file=sys.stderr)


def _collect_from_xml(path):
    """Yield facility dicts from an OSM XML file (v0.6 or legacy)."""
    node_coords = {}
    tagged_nodes = []
    ways = []
    for event, el in ET.iterparse(path, events=("end",)):
        tag = el.tag
        if tag == "node":
            try:
                lat, lon = float(el.get("lat")), float(el.get("lon"))
                nid = int(el.get("id"))
            except (TypeError, ValueError):
                el.clear()
                continue
            node_coords[nid] = (lat, lon)
            children_tags = {}
            for child in el:
                if child.tag == "tag":
                    children_tags[child.get("k")] = child.get("v")
            if children_tags:
                tagged_nodes.append((nid, lat, lon, children_tags))
            el.clear()
        elif tag == "way":
            refs = []
            tags = {}
            for child in el:
                if child.tag == "nd":
                    try:
                        refs.append(int(child.get("ref")))
                    except (TypeError, ValueError):
                        pass
                elif child.tag == "tag":
                    tags[child.get("k")] = child.get("v")
            ways.append((int(el.get("id")), refs, tags))
            el.clear()

    for facility in _collect_tagged_node_facilities(tagged_nodes):
        yield facility

    for wid, refs, tags in ways:
        if not selectors_match(tags):
            continue
        way = {"type": "way", "id": wid, "refs": refs, "tags": tags}
        wkt, lat, lon = build_way_geometry(way, node_coords)
        if wkt is None:
            continue
        yield _facility_from(way, tags, wkt)


def _collect_tagged_node_facilities(node_list):
    """Emit point facilities for tagged standalone nodes (XML path)."""
    for nid, lat, lon, tags in node_list:
        if not tags or not selectors_match(tags):
            continue
        elem = {"type": "node", "id": nid, "lat": lat, "lon": lon, "tags": tags}
        wkt = f"POINT({lon:.7f} {lat:.7f})"
        yield _facility_from(elem, tags, wkt)
    return


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    source = "osm/extract"
    if argv and argv[0] == "--source":
        source = argv[1]
        argv = argv[2:]
    if len(argv) != 1:
        print("usage: ingest_osm_extract.py [--source osm/extract] <file>")
        return 2
    path = argv[0]
    lower = path.lower()
    if lower.endswith((".pbf", ".osm.pbf")):
        iter_facilities = lambda: _collect_from_pbf(path)  # noqa: E731
    elif lower.endswith((".osm", ".xml")):
        iter_facilities = lambda: _collect_from_xml(path)  # noqa: E731
    else:
        print("ERROR: unknown extract type; use .osm.pbf/.pbf or .osm/.xml",
              file=sys.stderr)
        return 2

    load_dotenv()
    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    facilities = list(iter_facilities())
    print(f"matched facilities from {path}: {len(facilities)}")
    if not facilities:
        print("nothing to upsert (no industrial objects matched)")
        return 0

    with psycopg.connect(**conn_info) as conn:
        total, inserted, updated, skipped = upsert_facilities(
            conn, facilities, source=source)

    print("\n--- Extract Ingestion Summary ---")
    print(f"facilities matched:  {total}")
    print(f"inserted:            {inserted}")
    print(f"updated:             {updated}")
    print(f"skipped (db error):  {skipped}")
    print(f"source:              {source}")
    return 0


if __name__ == "__main__":
    sys.exit(main())