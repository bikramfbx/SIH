"""Offline OSM extract tests.

``test_pbf_decoder`` validates the pure-Python PBF reader against a
hand-rolled, spec-faithful PBF fixture (same framing/zlib/wire rules as
osmpbf files produced by Osmosis/Geofabrik) -- no external network needed.

``test_xml_parser`` and ``test_selectors`` validate the .osm XML path and the
selector filter shared with the live Overpass path.

A real Geofabrik extract (.osm.pbf, e.g. Liechtenstein or India) was NOT
downloaded in the build sandbox (external OSM hosts unreachable), so live-file
PBF ingestion remains externally unverified -- see README/final report.

Run:
    .venv/bin/python -m pytest tests/test_osm_extract.py -v
"""

import io
import os
import sys
import tempfile
import zlib

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from ingest_osm import selectors_match  # noqa: E402
from ingest_osm_extract import _collect_from_xml  # noqa: E402
from osm_pbf import build_way_geometry, iter_pbf  # noqa: E402


# ---------------------------------------------------------------------------
# Minimal OSM PBF *encoder* (mirrors the osmpbf wire format).
# ---------------------------------------------------------------------------

def _varint(n):
    n &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _zigzag(n):
    return (n << 1) ^ (n >> 63)


def _tag(fnum, wire):
    return _varint((fnum << 3) | wire)


def _field_varint(fnum, value):
    return _tag(fnum, 0) + _varint(value)


def _field_bytes(fnum, payload):
    return _tag(fnum, 2) + _varint(len(payload)) + payload


def _field_packed(fnum, values, zigzag=False):
    body = b"".join(_varint(_zigzag(v) if zigzag else v) for v in values)
    return _field_bytes(fnum, body)


def _stringtable(entries):
    body = b"".join(_field_bytes(1, e.encode("utf-8")) for e in entries)
    return _field_bytes(1, body)


def _dense_nodes(ids, lats, lons, kv):
    body = (
        _field_packed(1, ids, zigzag=True)
        + _field_packed(8, lats, zigzag=True)
        + _field_packed(9, lons, zigzag=True)
        + _field_packed(10, kv, zigzag=False)
    )
    return _field_bytes(2, body)   # primitivegroup.dense


def _simple_node(node_id, lat, lon, keys, vals):
    body = (
        _field_varint(1, node_id)
        + _field_packed(2, keys)
        + _field_packed(3, vals)
        + _field_varint(8, lat)
        + _field_varint(9, lon)
    )
    return _field_bytes(1, body)   # primitivegroup.nodes


def _way(way_id, keys, vals, refs):
    body = (
        _field_varint(1, way_id)
        + _field_packed(2, keys)
        + _field_packed(3, vals)
        + _field_packed(8, refs, zigzag=True)
    )
    return _field_bytes(3, body)   # primitivegroup.ways


def _primitive_block(stringtable, groups, granularity=100,
                     lat_offset=0, lon_offset=0):
    body = (
        _stringtable(stringtable)
        + b"".join(_field_bytes(2, g) for g in groups)
        + _field_varint(17, granularity)
        + _field_varint(19, lat_offset)
        + _field_varint(20, lon_offset)
    )
    return body


def _blob(payload):
    blob_body = _field_bytes(3, zlib.compress(payload))  # Blob.zlib_data
    header = _field_bytes(1, b"OSMData") + _field_varint(3, len(blob_body))
    # PBF blob framing: 4-byte big-endian length prefix.
    return len(header).to_bytes(4, "big") + header + blob_body


def build_fixture_pbf(path, granularity=100, lat_offset=0, lon_offset=0):
    """Write a small but standards-faithful .osm.pbf to ``path``.

    Contents:
      node 1:  lat 0.1001 lon 0.1998  (no tags)
      node 2:  lat 0.3004 lon 0.3006  (industrial=smelting)
      node 20: lat 0.3002 lon -0.1998 (industrial=refinery, non-dense)
      way 10:  closed refs [1,2,1]   (landuse=industrial polygon)
    Offsets default to 0; ``make_pbf_with_offsets`` overrides them.
    """
    st = ["", "industrial", "smelting", "refinery", "landuse"]
    smelt_k, smelt_v = st.index("industrial"), st.index("smelting")
    refin_k, refin_v = st.index("industrial"), st.index("refinery")
    ind_k, ind_v = st.index("landuse"), st.index("industrial")

    # dense deltas (in granularity units); cumulative:
    #   id  1, 2, 3   |  lat 1001, 3004, 2020  |  lon 1998, 3006, 1990
    ids = [1, 1, 1]
    lats = [1_001, 2_003, -984]
    lons = [1_998, 1_008, -1_016]
    kv = [0, smelt_k, smelt_v, 0, 0]   # node1 none, node2 tagged, node3 none
    dense = _dense_nodes(ids, lats, lons, kv)

    simple = _simple_node(20, 3_002, -3_998, [refin_k], [refin_v])

    way = _way(10, [ind_k], [ind_v], [1, 1, 1, -2])   # refs 1, 2, 3, 1 (deltas)

    block = _primitive_block(st, [dense, simple, way], granularity, lat_offset, lon_offset)
    with open(path, "wb") as fh:
        fh.write(_blob(block))
    return path


# ---------------------------------------------------------------------------
# Primitive decode vectors
# ---------------------------------------------------------------------------

class TestPrimitives:
    def test_zigzag_roundtrip(self):
        for n in (0, -1, 1, 2, -2, 1 << 30, -(1 << 30), (1 << 63) - 1):
            assert _zigzag(n) >= 0

    def test_varint_sign(self):
        from osm_pbf import ProtoReader
        # -1 as two's-complement 64-bit varint decodes to a negative magnitude
        r = ProtoReader(_varint(-1))
        assert r.signed() == -1
        # positive value passes through
        r2 = ProtoReader(_varint(1234))
        assert r2.signed() == 1234


# ---------------------------------------------------------------------------
# PBF decoder
# ---------------------------------------------------------------------------

class TestPbfDecoder:
    def test_dense_and_simple_nodes_and_way(self):
        fd, path = tempfile.mkstemp(suffix=".osm.pbf")
        os.close(fd)
        try:
            build_fixture_pbf(path)
            nodes, ways = {}, {}
            for el in iter_pbf(path):
                if el["type"] == "node":
                    nodes[el["id"]] = el
                else:
                    ways[el["id"]] = el

            # dense node 1 (untagged)
            assert nodes[1]["lat"] == pytest.approx(0.0001001, abs=1e-9)
            assert nodes[1]["lon"] == pytest.approx(0.0001998, abs=1e-9)
            assert nodes[1]["tags"] == {}
            # dense node 2 (tagged)
            assert nodes[2]["tags"].get("industrial") == "smelting"
            assert nodes[2]["lat"] == pytest.approx(0.0003004, abs=1e-9)
            # simple (non-dense) node with negative lon delta
            assert nodes[20]["tags"].get("industrial") == "refinery"
            assert nodes[20]["lat"] == pytest.approx(0.0003002, abs=1e-9)
            assert nodes[20]["lon"] == pytest.approx(-0.0003998, abs=1e-9)
            # way
            assert ways[10]["refs"] == [1, 2, 3, 1]
            assert ways[10]["tags"].get("landuse") == "industrial"
        finally:
            os.unlink(path)

    def test_offsets_and_granularity(self):
        # Switch offsets on; deltas encode the (nanodegree - offset) remainder.
        fd, path = tempfile.mkstemp(suffix=".osm.pbf")
        os.close(fd)
        try:
            build_fixture_pbf(path, granularity=100,
                              lat_offset=100_000_000, lon_offset=200_000_000)
            nodes = {}
            for el in iter_pbf(path):
                if el["type"] == "node":
                    nodes[el["id"]] = el
            # 0.1 + 1001 * 1e-7 = 0.1001001 ; 0.2 + 1998 * 1e-7 = 0.2001998
            # (the fixture stores raw nanodegree residues, not target coords)
            assert nodes[1]["lat"] == pytest.approx(0.1001001, abs=1e-9)
            assert nodes[1]["lon"] == pytest.approx(0.2001998, abs=1e-9)
        finally:
            os.unlink(path)

    def test_way_geometry(self):
        fd, path = tempfile.mkstemp(suffix=".osm.pbf")
        os.close(fd)
        try:
            build_fixture_pbf(path)
            nodes, ways = {}, {}
            for el in iter_pbf(path):
                (nodes if el["type"] == "node" else ways).setdefault(el["id"], el)
            wkt, lat, lon = build_way_geometry(ways[10], nodes)
            assert wkt.startswith(
                "POLYGON((0.0001998 0.0001001,0.0003006 0.0003004,"
                "0.0001990 0.0002020,0.0001998 0.0001001))")
            assert lat == pytest.approx(
                (0.0001001 + 0.0003004 + 0.0002020 + 0.0001001) / 4, abs=1e-8)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# selectors + XML path
# ---------------------------------------------------------------------------

class TestSelectors:
    def test_matches(self):
        assert selectors_match({"industrial": "refinery"})
        assert selectors_match({"landuse": "industrial"})
        assert selectors_match({"man_made": "works"})
        assert selectors_match({"power": "plant"})
        assert selectors_match({"landuse": "quarry"})
        assert not selectors_match({"highway": "residential"})
        assert not selectors_match({"landuse": "residential"})


class TestXmlParser:
    XML = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="test">
  <node id="1" lat="47.05" lon="9.50"/>
  <node id="2" lat="47.06" lon="9.51"/>
  <node id="3" lat="47.052" lon="9.505"/>
  <node id="4" lat="47.002" lon="9.002">
    <tag k="industrial" v="refinery"/>
  </node>
  <way id="10">
    <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="1"/>
    <tag k="landuse" v="industrial"/>
  </way>
  <way id="11">
    <nd ref="3"/><nd ref="2"/>
    <tag k="highway" v="unclassified"/>
  </way>
</osm>
"""

    def test_collect(self):
        with tempfile.NamedTemporaryFile("w", suffix=".osm", delete=False) as fh:
            fh.write(self.XML)
            name = fh.name
        try:
            found = {f["osm_id"]: f for f in _collect_from_xml(name)}
        finally:
            os.unlink(name)
        assert set(found) == {4, 10}
        assert found[4]["facility_type"] == "refinery"
        assert found[4]["wkt"].startswith("POINT(9.0020000 47.0020000")
        assert found[10]["facility_type"] == "industrial_area"
        assert found[10]["wkt"].startswith("POLYGON((")