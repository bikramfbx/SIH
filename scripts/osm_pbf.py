"""Minimal pure-Python reader for OSM PBF files (osmpbf format).

No third-party dependencies: only ``zlib`` (for compressed blobs) and a tiny
hand-rolled protobuf decoder. Supports the elements this project needs --
nodes and ways, including dense node packing -- enough to feed the industrial
facility ingestion pipeline (``ingest_osm_extract.py``) with regional extracts
(Geofabrik/Osmium) without requiring pyosmium or a compiler.

API
    open_pbf(path) -> iterator yielding ``dict`` elements:
        {"type": "node", "id", "lat", "lon", "tags": {k: v}}
        {"type": "way",  "id", "refs": [node_id, ...], "tags": {k: v}}

Notes / limitations
    * Relations (multipolygons) are read but skipped: industrial polygons in
      regional extracts are almost always closed ways. Skipped relations are
      counted via ``iter_pbf(..., stats=None)``.
    * Following libosmium, plain node ``id``/``lat``/``lon`` are decoded as
      raw int64 (non-zigzag) and dense deltas as zigzag sint64 -- this matches
      Osmosis/Geofabrik output.
    * Memory: all node coordinates are retained in a dict so way geometry can
      be resolved. For country-scale extracts use an OSMDownload/planet region
      (e.g. a Geofabrik subregion or a bbox-clipped extract via ``osmium``)
      rather than the full planet.
"""

from __future__ import annotations

import sys
import zlib

# ---------------- Tiny protobuf wire format reader -------------------------


class ProtoReader:
    __slots__ = ("buf", "pos", "end")

    def __init__(self, buf, pos=0, end=None):
        self.buf = buf
        self.pos = pos
        self.end = len(buf) if end is None else end

    def varint(self):
        result = 0
        shift = 0
        buf = self.buf
        while True:
            b = buf[self.pos]
            self.pos += 1
            result = (result | ((b & 0x7F) << shift)) & ((1 << 64) - 1)
            if not (b & 0x80):
                return result
            shift += 7
            if shift > 63:
                raise ValueError("varint too long")

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def signed(self):
        n = self.varint()
        if n >= 1 << 63:
            n -= 1 << 64
        return n

    def bytes_val(self):
        size = self.varint()
        start = self.pos
        self.pos += size
        return self.buf[start:start + size]

    def fields(self):
        """Yield (field_number, wire_type, value_material)."""
        while self.pos < self.end:
            key = self.varint()
            fnum, wire = key >> 3, key & 7
            if wire == 0:
                yield fnum, wire, self.varint()
            elif wire == 2:
                yield fnum, wire, self.bytes_val()
            elif wire == 5:
                yield fnum, wire, self.buf[self.pos:self.pos + 4]
                self.pos += 4
            elif wire == 1:
                yield fnum, wire, self.buf[self.pos:self.pos + 8]
                self.pos += 8
            else:
                raise ValueError(f"unsupported wire type {wire}")
        return


def _packed_varints(data, signed=False):
    r = ProtoReader(data)
    out = []
    while r.pos < r.end:
        out.append(r.signed() if signed else r.varint())
    return out


def _packed_zigzags(data):
    r = ProtoReader(data)
    out = []
    while r.pos < r.end:
        out.append(r.zigzag())
    return out


# ---------------- Slightly higher-level structure parsing -------------------

def parse_stringtable(data):
    r = ProtoReader(data)
    entries = []
    for fnum, wire, raw in r.fields():
        if fnum == 1 and wire == 2:
            entries.append(raw.decode("utf-8", "replace"))
    return entries


def _tags_from(s_idx, v_idx, stringtable):
    if s_idx is None or v_idx is None:
        return {}
    tags = {}
    if len(s_idx) != len(v_idx):
        return tags
    for k, v in zip(s_idx, v_idx):
        if k >= 0 and k < len(stringtable) and v < len(stringtable):
            tags[stringtable[k]] = stringtable[v]
    return tags


def _int64(val):
    """Interpret a raw 64-bit varint magnitude as signed int64."""
    return val if val < (1 << 63) else val - (1 << 64)


def _parse_node(raw, stringtable, granularity=100, lat_offset=0, lon_offset=0):
    r = ProtoReader(raw)
    nid = None
    s_idx = v_idx = None
    lat = lon = None
    for fnum, wire, val in r.fields():
        if fnum == 1 and wire == 0:
            nid = _int64(val)
        elif fnum == 2 and wire == 2:
            s_idx = _packed_varints(val)
        elif fnum == 3 and wire == 2:
            v_idx = _packed_varints(val)
        elif fnum == 8 and wire == 0:
            lat = _int64(val)
        elif fnum == 9 and wire == 0:
            lon = _int64(val)
    if nid is None or lat is None or lon is None:
        return None
    scale = granularity / 1e9
    return {"type": "node", "id": nid,
            "lat": lat_offset / 1e9 + lat * scale,
            "lon": lon_offset / 1e9 + lon * scale,
            "tags": _tags_from(s_idx, v_idx, stringtable)}


def _parse_dense(raw, stringtable, granularity, lat_offset, lon_offset):
    r = ProtoReader(raw)
    ids = []; lats = []; lons = []; kv = []
    for fnum, wire, val in r.fields():
        if fnum == 1 and wire == 2:
            ids = _packed_zigzags(val)          # delta-encoded
        elif fnum == 8 and wire == 2:
            lats = _packed_zigzags(val)         # delta-encoded
        elif fnum == 9 and wire == 2:
            lons = _packed_zigzags(val)         # delta-encoded
        elif fnum == 10 and wire == 2:
            kv = _packed_varints(val)
    n = min(len(ids), len(lats), len(lons))
    scale = granularity / 1e9
    out = []
    acc_id = acc_lat = acc_lon = 0
    kv_pos = 0
    for i in range(n):
        acc_id += ids[i]
        acc_lat += lats[i]
        acc_lon += lons[i]
        tags = {}
        while kv_pos < len(kv) and kv[kv_pos] != 0:
            if kv_pos + 1 < len(kv):
                k, v = kv[kv_pos], kv[kv_pos + 1]
                if k < len(stringtable) and v < len(stringtable):
                    tags[stringtable[k]] = stringtable[v]
            kv_pos += 2
        kv_pos += 1
        out.append({"type": "node",
                    "id": acc_id,
                    "lat": lat_offset / 1e9 + acc_lat * scale,
                    "lon": lon_offset / 1e9 + acc_lon * scale,
                    "tags": tags})
    return out


def _parse_way(raw, stringtable):
    r = ProtoReader(raw)
    wid = None
    s_idx = v_idx = None
    refs = []
    for fnum, wire, val in r.fields():
        if fnum == 1 and wire == 0:
            wid = _int64(val)
        elif fnum == 2 and wire == 2:
            s_idx = _packed_varints(val)
        elif fnum == 3 and wire == 2:
            v_idx = _packed_varints(val)
        elif fnum == 8 and wire == 2:
            # refs are sint64 packed (delta-encoded)
            acc = 0
            for delta in _packed_signed_deltas(val):
                acc += delta
                refs.append(acc)
    if wid is None or not refs:
        return None
    return {"type": "way", "id": wid, "refs": refs,
            "tags": _tags_from(s_idx, v_idx, stringtable)}


def _packed_signed_deltas(data):
    """Packed sint64 deltas (zigzag) for way refs."""
    return _packed_zigzags(data)


def _parse_primitive_group(raw, stringtable, granularity, lat_offset, lon_offset):
    r = ProtoReader(raw)
    nodes, ways, skipped_relations = [], [], 0
    for fnum, wire, val in r.fields():
        if fnum == 1 and wire == 2:                 # nodes
            node = _parse_node(val, stringtable,
                               granularity, lat_offset, lon_offset)
            if node is not None:
                nodes.append(node)
        elif fnum == 2 and wire == 2:               # dense
            nodes.extend(_parse_dense(val, stringtable,
                                      granularity, lat_offset, lon_offset))
        elif fnum == 3 and wire == 2:               # ways
            way = _parse_way(val, stringtable)
            if way is not None:
                ways.append(way)
        elif fnum == 4 and wire == 2:               # relations -> skipped
            skipped_relations += 1
    return nodes, ways, skipped_relations


# ---------------- Blob / file framing ---------------------------------------

def _blobs(fh):
    while True:
        head = fh.read(4)
        if not head:
            return
        if len(head) < 4:
            raise ValueError("truncated PBF header length")
        header_len = int.from_bytes(head, "big")
        header_raw = fh.read(header_len)
        if len(header_raw) < header_len:
            raise ValueError("truncated BlobHeader")
        hr = ProtoReader(header_raw)
        btype, datasize = None, None
        for fnum, wire, val in hr.fields():
            if fnum == 1 and wire == 2 and btype is None:
                btype = val
            elif fnum == 3 and wire == 0:
                datasize = val
        if btype is None or datasize is None:
            raise ValueError("malformed BlobHeader")
        blob_raw = fh.read(datasize)
        if len(blob_raw) < datasize:
            raise ValueError("truncated Blob")
        yield btype, blob_raw


def _inflate(blob_raw):
    r = ProtoReader(blob_raw)
    raw = None
    zlib_data = None
    for fnum, wire, val in r.fields():
        if fnum == 1 and wire == 2:
            raw = val
        elif fnum == 3 and wire == 2:
            zlib_data = val
    if zlib_data is not None:
        # Geofabrik/Osmosis write standard zlib streams (not raw deflate).
        try:
            return zlib.decompress(zlib_data)
        except zlib.error:
            return zlib.decompress(zlib_data, -15)
    if raw is not None:
        return raw
    raise ValueError("blob has no raw/zlib payload")


def iter_pbf(path, stats=None):
    """Yield element dicts (nodes then ways) from an OSM PBF file.

    ``stats`` -- optional dict updated with skipped_relations / skipped_nodes.
    """
    skipped = {"relations": 0, "nodes": 0, "ways": 0}
    with open(path, "rb") as fh:
        for btype, blob_raw in _blobs(fh):
            if btype != b"OSMData":
                continue
            payload = _inflate(blob_raw)
            br = ProtoReader(payload)
            stringtable = None
            groups = []
            granularity = 100
            lat_offset = lon_offset = 0
            for fnum, wire, val in br.fields():
                if fnum == 1 and wire == 2:
                    stringtable = parse_stringtable(val)
                elif fnum == 2 and wire == 2:
                    groups.append(val)
                elif fnum == 17 and wire == 0:
                    granularity = val
                elif fnum == 19 and wire == 0:
                    lat_offset = val if val < (1 << 63) else val - (1 << 64)
                elif fnum == 20 and wire == 0:
                    lon_offset = val if val < (1 << 63) else val - (1 << 64)
            if stringtable is None:
                continue
            for group_raw in groups:
                nodes, ways, rels = _parse_primitive_group(
                    group_raw, stringtable, granularity, lat_offset, lon_offset)
                skipped["relations"] += rels
                for n in nodes:
                    yield n
                for w in ways:
                    yield w
    if stats is not None:
        stats.update(skipped)
    return


def build_way_geometry(way, node_coords):
    """Resolve a way's node refs into geometry.

    Returns ``(geom_wkt, point_lat, point_lon)`` or ``(None, None, None)``.
    Closed rings become polygons; open ways become linestrings.
    """
    pts = [(c["lat"], c["lon"]) if isinstance(c, dict) else c
           for c in (node_coords.get(r) for r in way["refs"])]
    if not pts or any(p is None for p in pts):
        return None, None, None
    if len(pts) < 2:
        return None, None, None
    closed = pts[0] == pts[-1] and len(pts) >= 4
    ring = pts if closed else pts
    if len(ring) < 2:
        return None, None, None
    coords = "".join(f"{lon:.7f} {lat:.7f}," for lat, lon in ring).rstrip(",")
    if closed:
        wkt = f"POLYGON(({coords}))"
    else:
        wkt = f"LINESTRING({coords})"
    mid = pts[len(pts) // 2] if not closed else \
        (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))
    return wkt, mid[0], mid[1]


if __name__ == "__main__":
    # Self-check harness: dump counts for a .pbf path given as argv[1].
    if len(sys.argv) < 2:
        print("usage: python scripts/osm_pbf.py <file.osm.pbf>")
        sys.exit(1)
    stats = {}
    n_nodes = n_ways = 0
    samples = []
    for el in iter_pbf(sys.argv[1], stats=stats):
        if el["type"] == "node":
            n_nodes += 1
            if len(samples) < 3:
                samples.append(el)
        elif el["type"] == "way":
            n_ways += 1
    print("nodes:", n_nodes, "ways:", n_ways)
    print("skipped:", stats)
    for s in samples:
        print("  sample node:", s["id"], f'{s["lat"]:.5f}', f'{s["lon"]:.5f}',
              dict(list(s["tags"].items())[:3]))