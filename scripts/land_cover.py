"""Land-cover evidence seam (optional, inert by default).

The pipeline does NOT depend on land cover. When ``LAND_COVER=1`` and a
working provider is configured, ``lookup()`` returns a class label + class
fractions for a lat/lon and ``persist_if_enabled()`` fills the
``hotspot_enrichment.landcover_*`` columns during enrichment. Every failure
(connect error, HTTP error, parse error, unknown class) degrades to
``None`` -- returns "no data", never raises, never destabilizes a cycle.

Provider: Esri Land Cover 2020 ImageServer ``identify``
(``https://lulc.esri.com/rest/services/LandCover2020/ImageServer/identify``).

Note on the build sandbox: Terrascope (Terrascope), ESA WorldCover S3 and
lulc.esri.com were all unreachable from this network when the seam was built,
so no live lookup was exercised here. The module is therefore delivered as a
guarded, graceful no-data seam and should be activated with a connectivity
check (``scripts/land_cover.py --probe``) before being relied upon.
"""

from __future__ import annotations

import os
import sys
import time

PROVIDER_NONE = "none"
PROVIDER_ESRI = "esri"

DEFAULT_ESRI_URL = (
    "https://lulc.esri.com/rest/services/LandCover2020/ImageServer/identify"
)

# Esri Land Cover 2020 class codes (LULC v10) -> human label
ESRI_CLASSES = {
    1: "tree_cover", 2: "shrubland", 3: "grassland", 4: "cropland",
    5: "built_up", 6: "bare", 7: "snow_ice", 8: "water",
    9: "wetlands", 10: "mangroves", 11: "moss_lichen",
}

CROPLAND_LABELS = {"cropland"}

_probability_cache = {"checked_at": 0, "ok": None, "note": None}


def provider_name():
    return (os.getenv("LAND_COVER_PROVIDER", PROVIDER_NONE) or PROVIDER_NONE).lower()


def enabled():
    """True only when the operator explicitly switched land cover on."""
    return os.getenv("LAND_COVER", "0") == "1" and provider_name() != PROVIDER_NONE


def _esri_url():
    return os.getenv("LAND_COVER_URL", DEFAULT_ESRI_URL)


def probe_esri(lat=23.0, lon=77.0, timeout=6.0):
    """Best-effort one-shot reachability probe of the configured provider."""
    try:
        import requests
        resp = requests.post(
            _esri_url(),
            json={
                "f": "json",
                "geometryType": "esriGeometryPoint",
                "geometry": {"x": lon, "y": lat, "spatialReference": {"wkid": 4326}},
                "returnGeometry": "false",
                "returnCatalogItems": "false",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or "value" not in data:
            return False, f"provider returned unexpected payload: {data!r:.120}"
        return True, f"provider responded; sample class code {data.get('value')}"
    except Exception as e:  # noqa: BLE001 - graceful by design
        return False, f"provider unreachable: {type(e).__name__}: {e}"


def provider_status():
    """dict for /api/health; never raises."""
    name = provider_name()
    if not enabled():
        return {"configured": False, "provider": name, "ok": None,
                "note": "land cover disabled (LAND_COVER!=1 or provider=none)"}
    if name == PROVIDER_ESRI:
        now = time.time()
        if now - _probability_cache["checked_at"] > 300:
            ok, note = probe_esri()
            _probability_cache.update(checked_at=now, ok=ok, note=note)
        return {"configured": True, "provider": name,
                "ok": _probability_cache["ok"], "note": _probability_cache["note"]}
    return {"configured": True, "provider": name,
            "ok": None, "note": "no support for provider"}


def _parse_esri(data):
    """Parse an Esri identify response into ``(class, fracs, note)``."""
    value = data.get("value")
    if value is None:
        return None, None, "no class value in response"
    label = ESRI_CLASSES.get(value)
    if label is None:
        return None, None, f"unknown class code {value}"
    fracs = {}
    for item in data.get("rasterFunctionResults", {}) or {}:
        if isinstance(item, dict) and "value" in item:
            fracs = item.get("value")
            break
    if not isinstance(fracs, dict) or "classCount" not in fracs:
        fracs = {"dominant_class": label}
    return label, fracs, f"esri class {value} ({label})"


def lookup(lat, lon, timeout=6.0):
    """Return ``(class, fracs, note)`` or ``(None, None, note)`` on failure."""
    if not enabled():
        return None, None, "land cover disabled"
    name = provider_name()
    if name == PROVIDER_ESRI:
        try:
            import requests
            resp = requests.post(
                _esri_url(),
                json={
                    "f": "json",
                    "geometryType": "esriGeometryPoint",
                    "geometry": {"x": lon, "y": lat,
                                 "spatialReference": {"wkid": 4326}},
                    "returnGeometry": "false",
                    "returnCatalogItems": "false",
                },
                timeout=timeout,
            )
            resp.raise_for_status()
            return _parse_esri(resp.json())
        except Exception as e:  # noqa: BLE001 - graceful by design
            return None, None, f"provider error: {type(e).__name__}: {e}"
    return None, None, f"unsupported provider {name}"


def persist_if_enabled(conn, ids, timeout=6.0, fetch_limit=500):
    """Optional enrichment pass: fill ``landcover_*`` for hotspot ids.

    Returns a small dict summary (usually zeros when disabled). Never raises;
    on provider failure it records ``landcover_note`` only where the provider
    was simply unavailable -- the collection stays NULL otherwise.
    """
    summary = {"landcover_persisted": 0, "landcover_unavailable": 0}
    if not enabled():
        return summary
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.id, h.latitude, h.longitude
                FROM hotspots h
                WHERE h.id = ANY(%(ids)s)
                  AND NOT EXISTS (
                      SELECT 1 FROM hotspot_enrichment e
                      WHERE e.hotspot_id = h.id
                        AND e.landcover_class IS NOT NULL
                  )
                LIMIT %(limit)s
            """, {"ids": ids, "limit": fetch_limit})
            rows = cur.fetchall()
        for hid, lat, lon in rows:
            label, fracs, note = lookup(lat, lon, timeout=timeout)
            if label is None:
                summary["landcover_unavailable"] += 1
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE hotspot_enrichment
                        SET landcover_note = %(note)s
                        WHERE hotspot_id = %(id)s
                          AND landcover_class IS NULL
                    """, {"id": hid, "note": (note or "unavailable")[:300]})
            else:
                summary["landcover_persisted"] += 1
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE hotspot_enrichment
                        SET landcover_class = %(cls)s,
                            landcover_fracs = %(fracs)s::jsonb,
                            landcover_note = %(note)s
                        WHERE hotspot_id = %(id)s
                    """, {"id": hid, "cls": label,
                          "fracs": json_dumps(fracs),
                          "note": (note or "")[:300]})
        conn.commit()
    except Exception as e:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:
            pass
        summary["landcover_error"] = f"{type(e).__name__}: {e}"
    return summary


def json_dumps(obj):
    import json
    return json.dumps(obj) if obj is not None else None


if __name__ == "__main__":
    if "--probe" in sys.argv:
        from dotenv import load_dotenv
        load_dotenv()
        ok, note = probe_esri()
        print(("OK " if ok else "FAIL ") + note)
        print(provider_status())
    sys.exit(0)