"""Pre-storage screening of raw FIRMS detections.

Only *industrial candidate* detections are persisted. Raw FIRMS rows (a mix
of wildfires, agricultural burns, and persistent industrial heat sources) are
screened with cheap, deterministic rules *before* insertion.

A row is KEPT **only** when it is within ``FIRMS_SCREEN_FACILITY_REACH_M``
of a known industrial facility (checked on quantized ~1.1 km cells, matching
the classifier's industrial-context reach). Every other signal -- prior-cell
retention (S1), within-day persistence (S2), high FRP (S3) -- is computed and
reported as telemetry but never independently admits a row, because those
signals also match vegetation fires. Requiring facility context guarantees
that whatever the classifier later labels, it has industrial anchoring, and
does not re-flood the database with agricultural/forest burns.

All thresholds are env-configurable (see firms._screen_params()).
"""

import math
from collections import Counter

DEFAULT_MIN_IN_DAY = 2
DEFAULT_PRIOR_DAYS = 90
DEFAULT_FRP_PERCENTILE = 90.0
DEFAULT_FACILITY_REACH_M = 5000.0

CELL_ROUND = 2  # ~1.1 km bins at the equator (0.01 deg)
CELL_KM = 111.32 * 0.01  # ~1.11 km per cell step


def cell_key(latitude, longitude):
    """Quantized (streetblock-ish) bin used for persistence checks."""
    return (round(float(latitude), CELL_ROUND), round(float(longitude), CELL_ROUND))


def _frp(row):
    raw = (row.get("frp") or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def load_prior_cells(conn, source, days=DEFAULT_PRIOR_DAYS):
    """All (source, cell) locations already stored as industrial candidates."""
    if not days:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT source, round(latitude::numeric, 2),
                            round(longitude::numeric, 2)
            FROM hotspots
            WHERE acq_date >= (CURRENT_DATE - %(days)s::int)
            """,
            {"days": int(days)},
        )
        return {(r[0], (float(r[1]), float(r[2]))) for r in cur.fetchall()}


def load_facility_cells(conn, days=None):
    """All 0.01-deg cells that contain at least one industrial facility.

    ``days`` is accepted for API symmetry (S1) but facilities are static
    reference data, so every existing facility cell is returned regardless.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT round(ST_Y(location::geometry)::numeric, 2),
                            round(ST_X(location::geometry)::numeric, 2)
            FROM industrial_facilities
            WHERE location IS NOT NULL
            """,
        )
        return {(float(r[0]), float(r[1])) for r in cur.fetchall()}


def _percentile_threshold(frps, percentile):
    if percentile is None or not frps:
        return None
    idx = int(math.ceil(percentile / 100.0 * len(frps))) - 1
    idx = max(0, min(len(frps) - 1, idx))
    return frps[idx]


def _facility_near(cell, facility_cells, radius=0):
    """True when ``cell`` is within ``radius`` cells (8-neighborhood) of a
    facility cell."""
    if radius < 0:
        radius = 0
    lat, lon = cell
    step = 1 / (10 ** CELL_ROUND)
    for dlat in range(-radius, radius + 1):
        for dlon in range(-radius, radius + 1):
            if (round(lat + dlat * step, CELL_ROUND),
                    round(lon + dlon * step, CELL_ROUND)) in facility_cells:
                return True
    return False


def reach_cells(reach_m):
    """Number of neighboring cells that cover ``reach_m`` metres of radius."""
    reach_m = max(0.0, float(reach_m or 0))
    return int(math.ceil(reach_m / (CELL_KM * 1000.0)))


def screen_rows(rows, source, prior_cells, facility_cells=None, frp_mw=None,
                percentile=None, min_in_day=DEFAULT_MIN_IN_DAY,
                facility_reach_m=DEFAULT_FACILITY_REACH_M):
    """Screen raw CSV row dicts; returns ``(kept, stats)``.

    Keep rule: the row's quantized cell must be within ``facility_reach_m``
    of a known industrial facility cell. S1/S2/S3 are reported as stats only.
    """
    stats = {"total": len(rows), "kept": 0, "dropped": 0,
             "prior": 0, "in_day": 0, "frp": 0, "near_facility": 0}
    if not rows:
        return [], stats

    counts = Counter()
    for row in rows:
        try:
            counts[cell_key(row["latitude"], row["longitude"])] += 1
        except (ValueError, KeyError, TypeError):
            continue

    if facility_cells is None:
        facility_cells = set()

    frps = sorted(f for f in (_frp(row) for row in rows) if f is not None)
    threshold = None
    if frp_mw:
        threshold = float(frp_mw)
    elif percentile is not None:
        threshold = _percentile_threshold(frps, percentile)

    kept = []
    for row in rows:
        try:
            cell = cell_key(row["latitude"], row["longitude"])
            frp = _frp(row)
        except (ValueError, KeyError, TypeError):
            stats["dropped"] += 1
            continue
        m1 = (source, cell) in prior_cells
        m2 = counts[cell] >= min_in_day
        m3 = threshold is not None and frp is not None and frp >= threshold
        m4 = _facility_near(cell, facility_cells,
                            radius=reach_cells(facility_reach_m))
        # Accumulate telemetry regardless of the keep decision.
        if m1:
            stats["prior"] += 1
        if m2:
            stats["in_day"] += 1
        if m3:
            stats["frp"] += 1
        if m4:
            stats["near_facility"] += 1
        if m4:
            kept.append(row)
            stats["kept"] += 1
        else:
            stats["dropped"] += 1
    return kept, stats