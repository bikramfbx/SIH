"""Pre-storage screening of raw FIRMS detections.

Only *industrial candidate* detections should ever be persisted. Raw FIRMS
rows (a mix of wildfires, agricultural burns, and persistent industrial heat
sources) are screened with cheap, deterministic rules *before* insertion:

S1 (prior-retained): the quantized location was retained as a candidate in a
   previous day (within FIRMS_SCREEN_PRIOR_DAYS). Persistent multi-day heat
   sources such as refinery flares re-appear night after night; transient
   wildfires do not.
S2 (within-day persistence): the quantized location is observed more than
   once within the same day window (multiple satellite overpasses hit a
   continuously-burning source).
S3 (high FRP): FRP >= FIRMS_SCREEN_FRP_MW absolute threshold, or >= the
   FIRMS_SCREEN_FRP_PERCENTILE percentile of the day's FRP distribution.

A row is kept when ANY rule matches; drops are only counted, never stored.
All thresholds are env-configurable (see firms._screen_params()).
"""

import math
from collections import Counter

DEFAULT_MIN_IN_DAY = 2
DEFAULT_PRIOR_DAYS = 90
DEFAULT_FRP_PERCENTILE = 90.0

CELL_ROUND = 2  # ~1.1 km bins at the equator (0.01 deg)


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


def _percentile_threshold(frps, percentile):
    if percentile is None or not frps:
        return None
    idx = int(math.ceil(percentile / 100.0 * len(frps))) - 1
    idx = max(0, min(len(frps) - 1, idx))
    return frps[idx]


def screen_rows(rows, source, prior_cells, frp_mw=None, percentile=None,
                min_in_day=DEFAULT_MIN_IN_DAY):
    """Screen raw CSV row dicts; returns ``(kept, stats)``."""
    stats = {"total": len(rows), "kept": 0, "dropped": 0,
             "prior": 0, "in_day": 0, "frp": 0}
    if not rows:
        return [], stats

    counts = Counter()
    for row in rows:
        try:
            counts[cell_key(row["latitude"], row["longitude"])] += 1
        except (ValueError, KeyError, TypeError):
            continue

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
        if m1 or m2 or m3:
            kept.append(row)
            stats["kept"] += 1
            if m1:
                stats["prior"] += 1
            if m2:
                stats["in_day"] += 1
            if m3:
                stats["frp"] += 1
        else:
            stats["dropped"] += 1
    return kept, stats