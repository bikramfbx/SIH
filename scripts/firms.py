"""FIRMS ingestion core.

Shared by ``ingest_firms.py`` (on-demand CLI), ``backfill_firms.py``
(historical windows) and ``worker.py`` (near-real-time scheduler).

The NASA FIRMS Area API supports two URL shapes:

    NRT / latest N days
        /api/area/csv/{KEY}/{SOURCE}/{BBOX}/{DAYS}

    Historical window (DATE is the first day of a DIRECTIONS-day window)
        /api/area/csv/{KEY}/{SOURCE}/{BBOX}/{DAYS}/{DATE}

    DAYS is clamped to [1..5] by the API.

Ingestion is strictly idempotent: rows are inserted with
``INSERT ... ON CONFLICT DO NOTHING`` against the existing unique index on
(source, satellite, lat, lon, date, time). Duplicate rows are counted, never
re-inserted, and the raw FIRMS row (including daynight, confidence, FRP,
source, satellite) is stored untouched for later use.
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import requests
from dotenv import load_dotenv

API_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
DEFAULT_BBOX = "68,6,98,38"
DEFAULT_DAYS = 2
DEFAULT_RETRIES = 3
DEFAULT_TIMEOUT_S = 60
MAX_WINDOW_DAYS = 5
SUPPORTED_SOURCES = ("VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT")

_INSERT_COLUMNS = (
    "source", "satellite", "instrument",
    "latitude", "longitude",
    "acq_date", "acq_time",
    "frp", "bright_ti4", "bright_ti5",
    "scan", "track",
    "confidence", "daynight", "version",
)

INSERT_SQL = """
INSERT INTO hotspots (
    source, satellite, instrument,
    latitude, longitude,
    acq_date, acq_time,
    frp, bright_ti4, bright_ti5,
    scan, track,
    confidence, daynight, version,
    location
)
SELECT
    source, satellite, instrument,
    latitude, longitude,
    acq_date, acq_time,
    frp, bright_ti4, bright_ti5,
    scan, track,
    confidence, daynight, version,
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
FROM jsonb_to_recordset(%(rows)s::jsonb) AS x(
    source text, satellite text, instrument text,
    latitude double precision, longitude double precision,
    acq_date date, acq_time smallint,
    frp real, bright_ti4 real, bright_ti5 real,
    scan real, track real,
    confidence text, daynight text, version text
)
ON CONFLICT DO NOTHING
RETURNING id
"""


@dataclass
class InsertSummary:
    total: int = 0
    inserted: int = 0
    duplicates: int = 0
    invalid: int = 0
    inserted_ids: list[int] = field(default_factory=list)


def load_env():
    load_dotenv()


def parse_sources(csv_sources):
    """Normalize a command-line / env list of FIRMS source ids.

    Accepts a comma-separated string or a list; unknown sources raise
    ValueError so the caller fails fast instead of silently querying nothing.
    """
    if isinstance(csv_sources, str):
        csv_sources = [s.strip() for s in csv_sources.split(",") if s.strip()]
    if not csv_sources:
        raise ValueError("no FIRMS sources given")
    for s in csv_sources:
        if s not in SUPPORTED_SOURCES:
            raise ValueError(
                f"unknown FIRMS source {s!r}; supported: {SUPPORTED_SOURCES}"
            )
    return csv_sources


def _sleep(attempt, base=1.0):
    time.sleep(min(base * (2 ** (attempt - 1)), 20))


def fetch_firms_csv(map_key, source, bbox, days=DEFAULT_DAYS, date_param=None,
                    retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT_S):
    """Fetch raw FIRMS CSV. ``date_param`` is a date or 'YYYY-MM-DD' (window start).

    Raises RuntimeError when the API is unreachable/errored after ``retries``.
    Empty (CSV-header-only) responses are valid and returned as-is.
    """
    days = int(days)
    if not (1 <= days <= MAX_WINDOW_DAYS):
        raise ValueError(f"FIRMS day window must be in [1..{MAX_WINDOW_DAYS}], got {days}")

    if isinstance(date_param, str):
        date_param = date_param.strip() or None
    url = f"{API_BASE}/{map_key}/{source}/{bbox}/{days}"
    if date_param is not None:
        url = f"{url}/{date_param}"

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 429:
                last_error = HTTPRateLimited(resp.status_code, "FIRMS API rate-limited")
            else:
                last_error = HTTPStatusError(
                    resp.status_code, " ".join(resp.text.split())[:200]
                )
        except requests.exceptions.Timeout:
            last_error = RuntimeError("request timed out")
        except requests.exceptions.RequestException as exc:
            last_error = exc

        if attempt < retries:
            _sleep(attempt)
    raise RuntimeError(f"FIRMS API failed for {source} {bbox} days={days}"
                       f" after {retries} attempts: {last_error}")


class HTTPStatusError(Exception):
    def __init__(self, status, detail=""):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


class HTTPRateLimited(HTTPStatusError):
    pass


def validate_row(row):
    try:
        lat = float(row["latitude"])
        lon = float(row["longitude"])
    except (ValueError, KeyError, TypeError):
        return False
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return False

    try:
        datetime.strptime(row["acq_date"], "%Y-%m-%d")
    except (ValueError, KeyError, TypeError):
        return False

    try:
        acq_time = int(row["acq_time"])
    except (ValueError, KeyError, TypeError):
        return False
    if not (0 <= acq_time <= 2359) or (acq_time % 100) > 59:
        return False
    return True


def parse_row(row, source):
    """Normalize a raw CSV row to the JSON payload the bulk INSERT expects."""
    return {
        "source": source,
        "satellite": (row.get("satellite") or "").strip() or None,
        "instrument": (row.get("instrument") or "").strip() or None,
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "acq_date": row["acq_date"],
        "acq_time": int(row["acq_time"]),
        "frp": float(row["frp"]) if (row.get("frp") or "").strip() else None,
        "bright_ti4": float(row["bright_ti4"]) if (row.get("bright_ti4") or "").strip() else None,
        "bright_ti5": float(row["bright_ti5"]) if (row.get("bright_ti5") or "").strip() else None,
        "scan": float(row["scan"]) if (row.get("scan") or "").strip() else None,
        "track": float(row["track"]) if (row.get("track") or "").strip() else None,
        "confidence": (row.get("confidence") or "").strip() or None,
        "daynight": (row.get("daynight") or "").strip() or None,
        "version": (row.get("version") or "").strip() or None,
    }


def parse_csv(csv_text):
    """Parse raw FIRMS CSV into a list of row dicts (raw string fields)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def bulk_insert(conn, rows, collect_ids=True):
    """Insert normalized row dicts; returns an InsertSummary.

    Uses a single INSERT ... ON CONFLICT DO NOTHING RETURNING id so
    conflicts never abort the transaction and inserted ids come back for
    targeted enrichment.
    """
    valid = []
    summary = InsertSummary(total=len(rows))
    for row in rows:
        if validate_row(row):
            valid.append(parse_row(row, row.get("_source")))
        else:
            summary.invalid += 1
    n_valid = len(valid)

    if not valid:
        return summary

    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, {"rows": json.dumps(valid)})
        inserted_ids = [r[0] for r in cur.fetchall()] if collect_ids else []
        inserted = len(inserted_ids)
    summary.inserted = inserted
    summary.duplicates = n_valid - inserted
    summary.inserted_ids = inserted_ids
    return summary


def ingest_source(conn, map_key, source, bbox, days=DEFAULT_DAYS, date_param=None,
                  retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT_S,
                  collect_ids=True):
    """Fetch + insert one FIRMS source window. Returns an InsertSummary."""
    load_env()
    csv_text = fetch_firms_csv(
        map_key=map_key,
        source=source,
        bbox=bbox,
        days=days,
        date_param=date_param,
        retries=retries,
        timeout=timeout,
    )
    rows = parse_csv(csv_text)
    for row in rows:
        row["_source"] = source
    summary = bulk_insert(conn, rows, collect_ids=collect_ids)
    return summary


RUN_LOG_SQL = """
INSERT INTO ingestion_run_log (
    kind, source, bbox,
    date_from, date_to, days,
    returned, inserted, duplicates, invalid,
    status, error,
    started_at, finished_at
) VALUES (
    %(kind)s, %(source)s, %(bbox)s,
    %(date_from)s, %(date_to)s, %(days)s,
    %(returned)s, %(inserted)s, %(duplicates)s, %(invalid)s,
    %(status)s, %(error)s,
    %(started_at)s, %(finished_at)s
)
"""


def record_run(conn, *, kind="firms", source, bbox=None, date_from=None,
               date_to=None, days=None, summary=None, status="success",
               error=None, started_at=None, finished_at=None):
    """Insert one row into ingestion_run_log (pipeline telemetry)."""
    if summary is None:
        summary = InsertSummary()
    if started_at is None:
        started_at = datetime.now()
    if finished_at is None:
        finished_at = datetime.now()
    with conn.cursor() as cur:
        cur.execute(RUN_LOG_SQL, {
            "kind": kind,
            "source": source,
            "bbox": bbox,
            "date_from": date_from,
            "date_to": date_to,
            "days": days,
            "returned": summary.total,
            "inserted": summary.inserted,
            "duplicates": summary.duplicates,
            "invalid": summary.invalid,
            "status": status,
            "error": (error or "")[:500] or None,
            "started_at": started_at,
            "finished_at": finished_at,
        })
        rows_written = cur.rowcount
    return rows_written


def windows(start_date, end_date, window_days=MAX_WINDOW_DAYS):
    """Yield (start, days) cells covering [start_date .. end_date] inclusive.

    Each cell uses at most ``window_days`` days (FIRMS allows 1..5). The final
    cell is truncated to fit ``end_date``; overlapping/extra days are safe
    because ingestion is idempotent.
    """
    start_date = start_date if isinstance(start_date, date) else date.fromisoformat(start_date)
    end_date = end_date if isinstance(end_date, date) else date.fromisoformat(end_date)
    if start_date > end_date:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")
    if window_days < 1 or window_days > MAX_WINDOW_DAYS:
        raise ValueError(f"window_days must be in [1..{MAX_WINDOW_DAYS}]")

    cursor = start_date
    while cursor <= end_date:
        remaining = (end_date - cursor).days + 1
        days = min(window_days, remaining)
        yield cursor, days
        cursor += timedelta(days=window_days)