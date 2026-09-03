import csv
import io
import os
import sys
from datetime import datetime

import psycopg
import requests
from dotenv import load_dotenv

FIRMS_API_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

DEFAULT_SOURCE = "VIIRS_NOAA20_NRT"
DEFAULT_BBOX = "68,6,98,38"
DEFAULT_DAYS = 2

INSERT_SQL = """
INSERT INTO hotspots (
    source, satellite, instrument,
    latitude, longitude,
    acq_date, acq_time,
    frp, bright_ti4, bright_ti5,
    scan, track,
    confidence, daynight, version,
    location
) VALUES (
    %(source)s, %(satellite)s, %(instrument)s,
    %(latitude)s, %(longitude)s,
    %(acq_date)s, %(acq_time)s,
    %(frp)s, %(bright_ti4)s, %(bright_ti5)s,
    %(scan)s, %(track)s,
    %(confidence)s, %(daynight)s, %(version)s,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography
)
"""


def fetch_firms_csv(map_key, source, bbox, days):
    params = {
        "source": source,
        "areas": bbox,
        "days": days,
        "api_key": map_key,
    }
    resp = requests.get(FIRMS_API_URL, params=params, timeout=60)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        detail = " ".join(resp.text.split())
        print(
            f"ERROR: FIRMS API returned HTTP {resp.status_code}: {detail}",
            file=sys.stderr,
        )
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: FIRMS API request failed: {e}", file=sys.stderr)
        sys.exit(1)
    return resp.text


def validate_row(row):
    try:
        lat = float(row["latitude"])
    except (ValueError, KeyError):
        return False
    try:
        lon = float(row["longitude"])
    except (ValueError, KeyError):
        return False
    if not (-90 <= lat <= 90):
        return False
    if not (-180 <= lon <= 180):
        return False

    try:
        datetime.strptime(row["acq_date"], "%Y-%m-%d")
    except (ValueError, KeyError):
        return False

    try:
        acq_time = int(row["acq_time"])
    except (ValueError, KeyError):
        return False
    if not (0 <= acq_time <= 2359):
        return False
    minutes = acq_time % 100
    if not (0 <= minutes <= 59):
        return False

    return True


def parse_row(row, source):
    return {
        "source": source,
        "satellite": row.get("satellite") or None,
        "instrument": row.get("instrument") or None,
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "acq_date": row["acq_date"],
        "acq_time": int(row["acq_time"]),
        "frp": float(row["frp"]) if row.get("frp") else None,
        "bright_ti4": float(row["bright_ti4"]) if row.get("bright_ti4") else None,
        "bright_ti5": float(row["bright_ti5"]) if row.get("bright_ti5") else None,
        "scan": float(row["scan"]) if row.get("scan") else None,
        "track": float(row["track"]) if row.get("track") else None,
        "confidence": row.get("confidence") or None,
        "daynight": row.get("daynight") or None,
        "version": row.get("version") or None,
    }


def main():
    load_dotenv()

    map_key = os.getenv("FIRMS_MAP_KEY")
    if not map_key:
        print("ERROR: FIRMS_MAP_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    source = os.getenv("FIRMS_SOURCE", DEFAULT_SOURCE)
    bbox = os.getenv("FIRMS_BBOX", DEFAULT_BBOX)
    days = int(os.getenv("FIRMS_DAYS", DEFAULT_DAYS))

    print(f"Fetching FIRMS data: source={source}, bbox={bbox}, days={days}")
    csv_text = fetch_firms_csv(map_key, source, bbox, days)

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)

    total = len(rows)
    inserted = 0
    skipped_dup = 0
    skipped_invalid = 0

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    with psycopg.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            for row in rows:
                if not validate_row(row):
                    skipped_invalid += 1
                    continue
                try:
                    cur.execute(INSERT_SQL, parse_row(row, source))
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped_dup += 1
                except psycopg.errors.UniqueViolation:
                    conn.rollback()
                    skipped_dup += 1
                except Exception as e:
                    conn.rollback()
                    print(f"Error inserting row: {e}", file=sys.stderr)
                    skipped_invalid += 1

    print(f"\n--- Ingestion Summary ---")
    print(f"Records returned:  {total}")
    print(f"Records inserted:  {inserted}")
    print(f"Duplicates skipped: {skipped_dup}")
    print(f"Invalid rows skipped: {skipped_invalid}")


if __name__ == "__main__":
    main()
