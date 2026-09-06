"""On-demand FIRMS ingestion CLI.

Fetches FIRMS Area API data (NRT or a historical date) for one or more
sources and inserts new detections idempotently. Records each run in
ingestion_run_log. See scripts/firms.py for the shared engine.

Examples:
    python scripts/ingest_firms.py                              # defaults
    python scripts/ingest_firms.py --source VIIRS_NOAA21_NRT --bbox 68,6,98,38
    python scripts/ingest_firms.py --date 2026-09-02 --days 1 --enrich

ENV: FIRMS_MAP_KEY (required); FIRMS_SOURCES/FIRMS_BBOX/FIRMS_DAYS defaults.
"""

import argparse
import os
import sys
from datetime import datetime

import psycopg
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import firms  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", action="append", default=None,
                   help="FIRMS source; repeat for several (default: both NRT).")
    p.add_argument("--bbox", default=None,
                   help=f"west,south,east,north (default: {firms.DEFAULT_BBOX})")
    p.add_argument("--days", type=int, default=None,
                   help=f"NRT or date-anchored window, 1-5 (default: {firms.DEFAULT_DAYS})")
    p.add_argument("--date", default=None,
                   help="window start date YYYY-MM-DD (historical; default: "
                        "most recent days from the NRT feed)")
    p.add_argument("--enrich", action="store_true",
                   help="enrich + classify newly inserted detections after ingest")
    return p.parse_args(argv)


def main(argv=None):
    load_dotenv()
    args = parse_args(argv)

    map_key = os.getenv("FIRMS_MAP_KEY")
    if not map_key:
        print("ERROR: FIRMS_MAP_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    sources = args.source or firms.parse_sources(
        os.getenv("FIRMS_SOURCES", ",".join(firms.SUPPORTED_SOURCES)))
    bbox = args.bbox or os.getenv("FIRMS_BBOX", firms.DEFAULT_BBOX)
    days = args.days or int(os.getenv("FIRMS_DAYS", firms.DEFAULT_DAYS))
    date_param = args.date

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    new_ids = []
    with psycopg.connect(**conn_info) as conn:
        for source in sources:
            start_time = datetime.now()
            status, error = "success", None
            summary = firms.InsertSummary()
            try:
                summary = firms.ingest_source(
                    conn, map_key, source, bbox, days=days,
                    date_param=date_param, collect_ids=True)
                conn.commit()
                new_ids.extend(summary.inserted_ids)
            except Exception as e:
                conn.rollback()
                status, error = "failed", str(e)
                print(f"ERROR: {source}: {e}", file=sys.stderr)
            firms.record_run(
                conn, kind="firms", source=source, bbox=bbox,
                date_from=date_param, days=days, summary=summary,
                status=status, error=error,
                started_at=start_time, finished_at=datetime.now())
            conn.commit()
            print(
                f"{source}: returned={summary.total} inserted={summary.inserted} "
                f"duplicates={summary.duplicates} invalid={summary.invalid} "
                f"[{status}]")

        if args.enrich and new_ids:
            import enrich_hotspots
            enr = enrich_hotspots.enrich(conn, conn_info=conn_info,
                                         hotspot_ids=new_ids)
            conn.commit()
            print(f"Enriched {enr['enriched']} new detections: "
                  f"{enr['class_counts']}")

    print("\n--- Ingestion Summary ---")
    print(f"Sources: {sources}")
    print(f"Date window: {date_param or 'latest NRT'} (days={days})")


if __name__ == "__main__":
    main()