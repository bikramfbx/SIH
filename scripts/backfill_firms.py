"""Historical FIRMS backfill.

Populate historical detections for persistence / gas-flare analysis using the
FIRMS Area API, which serves windows of 1-5 days starting at a given date.

Example:
    python scripts/backfill_firms.py \
        --source VIIRS_NOAA20_NRT --source VIIRS_NOAA21_NRT \
        --start-date 2026-08-04 --end-date 2026-09-03 \
        --bbox 68,6,98,38 --days 5 --enrich

Idempotent: rows are INSERT ... ON CONFLICT DO NOTHING, so re-running is safe
and never duplicates data. Each FIRMS call is recorded in ingestion_run_log.

ENV: FIRMS_MAP_KEY (required); FIRMS_DAYS / FIRMS_BBOX overrides the defaults.
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
                   help="FIRMS source; repeat for several (default: NOAA-20 + "
                        "NOAA-21 NRT). One of: " + ", ".join(firms.SUPPORTED_SOURCES))
    p.add_argument("--start-date", required=True,
                   help="first backfill date, YYYY-MM-DD")
    p.add_argument("--end-date", required=True,
                   help="last backfill date (inclusive), YYYY-MM-DD")
    p.add_argument("--bbox", default=None,
                   help=f"west,south,east,north (default: {firms.DEFAULT_BBOX})")
    p.add_argument("--days", type=int, default=None,
                   help="FIRMS window size in days, 1-5 (default: 5)")
    p.add_argument("--enrich", action="store_true",
                   help="also run enrichment+classification for all hotspots "
                        "after backfill")
    p.add_argument("--dry-run", action="store_true",
                   help="print the FIRMS windows that would be fetched "
                        "without fetching anything")
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
    days = args.days or int(os.getenv("FIRMS_DAYS", firms.MAX_WINDOW_DAYS))

    try:
        cells = list(firms.windows(args.start_date, args.end_date, window_days=days))
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Backfill plan: {len(sources)} source(s) x {len(cells)} window(s) "
          f"= {len(sources) * len(cells)} FIRMS requests (bbox={bbox}, "
          f"days<= {days})")
    for start, wdays in cells:
        for src in sources:
            print(f"  {src}  {start}  +{wdays}d")

    if args.dry_run:
        print("\nDry run: nothing fetched.")
        return

    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    grand = {"fetched": 0, "inserted": 0, "duplicates": 0, "invalid": 0}
    with psycopg.connect(**conn_info) as conn:
        for src in sources:
            for start, wdays in cells:
                start_time = datetime.now()
                status, error = "success", None
                summary = firms.InsertSummary()
                try:
                    summary = firms.ingest_source(
                        conn, map_key, src, bbox, days=wdays, date_param=start)
                    conn.commit()
                except Exception as e:  # keep going; this window failed
                    conn.rollback()
                    status, error = "failed", str(e)
                    print(f"  [{src} {start}] FAILED: {e}", file=sys.stderr)
                firms.record_run(
                    conn, kind="backfill", source=src, bbox=bbox,
                    date_from=start, date_to=None, days=wdays,
                    summary=summary, status=status, error=error,
                    started_at=start_time, finished_at=datetime.now())
                conn.commit()
                grand["fetched"] += summary.total
                grand["inserted"] += summary.inserted
                grand["duplicates"] += summary.duplicates
                grand["invalid"] += summary.invalid
                print(
                    f"  [{src} {start}] returned={summary.total:5d} "
                    f"inserted={summary.inserted:5d} "
                    f"duplicates={summary.duplicates:5d} "
                    f"invalid={summary.invalid:4d} [{status}]")

        if args.enrich:
            import enrich_hotspots
            print("\nRunning enrichment + classification...")
            summary = enrich_hotspots.enrich(conn, conn_info=conn_info)
            conn.commit()
            print(f"Enriched rows: {summary['enriched']}")
            for label in sorted(summary["class_counts"]):
                print(f"  {label:>28}: {summary['class_counts'][label]}")

    print("\n--- Backfill Summary ---")
    print(f"Records fetched:   {grand['fetched']}")
    print(f"Records inserted:  {grand['inserted']}")
    print(f"Duplicates skipped:{grand['duplicates']}")
    print(f"Invalid skipped:   {grand['invalid']}")


if __name__ == "__main__":
    main()