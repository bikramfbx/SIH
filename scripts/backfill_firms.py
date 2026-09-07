"""Resumable historical FIRMS backfill.

Populate historical detections for persistence / gas-flare analysis using the
FIRMS Area API, which serves windows of 1-5 days starting at a given date.

Work is split into small **units of (source, one window)**. Each unit is its
own transaction, is recorded in ``ingestion_run_log`` after success, and is
*skipped on a re-run* unless ``--force`` -- so a re-run resumes from the last
incomplete unit instead of restarting everything.

Example:
    python scripts/backfill_firms.py \
        --source VIIRS_NOAA20_NRT --source VIIRS_NOAA21_NRT \
        --start-date 2026-09-03 --end-date 2026-09-07 \
        --bbox world --days 1

With FIRMS_SCREEN_ENABLED=1 the fetched footprint is reduced to industrial
candidates *before* insertion (see scripts/firms_screen.py); every unit
reports fetched/kept/inserted/duplicates.

ENV: FIRMS_MAP_KEY (required); FIRMS_DAYS / FIRMS_BBOX / FIRMS_SCREEN_* overrides.
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
                   help="FIRMS window size in days, 1-5 (default: 1; 1 = "
                        "per-day units, the most resumable)")
    p.add_argument("--enrich", action="store_true",
                   help="run one enrichment+classification pass over all "
                        "hotspots after the backfill units finish")
    p.add_argument("--force", action="store_true",
                   help="re-run units that already have a successful "
                        "ingestion_run_log entry")
    p.add_argument("--dry-run", action="store_true",
                   help="print the units that would be fetched (and which are "
                        "already done) without fetching anything")
    return p.parse_args(argv)


def _conn_info():
    info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }
    if os.getenv("POSTGRES_SSLMODE"):
        info["sslmode"] = os.getenv("POSTGRES_SSLMODE")
    return info


def _unit_done(conn, source, date_from, days, bbox_norm):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM ingestion_run_log
            WHERE kind = 'backfill' AND source = %s
              AND date_from = %s AND days = %s
              AND bbox = %s AND status = 'success'
            LIMIT 1
            """,
            (source, date_from, days, bbox_norm),
        )
        return cur.fetchone() is not None


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
    try:
        bbox_norm = firms.normalize_bbox(bbox)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    days = args.days or int(os.getenv("FIRMS_DAYS", "1"))

    try:
        cells = list(firms.windows(args.start_date, args.end_date, window_days=days))
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    units = [(src, start, wdays) for src in sources for (start, wdays) in cells]
    print(f"Backfill plan: {len(units)} unit(s) "
          f"({len(sources)} source(s) x {len(cells)} window(s)) "
          f"bbox={bbox_norm}, window_days={days}")
    for src, start, wdays in units:
        print(f"  {src}  {start}  +{wdays}d")

    if args.dry_run:
        print("\nDry run: nothing fetched.")
        return

    with psycopg.connect(**_conn_info()) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")

        grand = {"fetched": 0, "kept": 0, "inserted": 0,
                 "duplicates": 0, "invalid": 0, "skipped": 0, "failed": 0}
        for src, start, wdays in units:
            if not args.force and _unit_done(conn, src, start, wdays, bbox_norm):
                grand["skipped"] += 1
                print(f"  [skip] {src} {start} +{wdays}d (already backfilled)")
                continue

            start_time = datetime.now()
            status, error = "success", None
            summary = firms.InsertSummary()
            try:
                summary = firms.ingest_source(
                    conn, map_key, src, bbox_norm, days=wdays,
                    date_param=start, collect_ids=False, screen_kind="backfill")
                conn.commit()
            except Exception as e:  # keep going; this unit failed
                conn.rollback()
                status, error = "failed", str(e)
                grand["failed"] += 1
                print(f"  [{src} {start}] FAILED: {e}", file=sys.stderr)

            firms.record_run(
                conn, kind="backfill", source=src, bbox=bbox_norm,
                date_from=start, date_to=None, days=wdays,
                summary=summary, status=status, error=error,
                started_at=start_time, finished_at=datetime.now())
            conn.commit()

            grand["fetched"] += summary.fetched
            grand["inserted"] += summary.inserted
            grand["duplicates"] += summary.duplicates
            grand["invalid"] += summary.invalid
            line = (f"  [{src} {start}] fetched={summary.fetched:6d} "
                    f"inserted={summary.inserted:6d} "
                    f"duplicates={summary.duplicates:6d} "
                    f"invalid={summary.invalid:4d} [{status}]")
            if summary.screen:
                grand["kept"] += summary.screen["kept"]
                s = summary.screen
                line += (f" | screen: kept={s['kept']} dropped={s['dropped']} "
                         f"(prior={s['prior']}, in_day={s['in_day']}, "
                         f"frp={s['frp']})")
            print(line)

        if args.enrich:
            import enrich_hotspots
            print("\nRunning enrichment + classification (single pass)...")
            summary = enrich_hotspots.enrich(conn, conn_info=_conn_info())
            conn.commit()
            print(f"Enriched rows: {summary['enriched']}")
            for label in sorted(summary["class_counts"]):
                print(f"  {label:>28}: {summary['class_counts'][label]}")

    print("\n--- Backfill Summary ---")
    print(f"Units:             {len(units)} (skipped {grand['skipped']}, "
          f"failed {grand['failed']})")
    print(f"Records fetched:   {grand['fetched']}")
    if grand["kept"]:
        print(f"Screened kept:     {grand['kept']}")
    print(f"Records inserted:  {grand['inserted']}")
    print(f"Duplicates skipped:{grand['duplicates']}")
    print(f"Invalid skipped:   {grand['invalid']}")


if __name__ == "__main__":
    main()