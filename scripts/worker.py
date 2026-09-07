"""Scheduled near-real-time FIRMS ingestion worker (final backend).

Runs forever unless stopped; on every cycle it:

    1. fetches the latest NRT detections for each configured FIRMS source
    2. inserts only new rows (idempotent; duplicates are counted, not re-added)
    3. records each run in ingestion_run_log (telemetry for /api/health)
    4. enriches + classifies ONLY the newly inserted detections (temporal
       features are recomputed for those hotspots; the whole dataset is not
       re-enriched on a cycle)
    5. optionally runs async OSM facility-context refresh (opt-in, NOT by
       default) so Overpass never sits on the live path

Fault tolerance
    * transient FIRMS/network/DB failures are caught, logged, recorded as a
      failed run, and the worker continues to the next source/cycle
    * no unbounded retry loops: a FIRMS source gets a bounded number of
      attempts inside firms.fetch_firms_csv, then the failure is recorded
    * graceful shutdown on SIGINT/SIGTERM

The worker is a separate process from the API: if it crashes or is restarted
it never takes the FastAPI server down, and the API reads the same database.

ENV
    FIRMS_POLL_INTERVAL_MIN   seconds between cycles (default 600)
    FIRMS_MAP_KEY             NASA FIRMS key (required)
    FIRMS_BBOX                west,south,east,north (default 68,6,98,38)
    FIRMS_SOURCES             comma list (default VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT)
    FIRMS_DAYS                NRT window in days, 1-5 (default 2)
    WORKER_OSM_CONTEXT        1 = also refresh uncovered OSM facility context
                              for new hotspots (default 0 = never query Overpass)
"""

import argparse
import os
import signal
import sys
import time
from datetime import datetime

import psycopg
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import firms  # noqa: E402

DEFAULT_POLL_INTERVAL_MIN = 10
WORKER_VERSION = "worker-1.0.0"
_shutdown = False

HEARTBEAT_SQL = """
INSERT INTO worker_status (
    worker_id, started_at, last_heartbeat_at, cycle_count,
    last_cycle_status, last_error, last_inserted, version
) VALUES (
    %(worker_id)s, COALESCE(%(started_at)s, NOW()), NOW(),
    %(cycle_count)s, %(status)s, %(error)s, %(inserted)s, %(version)s
)
ON CONFLICT (worker_id) DO UPDATE SET
    last_heartbeat_at = NOW(),
    cycle_count        = EXCLUDED.cycle_count,
    last_cycle_status  = EXCLUDED.last_cycle_status,
    last_error         = EXCLUDED.last_error,
    last_inserted      = EXCLUDED.last_inserted,
    version            = EXCLUDED.version
"""


def write_heartbeat(conn, worker_id, *, status="success", error=None,
                    inserted=0, cycle_count=None, started_at=None):
    """Record one worker heartbeat. Never raises (best-effort telemetry)."""
    try:
        with conn.cursor() as cur:
            cur.execute(HEARTBEAT_SQL, {
                "worker_id": worker_id,
                "started_at": started_at,
                "cycle_count": cycle_count if cycle_count is not None else 1,
                "status": status,
                "error": (error or "")[:400] or None,
                "inserted": inserted,
                "version": WORKER_VERSION,
            })
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[{datetime.now():%H:%M:%S}] heartbeat failed: {e}",
              file=sys.stderr)


def _sig_handler(signum, frame):
    global _shutdown
    _shutdown = True


def _conn_info():
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }


def run_cycle(conn, map_key, cfg):
    """One ingestion cycle for all configured sources. Never raises."""
    cycle_started = datetime.now()
    total_new = 0
    for source in cfg["sources"]:
        run_started = datetime.now()
        summary = firms.InsertSummary()
        status, error = "success", None
        try:
            summary = firms.ingest_source(
                conn, map_key, source, cfg["bbox"],
                days=cfg["days"], collect_ids=True)
            conn.commit()
        except Exception as e:
            conn.rollback()
            status, error = "failed", str(e)
            print(f"[{datetime.now():%H:%M:%S}] {source}: FAILED ({e})",
                  file=sys.stderr)

        firms.record_run(
            conn, kind="firms", source=source, bbox=cfg["bbox"],
            date_from=None, date_to=None, days=cfg["days"],
            summary=summary, status=status, error=error,
            started_at=run_started, finished_at=datetime.now())
        conn.commit()
        total_new += summary.inserted
        print(
            f"[{datetime.now():%H:%M:%S}] {source}: "
            f"returned={summary.total} inserted={summary.inserted} "
            f"duplicates={summary.duplicates} invalid={summary.invalid} "
            f"[{status}]",
            flush=True)

        if status == "success" and summary.inserted_ids:
            try:
                import enrich_hotspots
                enr = enrich_hotspots.enrich(
                    conn, conn_info=_conn_info(),
                    hotspot_ids=summary.inserted_ids)
                conn.commit()
                print(
                    f"[{datetime.now():%H:%M:%S}] enriched "
                    f"{enr['enriched']} new detection(s): {enr['class_counts']}",
                    flush=True)
            except Exception as e:
                conn.rollback()
                print(f"[{datetime.now():%H:%M:%S}] enrichment failed: {e}",
                      file=sys.stderr)

    # Optional async OSM facility context for new hotspots. Off by default;
    # failures here never break the live pipeline.
    if cfg["osm_context"] and total_new > 0:
        try:
            from ingest_facility_context import process_new_hotspot_context
            processed = process_new_hotspot_context(conn, _conn_info())
            conn.commit()
            print(f"[{datetime.now():%H:%M:%S}] OSM context refresh: {processed}",
                  flush=True)
        except Exception as e:
            conn.rollback()
            print(f"[{datetime.now():%H:%M:%S}] OSM context skipped: {e}",
                  file=sys.stderr)

    return total_new


def main(argv=None):
    load_dotenv()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--once", action="store_true",
                   help="run a single cycle and exit (for cron/tests)")
    p.add_argument("--interval-min", type=float, default=None,
                   help="override FIRMS_POLL_INTERVAL_MIN")
    p.add_argument("--bbox", default=None)
    args = p.parse_args(argv)

    map_key = os.getenv("FIRMS_MAP_KEY")
    if not map_key:
        print("ERROR: FIRMS_MAP_KEY not set in .env", file=sys.stderr)
        sys.exit(2)

    interval = (args.interval_min if args.interval_min is not None
                else float(os.getenv("FIRMS_POLL_INTERVAL_MIN",
                                     DEFAULT_POLL_INTERVAL_MIN)))
    cfg = {
        "sources": firms.parse_sources(
            os.getenv("FIRMS_SOURCES", ",".join(firms.SUPPORTED_SOURCES))),
        "bbox": args.bbox or os.getenv("FIRMS_BBOX", firms.DEFAULT_BBOX),
        "days": int(os.getenv("FIRMS_DAYS", firms.DEFAULT_DAYS)),
        "osm_context": os.getenv("WORKER_OSM_CONTEXT", "0") == "1",
    }

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    print(f"Worker starting: sources={cfg['sources']} bbox={cfg['bbox']} "
          f"days={cfg['days']} interval={interval}min "
          f"osm_context={cfg['osm_context']}",
          flush=True)

    worker_id = os.getenv("HOSTNAME") or "worker-1"
    cycle_count = 0
    while not _shutdown:
        cycle = datetime.now()
        cycle_count += 1
        cycle_status, cycle_error, cycle_inserted = "success", None, 0
        try:
            with psycopg.connect(**_conn_info()) as conn:
                cycle_inserted = run_cycle(conn, map_key, cfg)
        except Exception as e:
            cycle_status, cycle_error = "failed", str(e)
            print(f"[{datetime.now():%H:%M:%S}] cycle failed: {e}",
                  file=sys.stderr)
        # Heartbeat uses its own connection so a failed ingestion cycle still
        # reports liveness to /api/health (write_heartbeat never raises).
        try:
            with psycopg.connect(**_conn_info()) as conn:
                write_heartbeat(
                    conn, worker_id, status=cycle_status, error=cycle_error,
                    inserted=cycle_inserted, cycle_count=cycle_count,
                    started_at=cycle,
                )
        except Exception as e:
            print(f"[{datetime.now():%H:%M:%S}] heartbeat write failed: {e}",
                  file=sys.stderr)
        if args.once or _shutdown:
            break
        # Sleep in small increments so SIGINT/SIGTERM is honoured promptly.
        slept = 0.0
        while slept < interval * 60.0 and not _shutdown:
            time.sleep(min(5.0, interval * 60.0 - slept))
            slept += 5.0

    print(f"Worker stopped (last cycle started {cycle.isoformat()}",
          flush=True)


if __name__ == "__main__":
    main()