"""Retention pruning for raw FIRMS hotspots.

Keeps classified-industrial detections forever (they are the product);
drops non-industrial / unclassified raw rows older than ``RAW_RETENTION_DAYS``
(floor 30 days so temporal baselines still have history).

FK-safe: ``hotspot_enrichment`` rows are deleted before their parent
``hotspots`` rows. Never touches ``ingestion_run_log`` or
``industrial_facilities``. Logs counts only (no row contents).
"""

import argparse
import os

import psycopg

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

INDUSTRIAL_CLASSES = (
    "industrial_fire",
    "persistent_industrial_source",
    "gas_flare",
)
MIN_RETENTION_DAYS = 30
DEFAULT_RETENTION_DAYS = 30
BATCH_SIZE = 1000


def retention_days(value=None):
    """Normalize a requested retention to the enforced floor (min 30)."""
    if value is None:
        value = os.getenv("RAW_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS))
    days = int(value)
    return max(days, MIN_RETENTION_DAYS)


def _conn_info():
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        **({"sslmode": os.getenv("POSTGRES_SSLMODE")}
           if os.getenv("POSTGRES_SSLMODE") else {}),
    }


_SELECT_DOOMED = """
SELECT DISTINCT h.id
FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE h.acq_date < (CURRENT_DATE - %(days)s)::date
  AND (e.class IS NULL OR e.class NOT IN %(keep)s)
LIMIT %(batch)s
"""


def prune(conn, retention_days_value=None, batch=BATCH_SIZE, dry_run=False):
    """Delete (or count, if ``dry_run``) prunable rows. Returns a summary dict."""
    days = retention_days(retention_days_value)
    params = {
        "days": days,
        "keep": list(INDUSTRIAL_CLASSES),
        "batch": batch,
    }
    deleted_hotspots = 0
    total_scanned = 0
    with conn.cursor() as cur:
        while True:
            cur.execute(_SELECT_DOOMED, params)
            doomed = [row[0] for row in cur.fetchall()]
            if not doomed:
                break
            if dry_run:
                total_scanned += len(doomed)
                if len(doomed) < batch:
                    break
                continue
            cur.execute(
                "DELETE FROM hotspot_enrichment WHERE hotspot_id = ANY(%s)",
                (doomed,),
            )
            cur.execute(
                "DELETE FROM hotspots WHERE id = ANY(%s)", (doomed,),
            )
            deleted_hotspots += len(doomed)
            conn.commit()
            if len(doomed) < batch:
                break
    return {
        "retention_days": days,
        "floor_days": MIN_RETENTION_DAYS,
        "dry_run": dry_run,
        "would_prune": total_scanned if dry_run else 0,
        "pruned": deleted_hotspots,
        "kept_classes": list(INDUSTRIAL_CLASSES),
    }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=None,
                   help=f"retention window (min {MIN_RETENTION_DAYS}); "
                        f"defaults to RAW_RETENTION_DAYS env")
    p.add_argument("--batch", type=int, default=BATCH_SIZE)
    p.add_argument("--dry-run", action="store_true",
                   help="count only; delete nothing")
    args = p.parse_args(argv)

    with psycopg.connect(**_conn_info()) as conn:
        out = prune(conn, args.days, args.batch, dry_run=args.dry_run)
    verb = "WOULD PRUNE" if args.dry_run else "PRUNED"
    print(f"{verb} {out['would_prune'] or out['pruned']} raw rows "
          f"(retention {out['retention_days']} days, floor {out['floor_days']})")
    return out


if __name__ == "__main__":
    main()