"""Dry-run / analysis for pruning production hotspots to industrial-relevant rows.

For every hotspot it computes the nearest-facility distance (indexed LATERAL,
same as enrichment) and, combined with any existing enrichment/class, assigns a
disposition:

    KEEP_FOREVER  -- already classified into an industrial class
                    (industrial_fire, persistent_industrial_source, gas_flare)
    KEEP_NEAR     -- within PRUNE_FAR_FROM_FACILITY_M of a facility, OR inside a
                    facility polygon, OR strong vision/land-cover evidence.
                    Not yet classified -> candidate for gated enrichment.
    DELETE        -- far from every facility with no industrial evidence and no
                    keep-class. Proposed for deletion (NOT executed in dry-run).

Only prints a report.  Run with --commit to actually delete.

ENV:
    PRUNE_FAR_FROM_FACILITY_M   facility buffer (meters, default 5000)
    PRUNE_BATCH                 rows per batch (default 50000)
    POSTGRES_*                  connection (pooler)
"""
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
import psycopg

load_dotenv()

BUFFER_M = float(os.getenv("PRUNE_FAR_FROM_FACILITY_M", "5000"))
BATCH = int(os.getenv("PRUNE_BATCH", "20000"))

KEEP_CLASSES = (
    "industrial_fire",
    "persistent_industrial_source",
    "gas_flare",
)

INDUSTRIAL_CLASSES = KEEP_CLASSES  # alias: canonical industrial class set

MIN_RETENTION_DAYS = 30


def retention_days(past_days=None):
    """Floor for how far back raw rows may be pruned.

    ``past_days`` explicitly passed wins unless below the 30-day floor;
    otherwise the RAW_RETENTION_DAYS env is used (default 30).
    """
    if past_days is None:
        past_days = int(os.getenv("RAW_RETENTION_DAYS", "30"))
    return max(int(past_days), MIN_RETENTION_DAYS)

# Set-based industrial-context flags (index-backed, no per-row LATERAL):
#   inside   -- lies within a facility polygon
#   near     -- within BUFFER_M of any facility point/geometry boundary
DISPOSITION_SQL = r"""
SELECT
  h.id,
  COALESCE(e.class, NULL) AS class,
  COALESCE(e.vision_industrial_prob > 0.6, FALSE) AS vision,
  COALESCE(
    EXISTS (
      SELECT 1 FROM industrial_facilities f
      WHERE f.geometry IS NOT NULL
        AND f.geometry && CAST(h.location AS geometry)
        AND ST_Covers(f.geometry, CAST(h.location AS geometry))
    ),
    FALSE) AS inside,
  COALESCE(
    EXISTS (
      SELECT 1 FROM industrial_facilities f
      WHERE f.location IS NOT NULL
        AND ST_DWithin(h.location, f.location, %(buf)s)
    ),
    FALSE) AS near
FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE h.id > %(after)s AND h.id <= %(upto)s
"""


def conn_info():
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "sslmode": os.getenv("POSTGRES_SSLMODE", "prefer"),
        "prepare_threshold": None,
    }


def id_bounds(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(id), MAX(id), COUNT(*) FROM hotspots")
        return cur.fetchone()


def analyze(conn):
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute("SELECT COUNT(*) FROM hotspots WHERE id IS NOT NULL")
        total = cur.fetchone()[0]

    keep_forever = 0
    keep_near = 0
    delete_cand = 0
    geared_keep_near_unclassified = 0

    lo, hi, _ = id_bounds(conn)
    if lo is None:
        return {
            "total": total,
            "keep_forever": 0, "keep_near": 0, "delete_cand": 0,
            "unclassified_near": 0,
        }

    after = lo - 1
    while after < hi:
        upto = min(after + BATCH, hi)
        with conn.cursor() as cur:
            cur.execute(DISPOSITION_SQL, {"after": after, "upto": upto, "buf": BUFFER_M})
            rows = cur.fetchall()
        for (hid, cls, vision, inside, near) in rows:
            if cls in KEEP_CLASSES:
                keep_forever += 1
                continue
            if inside or vision or near:
                keep_near += 1
                if cls is None:
                    geared_keep_near_unclassified += 1
                continue
            delete_cand += 1
        after = upto

    return {
        "total": total,
        "keep_forever": keep_forever,
        "keep_near": keep_near,
        "delete_cand": delete_cand,
        "unclassified_near": geared_keep_near_unclassified,
    }


def _fmt(n):
    return f"{n:,}"


def report(res, conn):
    total = res["total"]
    keep = res["keep_forever"] + res["keep_near"]
    delete = res["delete_cand"]
    print("=" * 62)
    print("PRUNE DRY-RUN  (facility buffer = %s m)" % f"{BUFFER_M:,.0f}")
    print("-" * 62)
    print(f"  Current hotspots            : {_fmt(total)}")
    print(f"  KEEP forever (classified)   : {_fmt(res['keep_forever'])}")
    print(f"  KEEP near-facility (ambig)  : {_fmt(res['keep_near'])}")
    print(f"      of which unclassified   : {_fmt(res['unclassified_near'])}  <- candidates to enrich")
    print(f"  DELETE candidates (non-ind) : {_fmt(delete)}")
    print(f"  Totals keep={_fmt(keep)}  delete={_fmt(delete)}")

    # DB size estimate for kept rows.
    est = _size_after(conn, keep)
    print("-" * 62)
    print(f"  Estimated rows after prune  : {_fmt(keep)}")
    print(f"  Estimated DB size after     : {est:.1f} MB (vs ~{_size_now(conn):.0f} MB now)")
    print("=" * 62)
    print("Run with --commit to execute the delete.")


def _usable(conn):
    """Return a live connection (fresh if ``conn`` is None/closed)."""
    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except psycopg.OperationalError:
            pass
    return psycopg.connect(**conn_info())


def _size_now(conn=None):
    c = _usable(conn)
    with c.cursor() as cur:
        cur.execute(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)/1e6),0) FROM pg_class c "
            "WHERE c.relname IN ('hotspots','hotspot_enrichment','industrial_facilities')")
        return float(cur.fetchone()[0])


def _size_after(conn, keep_rows):
    # Approximate: hotspots table size scales with rowcount; drop orphaned
    # enrichment (FK cascade removes it) too. Facilities unchanged.
    c = _usable(conn)
    with c.cursor() as cur:
        cur.execute(
            "SELECT pg_total_relation_size('hotspots'::regclass)/1e6,"
            "       pg_total_relation_size('hotspot_enrichment'::regclass)/1e6,"
            "       pg_total_relation_size('industrial_facilities'::regclass)/1e6")
        hs, en, fac = (float(x) for x in cur.fetchone())
    with c.cursor() as cur:
        cur.execute("SELECT count(*) FROM hotspots")
        total = cur.fetchone()[0]
    hs_new = hs * (keep_rows / total) if total else 0
    en_new = 0.0  # orphaned enrichment removed via CASCADE; kept rows get fresh enrich
    return hs_new + en_new + fac


def do_commit(conn):
    keep_arr = "{" + ",".join(KEEP_CLASSES) + "}"
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        lo, hi, _ = id_bounds(conn)
        after = lo - 1
        deleted = 0
        while after < hi:
            upto = min(after + BATCH, hi)
            cur.execute(
                """
                DELETE FROM hotspots h
                WHERE h.id > %(after)s AND h.id <= %(upto)s
                  AND NOT EXISTS (
                    SELECT 1 FROM hotspot_enrichment e
                    WHERE e.hotspot_id = h.id AND e.class = ANY(%(keep)s::text[])
                  )
                  AND NOT COALESCE((
                    SELECT e.vision_industrial_prob > 0.6
                    FROM hotspot_enrichment e
                    WHERE e.hotspot_id = h.id
                  ), FALSE)
                  AND NOT EXISTS (
                    SELECT 1 FROM industrial_facilities f
                    WHERE f.location IS NOT NULL
                      AND ST_DWithin(h.location, f.location, %(buf)s)
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM industrial_facilities f
                    WHERE f.geometry IS NOT NULL
                      AND f.geometry && CAST(h.location AS geometry)
                      AND ST_Covers(f.geometry, CAST(h.location AS geometry))
                  )
                """,
                {"after": after, "upto": upto,
                 "keep": keep_arr, "buf": BUFFER_M},
            )
            deleted += cur.rowcount
            conn.commit()
            after = upto
        return deleted


def main():
    commit = "--commit" in sys.argv
    with psycopg.connect(**conn_info()) as conn:
        res = analyze(conn)
    report(res, conn)
    if commit:
        print("COMMIT requested; executing delete...")
        with psycopg.connect(**conn_info()) as conn:
            n = do_commit(conn)
            print(f"Deleted {_fmt(n)} non-industrial rows.")
    else:
        print("(dry-run only; no rows changed)")


if __name__ == "__main__":
    main()
