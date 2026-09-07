"""Gated enrichment: enrich ONLY industrial-context candidate hotspots.

Candidates = hotspots with no keep-class that are within the facility buffer,
inside a facility polygon, or have strong vision/industrial evidence.  Far,
context-less rows are NOT enriched (they are prune candidates).

Runs in a batch loop (memory-bounded). Prints final class counts.
"""
import json
import os
import sys
from dotenv import load_dotenv
import psycopg

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import enrich_hotspots  # noqa: E402

BUFFER_M = float(os.getenv("PRUNE_FAR_FROM_FACILITY_M", "5000"))
BATCH = int(os.getenv("PRUNE_BATCH", "20000"))

KEEP_CLASSES = (
    "industrial_fire",
    "persistent_industrial_source",
    "gas_flare",
)

CANDIDATE_SQL = r"""
SELECT h.id
FROM hotspots h
LEFT JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE (e.class IS NULL OR NOT (e.class = ANY(%(keep)s::text[])))
  AND (
    COALESCE(e.vision_industrial_prob > 0.6, FALSE)
    OR EXISTS (
      SELECT 1 FROM industrial_facilities f
      WHERE f.geometry IS NOT NULL
        AND f.geometry && CAST(h.location AS geometry)
        AND ST_Covers(f.geometry, CAST(h.location AS geometry))
    )
    OR EXISTS (
      SELECT 1 FROM industrial_facilities f
      WHERE f.location IS NOT NULL
        AND ST_DWithin(h.location, f.location, %(buf)s)
    )
  )
AND h.id > %(after)s AND h.id <= %(upto)s
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


def candidate_ids(conn):
    keep_arr = "{" + ",".join(KEEP_CLASSES) + "}"
    ids = []
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(id), MAX(id) FROM hotspots")
        lo, hi = cur.fetchone()
        if lo is None:
            return ids
        after = lo - 1
        while after < hi:
            upto = min(after + BATCH, hi)
            cur.execute(
                CANDIDATE_SQL,
                {"keep": keep_arr, "buf": BUFFER_M, "after": after, "upto": upto},
            )
            ids.extend(r[0] for r in cur.fetchall())
            after = upto
    return ids


def main():
    with psycopg.connect(**conn_info()) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
        ids = candidate_ids(conn)
        print(f"Candidate rows to enrich: {len(ids):,}")
        if not ids:
            return

        # Enrich in chunks to bound transaction size.
        chunk = 5000
        for i in range(0, len(ids), chunk):
            part = ids[i:i + chunk]
            summary = enrich_hotspots.enrich(conn, hotspot_ids=part)
            conn.commit()
            print(
                f"  enriched {len(part):,} "
                f"(total reporters {sum(summary['class_counts'].values()):,})")

    # Final class counts after all chunks.
    with psycopg.connect(**conn_info()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT class, COUNT(*) FROM hotspot_enrichment "
                "GROUP BY class ORDER BY 2 DESC")
            print("FINAL CLASS COUNTS across enriched rows:")
            for cls, n in cur.fetchall():
                print(f"  {str(cls):>30}: {n:,}")


if __name__ == "__main__":
    main()