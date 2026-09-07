"""Classifier validation against GIS-derived reference labels.

Reference labels are derived *independently* of the classifier rules:

    positive (industrial)  -- hotspot inside a facility polygon or within
                              ``CONFIDENT_DIST_M`` (200 m) of a confidently
                              industrial facility type (power/refinery/gas/
                              petrochemical/steel/mine/terminal). These types
                              are self-describing industry, not the generic
                              OSM ``landuse=industrial`` areas the classifier
                              also keys off.
    negative (non-industrial) -- hotspot further than ``FAR_DIST_M`` (5 km)
                              from EVERY known facility (so even the
                              classifier's NEAR_DIST_M tolerance cannot fire).
    uncertain               -- everything else (excluded from accuracy).

The automatic classifier label is judged correct for a positive reference
when it is one of the industrial classes (industrial_fire /
persistent_industrial_source / gas_flare), and for a negative reference when
it is non_industrial (unknown also reported but not counted correct).

Honest caveat printed with the report: the facility table is shared OSM data,
so this validates the classifier's geometric/temporal *rules* against the
same source with stricter thresholds. It is NOT independent ground truth
(field/satellite labels would be required for that).

Usage
    python scripts/validate_classifier.py [--limit 5000] [--out reports/classifier_validation.json]

Reads POSTGRES_* env vars (same as the rest of the pipeline).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict

import psycopg
from dotenv import load_dotenv

CONFIDENT_DIST_M = 2500.0
FAR_DIST_M = 5000.0
CONFIDENT_TYPES = {
    "thermal_power_plant", "refinery", "oil_gas_facility", "petrochemical",
    "steel_or_metal", "mining", "lng_or_storage_terminal",
}
INDUSTRIAL_LABELS = {"industrial_fire", "persistent_industrial_source",
                     "gas_flare"}

FETCH_SQL = """
SELECT h.id,
       h.latitude, h.longitude,
       h.frp,
       e.distance_m,
       e.within_facility,
       e.nearest_facility_id,
       e.nearest_facility_type,
       e.class,
       e.reasons
FROM hotspots h
JOIN hotspot_enrichment e ON e.hotspot_id = h.id
WHERE e.class IS NOT NULL
ORDER BY h.id
"""


def reference_label(row):
    """GIS-only reference label: positive / negative / uncertain."""
    dist = row["distance_m"]
    ftype = row["nearest_facility_type"]
    if row.get("within_facility"):
        return "positive"
    if dist is None:
        return "uncertain"
    if dist <= CONFIDENT_DIST_M and ftype in CONFIDENT_TYPES:
        return "positive"
    if dist >= FAR_DIST_M:
        return "negative"
    return "uncertain"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=None,
                        help="cap number of hotspots evaluated")
    parser.add_argument("--out", default=None, help="JSON report path")
    args = parser.parse_args()

    load_dotenv()
    conn_info = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    cols = ["id", "latitude", "longitude", "frp", "distance_m",
            "within_facility", "nearest_facility_id", "nearest_facility_type",
            "class", "reasons"]
    rows = []
    with psycopg.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(FETCH_SQL + (f" LIMIT {int(args.limit)}"
                                     if args.limit else ""))
            for raw in cur.fetchall():
                rows.append({k: v for k, v in zip(cols, raw)})

    ref_counts = Counter()
    confusion = defaultdict(Counter)   # ref -> pred
    examples_fp = []                   # ref positive, pred non-industrial/unknown
    examples_fn = []                   # ref negative, pred industrial
    correct = 0
    for row in rows:
        ref = reference_label(row)
        pred = row["class"] or "unknown"
        ref_counts[ref] += 1
        confusion[ref][pred] += 1
        if ref == "positive":
            if pred in INDUSTRIAL_LABELS:
                correct += 1
            else:
                if len(examples_fp) < 10:
                    examples_fp.append(_example(row, pred))
        elif ref == "negative":
            if pred == "non_industrial":
                correct += 1
            elif pred in INDUSTRIAL_LABELS:
                if len(examples_fn) < 10:
                    examples_fn.append(_example(row, pred))

    decided = ref_counts["positive"] + ref_counts["negative"]
    accuracy = (correct / decided) if decided else None
    uncertain = ref_counts["uncertain"]

    report = {
        "method": "GIS-derived reference labels (strict thresholds on shared "
                  "facility geodata; NOT independent ground truth)",
        "confident_distance_m": CONFIDENT_DIST_M,
        "far_distance_m": FAR_DIST_M,
        "confident_types": sorted(CONFIDENT_TYPES),
        "total_evaluated": len(rows),
        "ref_positive": ref_counts["positive"],
        "ref_negative": ref_counts["negative"],
        "ref_uncertain": uncertain,
        "agreement": correct,
        "accuracy_decided": round(accuracy, 4) if accuracy is not None else None,
        "confusion": {r: dict(c) for r, c in confusion.items()},
        "predicted_class_counts": dict(sum(
            (c for c in confusion.values()), Counter())),
        "false_positives_samples": examples_fp,
        "false_negatives_samples": examples_fn,
    }

    lines = []
    lines.append("Classifier validation (GIS-derived reference labels)")
    lines.append("=" * 60)
    lines.append(f"facility-table source shared with classifier (see caveat)")
    lines.append(f"positive: inside any facility polygon, or <="
                 f" {CONFIDENT_DIST_M:g} m of a confident type: "
                 f"{report['ref_positive']}")
    lines.append(f"negative >= {FAR_DIST_M:g} m from all facilities: "
                 f"{report['ref_negative']}")
    lines.append(f"uncertain (excluded): {uncertain}")
    lines.append(f"total evaluated: {len(rows)}")
    lines.append("")
    if accuracy is not None:
        lines.append(f"agreement on decided refs: {correct}/{decided} "
                     f"({accuracy:.1%})")
    lines.append("")
    lines.append("confusion (reference -> predicted):")
    for ref in ("positive", "negative", "uncertain"):
        row = confusion[ref]
        if not row:
            continue
        shown = ", ".join(f"{k}={v}" for k, v in sorted(row.items()))
        lines.append(f"  {ref:>9}: {shown}")
    lines.append("")
    lines.append("False positives (ref industrial, classified"
                 " non_industrial/unknown):")
    for ex in examples_fp:
        lines.append(f"  {ex}")
    lines.append("False negatives (ref non-industrial, classified"
                 " industrial):")
    for ex in examples_fn:
        lines.append(f"  {ex}")
    report_text = "\n".join(lines)
    print(report_text)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        print(f"\nJSON report written to {args.out}")


def _example(row, pred):
    return {
        "id": row["id"],
        "lat": row["latitude"], "lon": row["longitude"],
        "frp": row["frp"],
        "predicted": pred,
        "distance_m": row["distance_m"],
        "facility_type": row["nearest_facility_type"],
        "facility_id": row["nearest_facility_id"],
        "reasons": row["reasons"],
    }


if __name__ == "__main__":
    sys.exit(main())