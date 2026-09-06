"""Rule-based hotspot classifier (MVP).

Transparent, deterministic rules produce one of five classes with human-
readable reasons:

    industrial_fire                fire detected at/near an industrial facility
    persistent_industrial_source   repeated detections near industry (e.g. kilns)
    gas_flare                      bright/persistent detection at a gas/thermal site
    non_industrial                 thermal anomaly far from any known industrial context
    unknown                        insufficient data to classify

The classifier is deliberately feature-based so a vision-model probability can
be added later as one more feature without reworking the rule engine:

    features["vision_fire_prob"]   <- set externally by the vision pipeline
    features["vision_flare_prob"]  <- set externally by the vision pipeline

Rules are kept in ``RULE_CHAIN`` and evaluated in order; every rule appends at
least one reason explaining the decision.
"""

# Tunable thresholds (shared by the enrichment + classifier pipeline).
WITHIN_DIST_M = 1500.0        # "at the facility" distance to polygon boundary
NEAR_DIST_M = 5000.0          # "near industry" distance to polygon boundary
HIGH_FRP = 15.0               # satellite FRP above this is "bright"
PERSISTENCE_HIGH = 0.5        # persistence score above this is "repeating"
NEARBY_COUNT_FOR_PERSISTENCE = 5  # denominator for the persistence score

# Facility types treated as gas / thermal / flare-prone.
GAS_RELATED_TYPES = {
    "oil_gas_facility",
    "lng_or_storage_terminal",
    "petrochemical",
    "refinery",
    "thermal_power_plant",
}

CLASSES = (
    "industrial_fire",
    "persistent_industrial_source",
    "gas_flare",
    "non_industrial",
    "unknown",
)


def build_features(hotspot, enrichment):
    """Normalize raw hotspot + enrichment rows into classifier features.

    ``hotspot``      -- dict with at least ``frp``.
    ``enrichment``   -- dict with nearest-facility fields and temporal metrics.
    Keys the rules rely on (always present, with sensible defaults).
    """
    frp = hotspot.get("frp")
    try:
        frp = float(frp) if frp is not None else None
    except (TypeError, ValueError):
        frp = None

    dist = enrichment.get("distance_m")
    try:
        dist = float(dist) if dist is not None else None
    except (TypeError, ValueError):
        dist = None

    persistence = enrichment.get("persistence_score")
    try:
        persistence = float(persistence) if persistence is not None else 0.0
    except (TypeError, ValueError):
        persistence = 0.0

    return {
        "frp": frp,
        "distance_m": dist,
        "within_facility": bool(enrichment.get("within_facility", False)),
        "facility_type": enrichment.get("nearest_facility_type") or None,
        "facility_name": enrichment.get("nearest_facility_name") or None,
        "persistence_score": persistence,
        "nearby_detections": int(enrichment.get("nearby_detections_7d") or 0),
        "frp_vs_nearby": enrichment.get("frp_vs_nearby"),
        # Vision-model seams (added by the vision pipeline later).
        "vision_fire_prob": enrichment.get("vision_fire_prob"),
        "vision_flare_prob": enrichment.get("vision_flare_prob"),
    }


def _fmts(dist):
    return f"{dist:,.0f}"


def apply_rules(f):
    """Return ``(label, reasons)`` for feature dict ``f`` using RULE_CHAIN."""
    for label, fn in RULE_CHAIN:
        result = fn(f)
        if result is not None:
            return label, [r for r in result if r]
    return "unknown", ["no rule produced a decision"]


def _r_gas_flare(f):
    """Bright or persistent detection at a gas/thermal/refinery site."""
    if not f["within_facility"] and not (
        f["distance_m"] is not None and f["distance_m"] <= WITHIN_DIST_M
    ):
        return None
    if f["facility_type"] not in GAS_RELATED_TYPES:
        return None
    high_frp = f["frp"] is not None and f["frp"] >= HIGH_FRP
    persisting = f["persistence_score"] >= PERSISTENCE_HIGH
    if not (high_frp or persisting):
        return None
    reasons = [
        f"at/near gas or thermal site {_fac_or_name(f)} "
        f"(distance {_fmts(f['distance_m'])} m)"
    ]
    if high_frp:
        reasons.append(f"FRP {f['frp']:.1f} >= {HIGH_FRP:g} (bright)")
    if persisting:
        reasons.append(
            f"persistence {f['persistence_score']:.2f} "
            f"({f['nearby_detections']} detections in 7d)"
        )
    return reasons


def _r_persistent_industrial(f):
    """Repeated detections within a few km of industry (steady source)."""
    if not (f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M):
        return None
    if f["persistence_score"] < PERSISTENCE_HIGH:
        return None
    return [
        f"within {_fmts(f['distance_m'])} m of {_fac_or_name(f)}",
        f"persistence {f['persistence_score']:.2f} "
        f"({f['nearby_detections']} detections in 7d) suggests a steady source",
    ]


def _r_industrial_fire(f):
    """A recent (non-persistent) detection near industrial infrastructure."""
    if not (f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M):
        return None
    if f["persistence_score"] >= PERSISTENCE_HIGH:
        return None  # persistent detections are handled by the rule above
    if f["within_facility"]:
        return [
            "hotspot lies inside facility polygon",
            f"facility {_fac_or_name(f)}",
        ]
    return [
        f"single recent detection within {_fmts(f['distance_m'])} m "
        f"of {_fac_or_name(f)}"
    ]


def _r_non_industrial(f):
    """Thermal anomaly far from known industrial context."""
    if f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M:
        return None
    if f["within_facility"]:
        return None
    if f["distance_m"] is None:
        return None  # no facility data at all -> leave to _r_unknown
    reasons = [f"nearest facility is {_fmts(f['distance_m'])} m away"]
    if f["nearby_detections"] > 0:
        reasons.append(
            f"{f['nearby_detections']} detections near this location in 7d "
            "(likely agricultural/vegetation fire clusters)"
        )
    return reasons


def _r_unknown(f):
    """No industrial-facility context is available to compare against."""
    if f["distance_m"] is not None or f["facility_type"]:
        return None
    return ["no industrial facility data available for this hotspot"]


def _fac_or_name(f):
    name = (f.get("facility_name") or "").strip()
    ftype = f.get("facility_type") or "unknown facility"
    return f'"{name}" ({ftype})' if name else ftype


# Evaluation order matters: gas first, then persistent steady sources, then
# one-off fires near industry, then far-from-industry, then unknown.
RULE_CHAIN = (
    ("gas_flare", _r_gas_flare),
    ("persistent_industrial_source", _r_persistent_industrial),
    ("industrial_fire", _r_industrial_fire),
    ("non_industrial", _r_non_industrial),
    ("unknown", _r_unknown),
)


def classify(hotspot, enrichment):
    """Classify a hotspot; returns ``(label, reasons)``."""
    label, reasons = apply_rules(build_features(hotspot, enrichment))
    if label not in CLASSES:
        label = "unknown"
    return label, reasons


if __name__ == "__main__":
    # Lightweight self-check (no DB/network needed).
    def show(name, hotspot, enrichment):
        label, reasons = classify(hotspot, enrichment)
        print(f"{name:>28} -> {label}")
        for r in reasons:
            print(f"      - {r}")

    show("at gas facility bright",
         {"frp": 40.0},
         {"distance_m": 200, "nearest_facility_type": "oil_gas_facility",
          "persistence_score": 0.8, "nearby_detections_7d": 6})
    show("at oil facility weak",
         {"frp": 2.0},
         {"distance_m": 300, "nearest_facility_type": "oil_gas_facility",
          "persistence_score": 0.2, "nearby_detections_7d": 1})
    show("inside steel plant",
         {"frp": 25.0},
         {"distance_m": 0, "within_facility": True,
          "nearest_facility_type": "steel_or_metal", "persistence_score": 0.1})
    show("inside steel plant persistent",
         {"frp": 25.0},
         {"distance_m": 0, "within_facility": True,
          "nearest_facility_type": "steel_or_metal", "persistence_score": 0.8,
          "nearby_detections_7d": 7})
    show("4km from works persistent",
         {"frp": 8.0},
         {"distance_m": 4000, "nearest_facility_type": "general_factory",
          "persistence_score": 0.9, "nearby_detections_7d": 8})
    show("2.7km from mine single",
         {"frp": 6.0},
         {"distance_m": 2700, "nearest_facility_type": "mining",
          "persistence_score": 0.0, "nearby_detections_7d": 0})
    show("1km from works single",
         {"frp": 5.0},
         {"distance_m": 1000, "nearest_facility_type": "general_factory",
          "persistence_score": 0.0, "nearby_detections_7d": 0})
    show("far from industry",
         {"frp": 12.0},
         {"distance_m": 120000, "persistence_score": 0.6,
          "nearby_detections_7d": 4})
    show("no context",
         {"frp": 10.0},
         {"distance_m": None, "nearest_facility_type": None,
          "persistence_score": 0.0})