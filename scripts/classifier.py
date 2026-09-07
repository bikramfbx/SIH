"""Transparent rule-based hotspot classifier (final backend).

Deterministic rules produce one of five classes with human-readable reasons:

    industrial_fire                sudden/large event at or near industry
    persistent_industrial_source   repeated, fairly stable detections near industry
    gas_flare                      bright/recurrent detection at a gas/thermal site
    non_industrial                 thermal anomaly far from known industrial context
    unknown                        insufficient data to classify

The classifier consumes FIRMS thermal features, GIS industrial context and
temporal/historical features. It also ACCEPTS optional vision-model signals
(``vision_industrial_prob``, ``vision_fire_prob``, ``vision_flare_prob``,
``vision_facility_type``, ``vision_model_version``) and treats them as
additional evidence. When those fields are absent/null the classification
still works using FIRMS + GIS + temporal features only -- the backend never
depends on a vision model.

Rules are kept in ``RULE_CHAIN`` (evaluated in order) and are deliberately
simple and editable; they are not claimed to be scientifically perfect.
"""

# Tunable thresholds (shared by the enrichment + classifier pipeline).
WITHIN_DIST_M = 1500.0            # "at the facility" distance to polygon boundary
NEAR_DIST_M = 5000.0              # "near industry" distance to polygon boundary
HIGH_FRP = 15.0                   # satellite FRP above this is "bright"
PERSISTENCE_HIGH = 0.5            # persistence >= this is "repeating"
NEARBY_COUNT_FOR_PERSISTENCE = 5  # denominator for the persistence score
FRP_SURGE_RATIO = 2.5             # current FRP / 30-d mean above this = sudden surge
NIGHT_FRAC_STRONG = 0.6           # nighttime share above this = strong night recurrence
GAS_MIN_DETECTIONS = 3            # minimum 7-d detections for gas recurrence
VISION_INDUSTRY_PROB = 0.8        # vision: "industrial" evidence threshold
VISION_FIRE_PROB = 0.7            # vision: "active fire" evidence threshold
VISION_FLARE_PROB = 0.6           # vision: "flare" evidence threshold

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


def _f(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _i(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def build_features(hotspot, enrichment):
    """Normalize raw hotspot + enrichment rows into classifier features.

    ``hotspot``      -- dict with at least ``frp`` (and optionally ``daynight``).
    ``enrichment``   -- dict with nearest-facility fields, temporal metrics and
                        (optionally) vision-model fields.
    Missing keys default to falsy/NULL so the rules never raise.
    """
    frp = _f(hotspot.get("frp"))
    dist = _f(enrichment.get("distance_m"))
    persistence = _f(enrichment.get("persistence_score")) or 0.0
    current_ratio = _f(enrichment.get("current_frp_vs_historical_mean"))
    night_frac = _f(enrichment.get("nighttime_detection_fraction"))

    features = {
        "frp": frp,
        "daynight": hotspot.get("daynight"),
        "distance_m": dist,
        "within_facility": bool(enrichment.get("within_facility", False)),
        "facility_type": enrichment.get("nearest_facility_type") or None,
        "facility_name": enrichment.get("nearest_facility_name") or None,
        "detections_24h": _i(enrichment.get("detections_24h")),
        "detections_7d": _i(enrichment.get("detections_7d")),
        "detections_30d": _i(enrichment.get("detections_30d")),
        "days_active_30d": _i(enrichment.get("days_active_30d")),
        "persistence_score": persistence,
        "mean_frp_7d": _f(enrichment.get("mean_frp_7d")),
        "mean_frp_30d": _f(enrichment.get("mean_frp_30d")),
        "max_frp_30d": _f(enrichment.get("max_frp_30d")),
        "current_frp_vs_historical_mean": current_ratio,
        "nighttime_detection_fraction": night_frac,
        "frp_vs_nearby": _f(enrichment.get("frp_vs_nearby")),
        # Vision-model seams (optional; provided by my future vision pipeline).
        "vision_industrial_prob": _f(enrichment.get("vision_industrial_prob")),
        "vision_fire_prob": _f(enrichment.get("vision_fire_prob")),
        "vision_flare_prob": _f(enrichment.get("vision_flare_prob")),
        "vision_facility_type": enrichment.get("vision_facility_type") or None,
        "vision_model_version": enrichment.get("vision_model_version") or None,
        # Land-cover seam (optional; populated only when a land-cover provider
        # is reachable and LAND_COVER=1). Never on the decision path on its
        # own -- it only adds a documentation reason line when present.
        "landcover_class": enrichment.get("landcover_class") or None,
    }
    return features


def _fac_or_name(f):
    name = (f.get("facility_name") or "").strip()
    ftype = f.get("facility_type") or "unknown facility"
    return f'"{name}" ({ftype})' if name else ftype


def _fmt_dist(m):
    return f"{m:,.0f}"


def _has_vision(f):
    return any(
        f[k] is not None
        for k in ("vision_industrial_prob", "vision_fire_prob", "vision_flare_prob")
    )


def _vision_reasons(f):
    reasons = []
    if f["vision_industrial_prob"] is not None:
        reasons.append(
            f"vision industrial probability {f['vision_industrial_prob']:.2f}")
    if f["vision_fire_prob"] is not None:
        reasons.append(f"vision fire probability {f['vision_fire_prob']:.2f}")
    if f["vision_flare_prob"] is not None:
        reasons.append(f"vision flare probability {f['vision_flare_prob']:.2f}")
    if f["vision_model_version"]:
        reasons.append(f"vision model version {f['vision_model_version']}")
    return reasons


def apply_rules(f):
    """Return ``(label, reasons)`` for feature dict ``f`` using RULE_CHAIN."""
    for label, fn in RULE_CHAIN:
        result = fn(f)
        if result is not None:
            reasons = [r for r in result if r]
            if _has_vision(f):
                reasons += _vision_reasons(f)
            if f.get("landcover_class"):
                reasons.append(f"land cover: {f['landcover_class']} (informational)")
            return label, reasons
    return "unknown", ["no rule produced a decision"]


def _r_gas_flare(f):
    """Bright/recurrent detection at a gas/thermal/refinery site, or a
    strong vision flare signal."""
    at_gas_site = (
        f["within_facility"]
        or (f["distance_m"] is not None and f["distance_m"] <= WITHIN_DIST_M)
    ) and f["facility_type"] in GAS_RELATED_TYPES

    vision_flare = (
        f["vision_flare_prob"] is not None
        and f["vision_flare_prob"] >= VISION_FLARE_PROB
    )
    if not (at_gas_site or vision_flare):
        return None

    high_frp = f["frp"] is not None and f["frp"] >= HIGH_FRP
    persisting = f["persistence_score"] >= PERSISTENCE_HIGH
    recurrent = f["detections_7d"] >= GAS_MIN_DETECTIONS or f["days_active_30d"] >= 3
    night = (
        f["nighttime_detection_fraction"] is not None
        and f["nighttime_detection_fraction"] >= NIGHT_FRAC_STRONG
    )
    surging = (
        f["current_frp_vs_historical_mean"] is not None
        and f["current_frp_vs_historical_mean"] >= FRP_SURGE_RATIO
    )

    if not (high_frp or persisting or recurrent or night or surging or vision_flare):
        return None

    reasons = [f"gas-flare candidate site {_fac_or_name(f)}"]
    if high_frp:
        reasons.append(f"FRP {f['frp']:.1f} >= {HIGH_FRP:g} (bright)")
    if recurrent:
        reasons.append(
            f"{f['detections_7d']} detections in 7 d, "
            f"{f['days_active_30d']} active days in 30 d (recurrent)")
    if night:
        reasons.append(
            f"nighttime fraction {f['nighttime_detection_fraction']:.2f} "
            f">= {NIGHT_FRAC_STRONG:g} (night recurrence)")
    if persisting:
        reasons.append(
            f"persistence {f['persistence_score']:.2f} "
            f"({f['detections_7d']} detections in 7 d)")
    if surging:
        reasons.append(
            f"FRP {f['frp']:.1f} is {f['current_frp_vs_historical_mean']:.1f}x "
            f"the 30-d mean (burst)")
    return reasons


def _r_persistent_industrial(f):
    """Repeated, fairly stable detections within a few km of industry."""
    if not (f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M):
        return None
    repeating = (
        f["detections_7d"] >= NEARBY_COUNT_FOR_PERSISTENCE
        or f["days_active_30d"] >= 3
        or f["persistence_score"] >= PERSISTENCE_HIGH
    )
    if not repeating:
        return None
    if (
        f["current_frp_vs_historical_mean"] is not None
        and f["current_frp_vs_historical_mean"] >= FRP_SURGE_RATIO
    ):
        return None  # a surge is a fire candidate, handled below

    reasons = [
        f"within {_fmt_dist(f['distance_m'])} m of {_fac_or_name(f)}",
        f"{f['detections_7d']} detections in 7 d, "
        f"{f['days_active_30d']} active days in 30 d",
        "FRP is steady relative to the historical mean"
        + (
            f" (ratio {f['current_frp_vs_historical_mean']:.1f})"
            if f["current_frp_vs_historical_mean"] is not None
            else ""
        ),
    ]
    return reasons


def _r_industrial_fire(f):
    """Sudden/large thermal event at or near industrial infrastructure.

    Near industry:
        * a new detection (low persistence), or
        * current FRP surging well above the 30-d baseline.
    Vision fallback: a high vision industrial/fire probability can mark a
    hotspot industrial even when no facility is stored yet (future proofing
    for missing OSM context).
    """
    near_industry = (
        f["within_facility"]
        or (f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M)
    )
    vision_industrial = (
        f["vision_industrial_prob"] is not None
        and f["vision_industrial_prob"] >= VISION_INDUSTRY_PROB
    )
    vision_fire = (
        f["vision_fire_prob"] is not None
        and f["vision_fire_prob"] >= VISION_FIRE_PROB
    )

    if not (near_industry or vision_industrial or vision_fire):
        return None

    if f["persistence_score"] >= PERSISTENCE_HIGH:
        surging = (
            f["current_frp_vs_historical_mean"] is not None
            and f["current_frp_vs_historical_mean"] >= FRP_SURGE_RATIO
        )
        if not surging:
            return None  # steady repeat -> persistent source rule already handled

    reasons = []
    if f["within_facility"]:
        reasons.append(f"hotspot lies inside {_fac_or_name(f)} polygon")
    elif f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M:
        reasons.append(
            f"within {_fmt_dist(f['distance_m'])} m of {_fac_or_name(f)}")
    if (
        f["current_frp_vs_historical_mean"] is not None
        and f["current_frp_vs_historical_mean"] >= FRP_SURGE_RATIO
    ):
        reasons.append(
            f"FRP {f['frp']:.1f} is {f['current_frp_vs_historical_mean']:.1f}x "
            f"the 30-d mean (sudden event)")
    elif f["detections_7d"] <= 1:
        reasons.append(f"new detection ({f['detections_7d']} prior detections in 7 d)")
    if not reasons:
        if vision_fire:
            reasons.append(
                f"vision fire probability {f['vision_fire_prob']:.2f} >= "
                f"{VISION_FIRE_PROB:g}")
        if vision_industrial:
            reasons.append(
                f"vision industrial probability {f['vision_industrial_prob']:.2f} "
                f">= {VISION_INDUSTRY_PROB:g}")
    if f["frp"] is not None and f["frp"] >= HIGH_FRP:
        reasons.append(f"FRP {f['frp']:.1f} >= {HIGH_FRP:g} (bright)")
    return reasons


def _r_non_industrial(f):
    """Thermal anomaly far from any known industrial context."""
    if f["distance_m"] is None and not f["facility_type"]:
        return None  # no facility data at all -> _r_unknown
    if f["within_facility"] or (
        f["distance_m"] is not None and f["distance_m"] <= NEAR_DIST_M
    ):
        return None
    if (
        f["vision_industrial_prob"] is not None
        and f["vision_industrial_prob"] >= VISION_INDUSTRY_PROB
    ):
        return None
    if f["vision_fire_prob"] is not None and f["vision_fire_prob"] >= VISION_FIRE_PROB:
        return None

    reasons = [f"nearest facility is {_fmt_dist(f['distance_m'])} m away"]
    if f["detections_7d"] > 0:
        reasons.append(
            f"{f['detections_7d']} detections in 7 d, "
            f"{f['days_active_30d']} active days in 30 d here "
            "(likely agricultural/vegetation fire cluster)")
    return reasons


def _r_unknown(f):
    """No industrial-facility context is available to compare against."""
    if f["distance_m"] is not None or f["facility_type"]:
        return None
    if f["vision_industrial_prob"] is not None:
        return None  # vision evidence means we should not call it unknown
    return ["no industrial facility data available for this hotspot"]


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
    def show(name, hotspot, enrichment=()):
        label, reasons = classify(hotspot, dict(enrichment))
        print(f"{name:>34} -> {label}")
        for r in reasons:
            print(f"      - {r}")

    show("at gas facility bright",
         {"frp": 40.0},
         {"distance_m": 200, "nearest_facility_type": "oil_gas_facility",
          "persistence_score": 0.8, "detections_7d": 6, "days_active_30d": 5})
    show("at gas facility night recurrent weak-frp",
         {"frp": 5.0},
         {"distance_m": 200, "nearest_facility_type": "refinery",
          "detections_7d": 4, "nighttime_detection_fraction": 0.85,
          "current_frp_vs_historical_mean": 1.2})
    show("inside steel plant surge",
         {"frp": 40.0},
         {"distance_m": 0, "within_facility": True,
          "nearest_facility_type": "steel_or_metal",
          "persistence_score": 0.1, "detections_7d": 0,
          "mean_frp_30d": 5.0, "max_frp_30d": 30.0,
          "current_frp_vs_historical_mean": 8.0})
    show("inside steel plant persistent stable",
         {"frp": 25.0},
         {"distance_m": 0, "within_facility": True,
          "nearest_facility_type": "steel_or_metal",
          "persistence_score": 0.8, "detections_7d": 7, "days_active_30d": 6,
          "current_frp_vs_historical_mean": 1.1})
    show("4km from works persistent",
         {"frp": 8.0},
         {"distance_m": 4000, "nearest_facility_type": "general_factory",
          "persistence_score": 0.9, "detections_7d": 8, "days_active_30d": 5,
          "current_frp_vs_historical_mean": 1.0})
    show("2.7km from mine single new",
         {"frp": 6.0},
         {"distance_m": 2700, "nearest_facility_type": "mining",
          "persistence_score": 0.0, "detections_7d": 0})
    show("far from industry cluster",
         {"frp": 12.0},
         {"distance_m": 120000, "persistence_score": 0.6,
          "detections_7d": 4, "days_active_30d": 2})
    show("no context",
         {"frp": 10.0},
         {"distance_m": None, "nearest_facility_type": None,
          "persistence_score": 0.0})
    show("no context but vision says industrial",
         {"frp": 10.0},
         {"distance_m": None, "nearest_facility_type": None,
          "vision_industrial_prob": 0.92, "vision_model_version": "v0-test"})
    show("gas site but vision flare high",
         {"frp": 2.0},
         {"distance_m": 300, "nearest_facility_type": "oil_gas_facility",
          "persistence_score": 0.1, "detections_7d": 0,
          "vision_flare_prob": 0.71, "vision_model_version": "v0-test"})