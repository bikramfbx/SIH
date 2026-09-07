-- Migration 004: global/serving-scale indexes + retention support.
-- Fresh-install only (Safe: IF NOT EXISTS, additive, no destructive reset).

-- Allow cheap DELETE by acquisition date (retention pruning).
CREATE INDEX IF NOT EXISTS idx_hotspots_acq_date
    ON hotspots (acq_date);

-- Allow pruning/stat aggregation on classification.
CREATE INDEX IF NOT EXISTS idx_hotspot_enrichment_class
    ON hotspot_enrichment (class);