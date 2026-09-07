-- 005: pre-storage screening log.
-- One row per fetched FIRMS window recording how many raw detections were
-- screened out as non-industrial before anything was permanently stored.
CREATE TABLE IF NOT EXISTS screening_run_log (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            text NOT NULL DEFAULT 'firms',
    source          text NOT NULL,
    bbox            text,
    date_from       date,
    days            int,
    fetched         int NOT NULL DEFAULT 0,
    kept            int NOT NULL DEFAULT 0,
    dropped         int NOT NULL DEFAULT 0,
    in_day          int NOT NULL DEFAULT 0,
    prior           int NOT NULL DEFAULT 0,
    frp             int NOT NULL DEFAULT 0,
    percentile_pct  numeric,
    frp_mw          numeric,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_screening_log_source_date
    ON screening_run_log (source, date_from);