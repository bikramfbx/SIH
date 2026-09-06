# SIH — Thermal Anomaly Intelligence (final backend)

Near-real-time, global-capable thermal-anomaly monitoring backend.

NASA FIRMS (NOAA-20 + NOAA-21 VIIRS NRT)
    -> scheduled ingestion worker
    -> PostGIS raw hotspots
    -> industrial context (stored OSM facilities)
    -> temporal / historical features
    -> transparent rule classification
    -> future vision-model hook (optional)
    -> FastAPI  ->  Leaflet frontend

## Stack

- **PostgreSQL + PostGIS 16** (`database/init.sql` + `database/migrations/`)
- **FIRMS core** — `scripts/firms.py` (shared fetch/parse/idempotent-insert/telemetry)
- **On-demand CLI** — `scripts/ingest_firms.py` (NRT or historical date)
- **Historical backfill** — `scripts/backfill_firms.py` (1–5 day windows)
- **Scheduled worker** — `scripts/worker.py` (near-real-time NRT ingestion + enrichment)
- **Industrial context** — `scripts/ingest_osm.py` (regional) and
  `scripts/ingest_facility_context.py` (radius queries around hotspots; opt-in async)
- **Enrich + classify** — `scripts/enrich_hotspots.py` + `scripts/classifier.py`
- **API + map** — FastAPI (`api/main.py`) + Leaflet (`frontend/index.html`)

## Quick start

1. Copy config and set your secrets:

   ```bash
   cp .env.example .env
   # set POSTGRES_* and FIRMS_MAP_KEY (free key: firms.modaps.eosdis.nasa.gov)
   ```

2. Start database + API + scheduler:

   ```bash
   docker compose up -d db api worker
   docker compose up --build api worker      # after code changes
   # open http://localhost:8000
   ```

   A fresh DB volume runs `database/init.sql`. On an existing DB apply the
   migrations in order:

   ```bash
   for m in database/migrations/*.sql; do
     docker exec -i sih-db-1 psql -U thermal_admin -d thermal_anomaly < "$m"
   done
   ```

3. Optional: backfill ~1 month of history so persistence/gas-flare signals
   have data:

   ```bash
   python3 scripts/backfill_firms.py \
     --source VIIRS_NOAA20_NRT --source VIIRS_NOAA21_NRT \
     --start-date 2026-08-05 --end-date 2026-09-06 \
     --bbox 68,6,98,38 --days 5 --enrich
   ```

4. Manual NRT ingestion + enrichment:

   ```bash
   python3 scripts/ingest_firms.py --enrich
   python3 scripts/enrich_hotspots.py --all        # recompute everything
   ```

## Architecture notes

- **Raw data separation.** `hotspots` keeps untouched raw FIRMS rows
  (source, satellite, instrument, daynight, confidence, FRP, …). All derived
  values live in `hotspot_enrichment`; `industrial_facilities` and
  `osm_query_cache` are separate. Class goes into `hotspot_enrichment`.
- **Idempotency.** FIRMS inserts use `INSERT ... ON CONFLICT DO NOTHING`
  against the unique index `(source, satellite, rounded lat/lon, date, time)`.
  Enrichment upserts. Backfill windows overlap safely; reruns insert 0 rows.
- **Scheduler.** `worker.py` polls every `FIRMS_POLL_INTERVAL_MIN` minutes,
  fetches each configured NRT source, records every run in
  `ingestion_run_log`, and enriches **only the newly inserted** hotspot ids.
  It is a separate process from the API; failures are logged/recorded and the
  loop continues. Graceful on SIGINT/SIGTERM; `--once` for cron/tests.
- **Overpass is off the live path.** The worker never queries Overpass unless
  `WORKER_OSM_CONTEXT=1` (default off), and then only for hotspots not already
  covered by `osm_query_cache`. If Overpass fails, the rest of the pipeline
  continues. Stored facilities + query cache are always reused. Replacing
  Overpass with a regional/global OSM extract later needs no pipeline change.
- **Future vision model.** `hotspot_enrichment` stores optional
  `vision_industrial_prob`, `vision_fire_prob`, `vision_flare_prob`,
  `vision_facility_type`, `vision_model_version`. The classifier reads them as
  extra evidence when present, and works without them otherwise. No model is
  trained or invoked by this backend.

## Temporal features (per hotspot, 1 km matching tolerance)

`detections_24h`, `detections_7d`, `detections_30d`, `days_active_30d`,
`mean_frp_7d`, `mean_frp_30d`, `max_frp_30d`,
`current_frp_vs_historical_mean` (FRP vs 30-d baseline),
`nighttime_detection_fraction`, `persistence_score`.
Thresholds are exposed as constants in `scripts/classifier.py` and env vars
(`PERSIST_RADIUS_M`, `PERSIST_WINDOW_DAYS`, `PERSIST_DENOM`).

## API

| Endpoint | Purpose |
|---|---|
| `GET /` | Leaflet map (served statically) |
| `GET /api/health` | Pipeline status: DB, last successful ingestion, sources used, last inserted count, enrichment time, error state |
| `GET /api/hotspots.geojson` | GeoJSON; filters: `class` (repeatable), `source`, `bbox=w,s,e,n`, `date_from`, `date_to`, `limit` |
| `GET /api/hotspots/{id}` | Full detail: raw row + enrichment + temporal + vision fields + reasons |
| `GET /api/stats` | Class counts, sources, date range, last ingestion |

Example:

```bash
curl "http://localhost:8000/api/hotspots.geojson?class=gas_flare&bbox=80,6,82,9"
curl "http://localhost:8000/api/hotspots.geojson?class=industrial_fire&date_from=2026-09-01"
curl http://localhost:8000/api/health
```

## Verification results (dev run)

- Live ingestion verified for `VIIRS_NOAA20_NRT` and `VIIRS_NOAA21_NRT`
  (path-style Area API, `{bbox}/{days}` and historical `{bbox}/{days}/{date}`).
- 30-day backfill: 12,172 records fetched, 10,359 inserted, 1,813 duplicates
  skipped; rerun inserted 0.
- Enrichment + classification of 12,172 hotspots (45 s): `industrial_fire`
  302, `persistent_industrial_source` 164, `gas_flare` 10 (thermal plants,
  e.g. Sri Lanka), `non_industrial` 11,696. Rerun produced identical counts.
- Worker cycle fetches both sources, records telemetry, enriches only new ids.
- Simulated Overpass outage: OSM context step reports failure and continues;
  FIRMS + enrichment unaffected.
- `docker compose config` valid; API + worker run as separate services.

## Notes / known limitations

- Public Overpass is flaky/rate-limited; context ingestion is conservative
  (retries/backoff, query cache, abort after N consecutive failures). This
  never blocks the live NRT pipeline.
- FIRMS Area API limits day windows to 1–5 days per request; the backfill
  chunks longer ranges into 5-day cells.
- `hotspots.location` is `geography`; facility geometry is `geometry(4326)`;
  distances are in meters. The geography bbox filter crosses the antimeridian
  as two boxes; not applicable for the current India bbox.
- Secrets live only in `.env` (gitignored); never in committed code/README.