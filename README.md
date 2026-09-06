# SIH — Thermal Anomaly Intelligence (MVP)

PostGIS-backed pipeline that ingests NASA FIRMS hotspots and OpenStreetMap
industrial infrastructure, then spatial-enriches and classifies each hotspot.

## Stack

- **PostgreSQL + PostGIS 16** (`database/init.sql`)
- **FIRMS ingest** — `scripts/ingest_firms.py` (raw satellite hotspots)
- **OSM industrial** — `scripts/ingest_osm.py` (regional/bbox) and
  `scripts/ingest_facility_context.py` (small radius queries around hotspots)
- **Enrich + classify** — `scripts/enrich_hotspots.py` + `scripts/classifier.py`
- **API + map** — FastAPI (`api/main.py`) + Leaflet (`frontend/index.html`)

## Quick start

1. Copy config and set your secrets:

   ```bash
   cp .env.example .env
   # set POSTGRES_* and FIRMS_MAP_KEY in .env
   ```

2. Start the database (fresh volume runs `database/init.sql`):

   ```bash
   docker compose up -d db
   ```

   On an existing DB, apply the MVP migration once:

   ```bash
   docker exec -i sih-db-1 psql -U thermal_admin -d thermal_anomaly < database/migrations/001_mvp.sql
   ```

3. Ingest data (all scripts are idempotent / upsert):

   ```bash
   python3 scripts/ingest_firms.py                 # FIRMS hotspots
   python3 scripts/ingest_facility_context.py      # small OSM queries around hotspots
   python3 scripts/enrich_hotspots.py              # enrich + classify
   ```

4. Run the API + frontend:

   ```bash
   pip install -r requirements.txt
   uvicorn api.main:app --host 0.0.0.0 --port 8000
   # open http://localhost:8000
   ```

   Or run it alongside the DB:

   ```bash
   docker compose up --build api
   ```

## API

| Endpoint | Purpose |
|---|---|
| `GET /` | Leaflet map (served statically) |
| `GET /api/health` | DB connectivity check |
| `GET /api/hotspots.geojson?class=industrial_fire` | Classified hotspots as GeoJSON (repeat `class` to filter) |
| `GET /api/hotspots/{id}` | Full detail + enrichment + reasons |
| `GET /api/stats` | Class counts, facility count, date range |

## Classification

Transparent rule-based classifier (`scripts/classifier.py`): `industrial_fire`,
`persistent_industrial_source`, `gas_flare`, `non_industrial`, `unknown`.
Each decision carries human-readable reasons. Rules are feature-based so a
vision-model probability can be added later as extra features
(`vision_fire_prob`, `vision_flare_prob`) without reworking the engine.

## Notes / known limitations

- Public Overpass is intermittently rate-limited/unreliable; the context
  ingester uses conservative retries/backoff, a per-area query cache
  (`osm_query_cache`), and skips a hotspot after a small number of failures.
  Reruns resume where they stopped (no duplicate queries).
- FIRMS data used for the demo spans a single day; persistence scoring grows
  more meaningful once multiple days are ingested.
- `hotspots.location` is `geography`; facility geometry is `geometry(4326)`.
  Distance comparisons are in meters.