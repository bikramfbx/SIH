# PyroSphere — Industrial Thermal Anomaly Monitor

Near-real-time monitoring and classification of industrial thermal anomalies using NASA FIRMS (VIIRS) satellite data, enriched with geographic industrial context and presented on a global map.

PyroSphere ingests FIRMS hotspots, adds geospatial industrial context (WRI power plants + OpenStreetMap power/industrial data), tracks temporal history at each location, and classifies likely industrial sources — gas flares, industrial fires, persistent industrial sources — using transparent, deterministic rules. The output surfaces a curated, defensible list of industrial classified hotspots instead of the raw noise floor of vegetation and agricultural fires.

Two pages:

- **`/`** — landing page introducing the product
- **`/map`** — the live map of classified industrial anomalies

Live: https://sih-one-wheat.vercel.app

## The Problem

Raw thermal hotspots are ambiguous. A single VIIRS detection can be industrial activity, a wildfire, agricultural burning, routine flaring, or an equipment abnormal event. Without industrial context, an analyst faces hundreds of thousands of raw detections with no way to separate signal from noise. PyroSphere separates likely industrial signals from background fire activity and flags interesting detections for follow-up.

## How It Works

1. **FIRMS ingestion** — scheduled worker and on-demand CLI pull VIIRS NRT data.
2. **PostGIS storage** — raw hotspots stored idempotently, separate from derived data.
3. **Industrial GIS enrichment** — hotspots matched against stored industrial facilities (WRI + OSM).
4. **Temporal analysis** — per-location history: repeat detections, FRP baselines, persistence.
5. **Transparent classification** — deterministic ruled chain (gas flare → persistent source → industrial fire → unknown) with human-readable reasons.
6. **FastAPI** — health, stats, and GeoJSON endpoints.
7. **Global map** — Leaflet frontend serving the classified signal.

## Features

### Backend / pipeline
- Scheduled polling with per-cycle telemetry and a worker heartbeat
- Idempotent ingestion (reruns insert zero rows)
- Deterministic rule-based classifier with per-detection reasons
- `/api/health` reporting pipeline and worker status
- Offline OSM extract ingestion (`.osm.pbf` / `.osm`), no live Overpass needed
- `/api/stats`, `/api/hotspots.geojson`, `/api/hotspots/{id}` endpoints

### Frontend
- Single-world Leaflet map (no seam wrapping) over a dark Esri canvas basemap
- Antimeridian-clipped country geometry (plain GeoJSON, closed rings — no degenerate dateline chords)
- Countries are color-coded by hotspot count; **names only appear on click** as offset callout labels tied to the country by a leader line
- Anomalies rendered as crisp, class-colored dots with a painted halo (no blur filters) that stay clean when stacked
- Per-class filters, hotspot popups, and a detail panel with evidence and reasons
- "All anomalies" toggle: show every hotspot and hide country borders/names at any zoom
- Full zoom-out to the whole world with solid black margins (out-of-world tiles pruned — no placeholder tiles)
- Starts at a readable overview centered on India; "PyroSphere" title links back to the landing page

## Tech Stack

| Layer | Choice |
|---|---|
| Language | Python (backend) + HTML/CSS/JS (frontend) |
| API | FastAPI + uvicorn (local) → Vercel serverless handler (deploy) |
| Database | PostgreSQL + PostGIS |
| Frontend | Leaflet (vanilla JS), d3-geo (build-time geometry clip) |
| Basemap | Esri Canvas (dark gray) |
| Source data | NASA FIRMS / VIIRS (NRT + historical) |
| Context data | WRI global power plants + OpenStreetMap |
| Classification | Transparent deterministic rules |
| Deployment | Vercel (static + serverless), Supabase Postgres |
| Runtime | Docker Compose (db, api, worker) |

## Getting Started

```bash
cp .env.example .env
# set POSTGRES_* and FIRMS_MAP_KEY (free: firms.modaps.eosdis.nasa.gov)

docker compose up -d db api worker
# open http://localhost:8000  (map at /map)
```

Run tests:

```bash
.venv/bin/python -m pytest tests/ -q
```

## Environment Variables

`FIRMS_MAP_KEY`, `FIRMS_SOURCES`, `FIRMS_BBOX`, `FIRMS_DAYS`, `FIRMS_POLL_INTERVAL_MIN`, `FIRMS_BASE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`, `WORKER_OSM_CONTEXT`, `PERSIST_RADIUS_M`, `PERSIST_WINDOW_DAYS`, `PERSIST_DENOM`.

## Project Structure

```
api/       FastAPI app: health, stats, GeoJSON; serverless handler (deploy)
database/  init.sql + migrations (PostGIS schema)
frontend/  landing page + Leaflet map page + static assets
data/      facility context datasets
scripts/   ingestion, enrichment, classification, validation
tests/     unit tests + fresh-install/failure-mode harness
reports/   PRD, final report, classifier validation
```

## Deployment

Vercel rewrites: `/` → landing, `/map` → map page, `/assets/*` → static files, `/api/*` → serverless handler. The `api/handler.py` serves the FastAPI endpoints; the Postgres backend uses the Supabase pooler.

## Limitations

- FIRMS is near-real-time, not continuous monitoring.
- Industrial GIS coverage (OSM) varies geographically, so classification can miss facilities that are not mapped.
- Rules are transparent and deterministic but are an approximation — `unknown` never implies "non-industrial".
- The live map currently serves a representative dev/demo dataset while the full pruned industrial dataset is wired through the same endpoints.