# SIH — Thermal Anomaly Intelligence

Near-real-time monitoring and classification of thermal anomalies using NASA FIRMS (VIIRS) data with industrial-context enrichment.

The pipeline ingests FIRMS hotspots, adds geospatial industrial context from OpenStreetMap, tracks temporal history at each location, and classifies likely industrial sources (gas flares, industrial fires, persistent sources) versus non-industrial activity using transparent rules. Optional vision and land-cover columns are reserved for future enrichment.

## The Problem

Raw thermal hotspots are ambiguous. A single VIIRS detection can be industrial activity, a wildfire, agricultural burning, routine flaring, or an equipment abnormal event. The goal is to separate likely industrial signals from background fire activity and flag interesting detections for follow-up.

## How It Works

1. FIRMS ingestion — scheduled worker and on-demand CLI pull VIIRS NRT data.
2. PostGIS storage — raw hotspots stored idempotently, separate from any derived data.
3. Industrial GIS enrichment — hotspots matched against stored OSM industrial facilities.
4. Temporal analysis — per-location history: repeat detections, FRP baselines, persistence.
5. Optional vision/land-cover enrichment — reserved columns, no model invoked by default.
6. Multimodal classification — transparent rules combine all evidence into a class label.
7. FastAPI — GeoJSON, stats, and health endpoints.
8. Global map — Leaflet frontend served by the API.

## Features

- Scheduled polling with per-cycle telemetry and a worker heartbeat
- Idempotent ingestion (reruns insert zero rows)
- Rule-based classifier with per-detection reasons
- `/api/health` reporting pipeline and worker status
- Offline OSM extract ingestion (`.osm.pbf` / `.osm`), no live Overpass needed
- Disposable fresh-install and failure-mode test harnesses

## Tech Stack

| Layer | Choice |
|---|---|
| Language | Python |
| API | FastAPI + uvicorn |
| Database | PostgreSQL + PostGIS |
| Source data | NASA FIRMS / VIIRS (NRT + historical) |
| Context data | OpenStreetMap (Overpass or offline extracts) |
| Frontend | Leaflet (static, served by API) |
| Runtime | Docker Compose (db, api, worker) |
| Vision | PyTorch (optional, not enabled) |

## Getting Started

```bash
cp .env.example .env
# set POSTGRES_* and FIRMS_MAP_KEY (free: firms.modaps.eosdis.nasa.gov)

docker compose up -d db api worker
# open http://localhost:8000
```

Run tests:

```bash
.venv/bin/python -m pytest tests/ -q
```

## Environment Variables

`FIRMS_MAP_KEY`, `FIRMS_SOURCES`, `FIRMS_BBOX`, `FIRMS_DAYS`, `FIRMS_POLL_INTERVAL_MIN`, `FIRMS_BASE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`, `WORKER_OSM_CONTEXT`, `PERSIST_RADIUS_M`, `PERSIST_WINDOW_DAYS`, `PERSIST_DENOM`, `LAND_COVER`, `LAND_COVER_PROVIDER`.

## Project Structure

```
api/       FastAPI app: health, GeoJSON, stats
database/  init.sql + migrations (PostGIS schema)
frontend/  Leaflet map page
scripts/   ingestion, enrichment, classification, validation
tests/     unit tests + fresh-install/failure-mode harness
```

## Limitations

- FIRMS is near-real-time, not continuous monitoring.
- Industrial GIS coverage (OSM) varies geographically.
- Optional vision enrichment depends on imagery quality if enabled later.
- Land-cover provider endpoints were unreachable during development; the seam is implemented but disabled.