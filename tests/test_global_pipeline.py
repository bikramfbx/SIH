"""Tests for the global/production pipeline additions.

Covers:
- FIRMS ``global`` bbox support and formatting
- DB connection params (SSL mode, connect timeout)
- Vercel handler import (Mangum)
- worker token guard (401 without/with wrong token; never executes live)
- prune retention floor + industrial classes
- vision-unavailable fallback in the classifier
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import firms  # noqa: E402


# ---------------------------------------------------------------------------
# FIRMS global bbox
# ---------------------------------------------------------------------------

class TestGlobalBbox:
    def test_world_literal(self):
        assert firms.normalize_bbox("world") == "world"

    def test_world_case_and_whitespace(self):
        assert firms.normalize_bbox(" WORLD ") == "world"

    def test_global_rejected(self):
        with pytest.raises(ValueError):
            firms.normalize_bbox("global")

    def test_regular_box_passthrough(self):
        assert firms.normalize_bbox("68,6,98,38") == "68,6,98,38"

    def test_bad_box(self):
        with pytest.raises(ValueError):
            firms.normalize_bbox("68,6,98")
        with pytest.raises(ValueError):
            firms.normalize_bbox("foo")
        with pytest.raises(ValueError):
            firms.normalize_bbox("")
        with pytest.raises(ValueError):
            firms.normalize_bbox(None)

    def test_out_of_range(self):
        with pytest.raises(ValueError):
            firms.normalize_bbox("-200,-90,0,90")

    def test_reversed_box(self):
        with pytest.raises(ValueError):
            firms.normalize_bbox("98,6,68,38")

    def test_world_url_formatting(self, monkeypatch):
        import requests

        captured = {}

        def fake_get(url, timeout=60):
            captured["url"] = url
            class R:
                status_code = 200
                text = "latitude,longitude\n"
            return R()

        monkeypatch.setattr(requests, "get", fake_get)
        firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "world", 1, retries=1)
        assert captured["url"].endswith("/k/VIIRS_NOAA20_NRT/world/1")


# ---------------------------------------------------------------------------
# DB connection params
# ---------------------------------------------------------------------------

class TestConnectionParams:
    def test_defaults_without_ssl(self, monkeypatch):
        from api.app import db

        for v in ("POSTGRES_SSLMODE", "POSTGRES_CONNECT_TIMEOUT"):
            monkeypatch.delenv(v, raising=False)
        p = db.connection_params()
        assert "sslmode" not in p
        assert "connect_timeout" not in p
        assert p["host"] == "localhost"

    def test_ssl_and_timeout_flow_through(self, monkeypatch):
        from api.app import db

        monkeypatch.setenv("POSTGRES_SSLMODE", "require")
        monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "10")
        p = db.connection_params()
        assert p["sslmode"] == "require"
        assert p["connect_timeout"] == 10


# ---------------------------------------------------------------------------
# Vercel handler import
# ---------------------------------------------------------------------------

class TestVercelHandler:
    def test_handler_imports_mangum(self):
        import api.handler

        assert api.handler.app is not None
        assert callable(getattr(api.handler, "handler", None))


# ---------------------------------------------------------------------------
# Worker token guard
# ---------------------------------------------------------------------------

class TestWorkerGuard:
    def test_missing_token_env_rejects(self, monkeypatch):
        monkeypatch.delenv("WORKER_TOKEN", raising=False)
        from api.app.main import _worker_authorized

        assert _worker_authorized({"X-Worker-Token": "anything"}) is False
        assert _worker_authorized({}) is False

    def test_correct_token_passes(self, monkeypatch):
        monkeypatch.setenv("WORKER_TOKEN", "s3cret")
        from api.app.main import _worker_authorized

        assert _worker_authorized({"X-Worker-Token": "s3cret"}) is True
        assert _worker_authorized({"X-Worker-Token": "wrong"}) is False

    def test_endpoint_401_without_token(self):
        from fastapi.testclient import TestClient
        from api.app import main

        client = TestClient(main.app)
        r = client.post("/api/worker/once")
        assert r.status_code == 401
        r2 = client.post("/api/worker/maintenance")
        assert r2.status_code == 401

    def test_endpoint_401_with_wrong_token(self):
        from fastapi.testclient import TestClient
        from api.app import main

        app = main.app
        client = TestClient(app)
        r = client.post(
            "/api/worker/once",
            headers={"X-Worker-Token": "not-the-token"},
        )
        assert r.status_code == 401

    def test_endpoint_requires_firms_key_before_running(self, monkeypatch):
        # Correct token but no FIRMS_MAP_KEY -> bounded 500, never a live run.
        monkeypatch.setenv("WORKER_TOKEN", "t0ken")
        monkeypatch.delenv("FIRMS_MAP_KEY", raising=False)
        monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "1")
        from fastapi.testclient import TestClient
        from api.app import main

        client = TestClient(main.app)
        r = client.post(
            "/api/worker/once",
            headers={"X-Worker-Token": "t0ken"},
        )
        assert r.status_code == 500


# ---------------------------------------------------------------------------
# Retention / pruning
# ---------------------------------------------------------------------------

class TestPrune:
    def test_retention_floor(self, monkeypatch):
        import prune_hotspots

        assert prune_hotspots.retention_days(1) == 30
        assert prune_hotspots.retention_days(30) == 30
        assert prune_hotspots.retention_days(45) == 45
        monkeypatch.setenv("RAW_RETENTION_DAYS", "10")
        assert prune_hotspots.retention_days() == 30
        monkeypatch.setenv("RAW_RETENTION_DAYS", "90")
        assert prune_hotspots.retention_days() == 90

    def test_industrial_classes_preserved(self):
        import prune_hotspots

        for cls in ("industrial_fire",
                    "persistent_industrial_source", "gas_flare"):
            assert cls in prune_hotspots.INDUSTRIAL_CLASSES


# ---------------------------------------------------------------------------
# Classifier vision fallback
# ---------------------------------------------------------------------------

class TestVisionFallback:
    def test_no_vision_fields_classifies_cleanly(self):
        import classifier

        hotspot = {"frp": 18.0, "daynight": "D", "latitude": 23.4,
                   "longitude": 78.2, "acq_datetime": None}
        enrichment = {"distance_m": 2500.0, "within_facility": False,
                      "nearest_facility_type": "thermal_power_plant",
                      "nearest_facility_name": None,
                      "detections_24h": 3, "detections_7d": 5,
                      "detections_30d": 9, "days_active_30d": 4,
                      "mean_frp_7d": 15.0, "mean_frp_30d": 12.0,
                      "max_frp_30d": 30.0,
                      "current_frp_vs_historical_mean": 1.2,
                      "nighttime_detection_fraction": 0.5,
                      "persistence_score": 0.5}
        feats = classifier.build_features(hotspot, enrichment)
        assert feats["vision_industrial_prob"] is None
        label, _reasons = classifier.apply_rules(feats)
        assert label in classifier.CLASSES

    def test_vision_signals_are_read_when_present(self):
        import classifier

        hotspot = {"frp": 18.0, "daynight": "D", "latitude": 23.4,
                   "longitude": 78.2, "acq_datetime": None}
        enrichment = {"distance_m": 90000.0, "within_facility": False,
                      "nearest_facility_type": None,
                      "nearest_facility_name": None,
                      "detections_24h": 0, "detections_7d": 0,
                      "detections_30d": 1, "days_active_30d": 1,
                      "mean_frp_7d": None, "mean_frp_30d": None,
                      "max_frp_30d": None,
                      "current_frp_vs_historical_mean": None,
                      "nighttime_detection_fraction": 0.0,
                      "persistence_score": 0.0,
                      "vision_industrial_prob": 0.95,
                      "vision_facility_type": "refinery"}
        feats = classifier.build_features(hotspot, enrichment)
        assert feats["vision_industrial_prob"] == 0.95