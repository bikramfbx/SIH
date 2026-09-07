"""Failure-mode unit tests (no Docker/network needed).

Covers firms.py HTTP/parse failure handling and the API validation helpers.
The end-to-end failure modes (FIRMS outage -> degraded health, DB down -> 503,
idempotent reruns, worker heartbeat) are exercised by tests/fresh_install.sh.
"""

import datetime as _dt
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import firms  # noqa: E402


# ---------------------------------------------------------------------------
# firms.py: parsing validation
# ---------------------------------------------------------------------------

class TestRowValidation:
    def test_valid_row(self):
        assert firms.validate_row({
            "latitude": "23.8", "longitude": "78.3",
            "acq_date": "2026-09-06", "acq_time": "1235",
        })

    def test_bad_lat(self):
        assert not firms.validate_row({
            "latitude": "abc", "longitude": "78.3",
            "acq_date": "2026-09-06", "acq_time": "1235"})

    def test_out_of_range_lon(self):
        assert not firms.validate_row({
            "latitude": "23.8", "longitude": "181",
            "acq_date": "2026-09-06", "acq_time": "1235"})

    def test_bad_date(self):
        assert not firms.validate_row({
            "latitude": "23.8", "longitude": "78.3",
            "acq_date": "2026-13-01", "acq_time": "1235"})

    def test_bad_time(self):
        assert not firms.validate_row({
            "latitude": "23.8", "longitude": "78.3",
            "acq_date": "2026-09-06", "acq_time": "2370"})

    def test_malformed_csv_rows_become_invalid(self):
        # garbage row: header mismatch -> dropped, not a crash
        csv_text = (
            "latitude,longitude,acq_date,acq_time\n"
            "23.8,78.3,2026-09-06,1235\n"
            "oops,no,fields,here\n")
        rows = firms.parse_csv(csv_text)
        assert len(rows) == 2
        for r in rows:
            r["_source"] = "VIIRS_NOAA20_NRT"
        good = sum(1 for r in rows if firms.validate_row(r))
        assert good == 1


# ---------------------------------------------------------------------------
# firms.py: HTTP failure modes (monkeypatched requests)
# ---------------------------------------------------------------------------

class FakeResp:
    def __init__(self, status, text="", exc=None):
        self.status_code = status
        self.text = text
        self._exc = exc

    def raise_for_status(self):
        if self._exc:
            raise self._exc


def _patch_requests(monkeypatch, resp):
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: resp)


def _patch_defaults(monkeypatch, retries=1, timeout=5):
    monkeypatch.setattr(firms, "DEFAULT_RETRIES", retries)
    monkeypatch.setattr(firms, "DEFAULT_TIMEOUT_S", timeout)


class TestFetchFailures:
    def test_http_500_raises_after_retries(self, monkeypatch):
        _patch_requests(monkeypatch, FakeResp(500, "server error"))
        _patch_defaults(monkeypatch, retries=2)
        with pytest.raises(RuntimeError):
            firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "68,6,98,38", 2)

    def test_http_429_maps_to_rate_limited(self, monkeypatch):
        _patch_requests(monkeypatch, FakeResp(429, ""))
        with pytest.raises(RuntimeError) as exc:
            firms.fetch_firms_csv(
                "k", "VIIRS_NOAA20_NRT", "68,6,98,38", 2, retries=1)
        assert "rate-limited" in str(exc.value)

    def test_timeout_raises_cleanly(self, monkeypatch):
        import requests
        def slow(*a, **k):
            raise requests.Timeout()
        monkeypatch.setattr(requests, "get", slow)
        _patch_defaults(monkeypatch, retries=1)
        with pytest.raises(RuntimeError) as exc:
            firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "68,6,98,38", 2)
        assert "timed out" in str(exc.value)

    def test_connection_error_raises_cleanly(self, monkeypatch):
        import requests
        def blown(*a, **k):
            raise requests.ConnectionError("no route to host")
        monkeypatch.setattr(requests, "get", blown)
        _patch_defaults(monkeypatch, retries=1)
        with pytest.raises(RuntimeError) as exc:
            firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "68,6,98,38", 2)
        assert "no route to host" in str(exc.value)

    def test_empty_csv_succeeds_with_zero_rows(self, monkeypatch):
        header = ("latitude,longitude,acq_date,acq_time\n")
        _patch_requests(monkeypatch, FakeResp(200, header))
        _patch_defaults(monkeypatch)
        text = firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "68,6,98,38", 2)
        assert firms.parse_csv(text) == []

    def test_invalid_day_window_rejected(self, monkeypatch):
        with pytest.raises(ValueError):
            firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "68,6,98,38", 0)
        with pytest.raises(ValueError):
            firms.fetch_firms_csv("k", "VIIRS_NOAA20_NRT", "68,6,98,38", 99)


# ---------------------------------------------------------------------------
# API validation helpers
# ---------------------------------------------------------------------------

class TestApiValidation:
    @pytest.fixture(autouse=True)
    def _load_api(self):
        sys.path.insert(0, os.path.join(
            os.path.dirname(__file__), ".."))
        import api.app.main as api_main
        self.api = api_main

    def test_bbox_valid(self):
        assert self.api._parse_bbox("68,6,98,38") == (68.0, 6.0, 98.0, 38.0)

    def test_bbox_bad_length(self):
        with pytest.raises(ValueError):
            self.api._parse_bbox("1,2,3")

    def test_bbox_out_of_range(self):
        with pytest.raises(ValueError):
            self.api._parse_bbox("-200,0,0,1")

    def test_date_valid(self):
        assert self.api._parse_date("2026-09-06", "date_from") == \
            _dt.date(2026, 9, 6)

    def test_date_invalid(self):
        with pytest.raises(ValueError):
            self.api._parse_date("2026-09-32", "date_from")
        with pytest.raises(ValueError):
            self.api._parse_date("0932", "date_from")
        with pytest.raises(ValueError):
            self.api._parse_date("not-a-date", "date_from")

    def test_grid_sql_formatting(self):
        # resolution mode must substitute the where-clause exactly once and the
        # grid query must never reuse the per-row feature fragment.
        assert self.api.GEOJSON_GRID_SQL.count("{where}") == 1
        assert "{features}" not in self.api.GEOJSON_GRID_SQL
        sql = self.api.GEOJSON_GRID_SQL.format(where="TRUE")
        assert "floor(h.latitude / %(res)s)" in sql

    def test_grid_sql_reports_resolution_keyword(self):
        # the grid variant emits a resolution_deg property for clients
        assert "resolution_deg" in self.api.GEOJSON_GRID_SQL