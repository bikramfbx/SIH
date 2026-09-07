"""Land-cover seam tests: graceful no-data by default, provider parse, and a
mock provider exercised through lookup() without any network."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import land_cover  # noqa: E402


def test_disabled_by_default():
    assert land_cover.enabled() is False
    cls, fracs, note = land_cover.lookup(23.0, 77.0)
    assert cls is None and fracs is None
    assert land_cover.provider_status()["configured"] is False


def test_parse_esri_known_class():
    cls, fracs, note = land_cover._parse_esri(
        {"value": 4, "rasterFunctionResults": []})
    assert cls == "cropland"
    assert note.startswith("esri class 4")


def test_parse_esri_unknown_class_is_graceful():
    cls, fracs, note = land_cover._parse_esri({"value": 99})
    assert cls is None
    assert "unknown class code 99" in note


def test_mock_esri_provider(monkeypatch):
    monkeypatch.setenv("LAND_COVER", "1")
    monkeypatch.setenv("LAND_COVER_PROVIDER", "esri")
    monkeypatch.setattr(land_cover, "provider_name",
                        lambda: land_cover.PROVIDER_ESRI)

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"value": 5, "rasterFunctionResults": []}

    import requests as _requests
    monkeypatch.setattr(_requests, "post",
                        lambda *a, **k: FakeResp())
    cls, fracs, note = land_cover.lookup(1.0, 2.0)
    assert cls == "built_up"


def test_mock_esri_provider_connect_error(monkeypatch):
    monkeypatch.setenv("LAND_COVER", "1")
    monkeypatch.setenv("LAND_COVER_PROVIDER", "esri")
    import requests as _requests

    def boom(*a, **k):
        raise _requests.ConnectionError("no route to host")

    monkeypatch.setattr(_requests, "post", boom)
    cls, fracs, note = land_cover.lookup(1.0, 2.0)
    assert cls is None and fracs is None
    assert "no route to host" in note