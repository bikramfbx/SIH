import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import firms_screen  # noqa: E402


def row(lat, lon, frp="10.0"):
    return {"latitude": str(lat), "longitude": str(lon), "frp": str(frp)}


class TestCellKey:
    def test_quantizes(self):
        assert firms_screen.cell_key("12.3456", "-5.6789") == (12.35, -5.68)

    def test_rounding_bins_adjacent(self):
        assert firms_screen.cell_key(1.234, 0) == (1.23, 0.0)
        assert firms_screen.cell_key(1.239, 0) == (1.24, 0.0)


class TestScreenRows:
    def test_empty(self):
        kept, stats = firms_screen.screen_rows([], "S", set())
        assert kept == []
        assert stats["total"] == 0

    def test_no_rules_keep_nothing(self):
        rows = [row(1, 1), row(2, 2)]
        kept, stats = firms_screen.screen_rows(rows, "S", set())
        assert kept == []
        assert stats["dropped"] == 2

    def test_within_day_persistence(self):
        rows = [row(10, 20), row(10, 20), row(1, 1)]
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), min_in_day=2)
        assert len(kept) == 2
        assert stats["in_day"] == 2
        assert stats["kept"] == 2

    def test_prior_retained(self):
        rows = [row(10, 20, "5"), row(1, 1, "7")]
        prior = {("S", (10.0, 20.0))}
        kept, stats = firms_screen.screen_rows(rows, "S", prior)
        assert len(kept) == 1
        assert stats["prior"] == 1

    def test_frp_threshold(self):
        rows = [row(0, 0, "100"), row(0, 1, "5")]
        kept, stats = firms_screen.screen_rows(rows, "S", set(), frp_mw=50)
        assert [r["frp"] for r in kept] == ["100"]
        assert stats["frp"] == 1

    def test_frp_percentile(self):
        rows = [row(0, i, str(i)) for i in range(1, 11)]  # frp 1..10
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), percentile=90.0)
        assert [float(r["frp"]) for r in kept] == [9.0, 10.0]
        assert stats["frp"] == 2