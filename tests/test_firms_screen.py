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

    def test_no_facility_keeps_nothing(self):
        rows = [row(1, 1), row(2, 2)]
        kept, stats = firms_screen.screen_rows(rows, "S", set())
        assert kept == []
        assert stats["dropped"] == 2

    def test_within_day_persistence_is_stat_only(self):
        rows = [row(10, 20, "90"), row(10, 20, "95"), row(1, 1)]
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), min_in_day=2)
        assert kept == []
        assert stats["in_day"] == 2
        assert stats["kept"] == 0
        assert stats["dropped"] == 3

    def test_prior_retention_is_stat_only(self):
        rows = [row(10, 20, "5"), row(1, 1, "7")]
        prior = {("S", (10.0, 20.0))}
        kept, stats = firms_screen.screen_rows(rows, "S", prior)
        assert kept == []
        assert stats["prior"] == 1
        assert stats["kept"] == 0

    def test_high_frp_is_stat_only(self):
        rows = [row(0, 0, "100"), row(0, 1, "5")]
        kept, stats = firms_screen.screen_rows(rows, "S", set(), frp_mw=50)
        assert kept == []
        assert stats["frp"] == 1
        assert stats["kept"] == 0

    def test_high_frp_percentile_is_stat_only(self):
        rows = [row(0, i, str(i)) for i in range(1, 11)]  # frp 1..10
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), percentile=90.0)
        assert kept == []
        assert stats["frp"] == 2
        assert stats["kept"] == 0

    def test_facility_cell_kept(self):
        rows = [row(10.005, 20.005, "3"), row(80, 0, "8")]
        facility_cells = {(10.0, 20.0)}
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), facility_cells=facility_cells)
        assert len(kept) == 1
        assert stats["near_facility"] == 1
        assert stats["kept"] == 1

    def test_facility_neighbor_cell_kept(self):
        # same cell + 0.01 lat (+/- ~1.1 km) counts as near a facility cell
        rows = [row(10.01, 20.0, "3")]
        facility_cells = {(10.0, 20.0)}
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), facility_cells=facility_cells)
        assert len(kept) == 1
        assert stats["near_facility"] == 1

    def test_far_from_facilities_dropped_even_with_persistence(self):
        rows = [row(10.5, 20.5, "3"), row(10.5, 20.5, "4")]
        facility_cells = {(10.0, 20.0)}
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), facility_cells=facility_cells, min_in_day=2)
        assert kept == []
        assert stats["near_facility"] == 0
        assert stats["in_day"] == 2

    def test_extended_reach_keeps_furthest_rows(self):
        # default reach 5,000 m ~ 5 cells; a cell 3 cells away is kept
        facility_cells = {(10.0, 20.0)}
        rows = [row(10.03, 20.0, "3")]  # 3 cells ~3.3 km -> within 5 km reach
        kept, stats = firms_screen.screen_rows(
            rows, "S", set(), facility_cells=facility_cells,
            facility_reach_m=5000.0)
        assert len(kept) == 1
        assert stats["kept"] == 1

    def test_custom_reach_respects_radius(self):
        facility_cells = {(10.0, 20.0)}
        rows = [row(10.03, 20.0, "3")]  # ~3.3 km away
        kept, _ = firms_screen.screen_rows(
            rows, "S", set(), facility_cells=facility_cells,
            facility_reach_m=2000.0)  # only ~2 cells -> drop
        assert kept == []