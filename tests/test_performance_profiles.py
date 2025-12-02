"""Simple performance probes for common UI calculations.

These tests aren't strict unit tests—they provide a repeatable way to time
key operations so we can spot regressions or confirm that slow UI paths have
improved after refactors.
"""

from __future__ import annotations

from pathlib import Path
import statistics
import time

import polars as pl
import pytest


SUMMARY_CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "cache" / "player_summary_stats.parquet"

POSITION_GROUPS = {
    "QB": ["QB"],
    "RB": ["RB", "FB"],
    "WR": ["WR"],
    "TE": ["TE"],
    "DL": ["DL", "DE", "DT", "NT"],
    "LB": ["LB", "ILB", "OLB", "MLB"],
    "DB": ["DB", "CB", "S", "FS", "SS"],
    "OL": ["G", "T", "C", "OL"],
    "K": ["K"],
    "P": ["P"],
}


def _require_summary_cache() -> pl.DataFrame:
    if not SUMMARY_CACHE_PATH.exists():
        pytest.skip(
            f"Missing player summary cache at {SUMMARY_CACHE_PATH}. "
            "Run scripts/build_player_summary_cache.py first."
        )
    return pl.read_parquet(SUMMARY_CACHE_PATH)


def _measure(func, *, iterations: int = 1) -> float:
    samples: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        samples.append(end - start)
    return statistics.mean(samples) if samples else 0.0


class PerformanceReport:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, float]] = []

    def record(self, label: str, position: str, seconds: float) -> None:
        self.rows.append((label, position, seconds))

    def dump(self) -> None:
        if not self.rows:
            return
        print("\n=== Performance Measurements (avg ms) ===")
        label_width = max(len(label) for label, _, _ in self.rows) + 2
        position_width = max(len(pos) for _, pos, _ in self.rows) + 2
        header = f"{'Metric':{label_width}}{'Position':{position_width}}Average"
        print(header)
        print("-" * len(header))
        for label, position, seconds in self.rows:
            print(f"{label:{label_width}}{position:{position_width}}{seconds * 1000:.2f}")
        print("=========================================\n")


@pytest.fixture
def perf_report() -> PerformanceReport:
    report = PerformanceReport()
    yield report
    report.dump()


def _summary_calculation(frame: pl.DataFrame, player_ids: list[str]) -> None:
    subset = frame.filter(pl.col("player_id").is_in(player_ids))
    (
        subset.group_by(["player_id", "player_name", "position"])
        .agg(
            pl.col("games_played").sum().alias("games"),
            pl.col("passing_yards").sum().alias("pass_yards"),
            pl.col("passing_tds").sum().alias("pass_tds"),
            pl.col("passing_ints").sum().alias("pass_ints"),
            pl.col("rushing_yards").sum().alias("rush_yards"),
            pl.col("rushing_tds").sum().alias("rush_tds"),
            pl.col("receiving_yards").sum().alias("rec_yards"),
            pl.col("receiving_tds").sum().alias("rec_tds"),
            pl.col("qb_epa").sum().alias("qb_epa"),
            pl.col("qb_wpa").sum().alias("qb_wpa"),
            pl.col("skill_epa").sum().alias("skill_epa"),
            pl.col("skill_wpa").sum().alias("skill_wpa"),
        )
        .sort("games", descending=True)
    )


def _basic_rating_calculation(frame: pl.DataFrame, player_ids: list[str]) -> None:
    subset = frame.filter(pl.col("player_id").is_in(player_ids))
    (
        subset.group_by(["player_id", "player_name", "position"])
        .agg(
            pl.col("rushing_yards").sum().alias("rush_yards"),
            pl.col("receiving_yards").sum().alias("rec_yards"),
            pl.col("rushing_tds").sum().alias("rush_tds"),
            pl.col("receiving_tds").sum().alias("rec_tds"),
            pl.col("passing_yards").sum().alias("pass_yards"),
        )
        .with_columns(
            (
                pl.col("rush_yards")
                + pl.col("rec_yards") * 0.6
                + (pl.col("rush_tds") + pl.col("rec_tds")) * 25
                + pl.col("pass_yards") * 0.25
            ).alias("rating_score")
        )
        .sort("rating_score", descending=True)
    )


def _search_players(frame: pl.DataFrame, *, name_substring: str, team: str | None, position: str | None) -> None:
    filters = []
    if name_substring:
        filters.append(pl.col("player_name").str.contains(name_substring, literal=False, case=False))
    if team:
        filters.append(pl.col("team") == team.upper())
    if position:
        filters.append(pl.col("position") == position.upper())
    filtered = frame.filter(pl.all_horizontal(filters)) if filters else frame
    _ = filtered.select(["player_name", "team", "position", "season"]).head(50)


def _sample_player_ids(frame: pl.DataFrame, positions: list[str], count: int) -> list[str]:
    subset = frame.filter(pl.col("position").is_in(positions))
    if subset.height <= count:
        return subset["player_id"].unique().to_list()
    rows = subset.select(["player_id"]).unique().sample(count, with_replacement=False)
    return rows["player_id"].to_list()


def _profile_summary_tables(frame: pl.DataFrame, perf_report: PerformanceReport) -> None:
    for label, positions in POSITION_GROUPS.items():
        player_ids = _sample_player_ids(frame, positions, 50)
        if not player_ids:
            continue
        duration = _measure(lambda: _summary_calculation(frame, player_ids), iterations=3)
        perf_report.record("Summary table", label, duration)


def _profile_basic_ratings(frame: pl.DataFrame, perf_report: PerformanceReport) -> None:
    for label, positions in POSITION_GROUPS.items():
        player_ids = _sample_player_ids(frame, positions, 50)
        if not player_ids:
            continue
        duration = _measure(lambda: _basic_rating_calculation(frame, player_ids), iterations=3)
        perf_report.record("Basic ratings", label, duration)


@pytest.mark.performance
def test_player_search_speed(perf_report: PerformanceReport) -> None:
    frame = _require_summary_cache()
    duration = _measure(
        lambda: _search_players(frame, name_substring="john", team="NE", position="WR"),
        iterations=10,
    )
    perf_report.record("Player search", "all", duration)


@pytest.mark.performance
def test_summary_tables_across_positions(perf_report: PerformanceReport) -> None:
    frame = _require_summary_cache()
    _profile_summary_tables(frame, perf_report)


@pytest.mark.performance
def test_basic_ratings_across_positions(perf_report: PerformanceReport) -> None:
    frame = _require_summary_cache()
    _profile_basic_ratings(frame, perf_report)


