"""Season-level cache that powers every player summary table."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
import logging

import polars as pl

from . import basic_cache
from . import player_impacts

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIRECTORY = PROJECT_ROOT / "data" / "cache"
CACHE_PATH = CACHE_DIRECTORY / "player_summary_stats.parquet"


def cache_exists() -> bool:
    """Return ``True`` when the summary cache already exists on disk."""

    return CACHE_PATH.exists()


def load_player_summary_cache() -> pl.DataFrame:
    """Load the cached player summary stats into memory."""

    return pl.read_parquet(CACHE_PATH)


def scan_player_summary_cache() -> pl.LazyFrame:
    """Return a lazy scan over the cached player summary stats."""

    return pl.scan_parquet(CACHE_PATH)


def build_player_summary_cache(
    *,
    seasons: Iterable[int] | None = None,
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Build (or rebuild) the summary cache covering every player table stat."""

    base = _load_basic_frame(seasons=seasons, force_refresh=force_refresh)
    impacts = _load_impact_frame(seasons=seasons, force_refresh=force_refresh)
    summary = _merge_with_impacts(base, impacts)

    CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    summary.write_parquet(CACHE_PATH, compression="zstd")
    logger.info("Wrote player summary cache with %s rows to %s", summary.height, CACHE_PATH)
    return summary


def _load_basic_frame(
    *,
    seasons: Iterable[int] | None,
    force_refresh: bool,
) -> pl.DataFrame:
    if force_refresh or not basic_cache.cache_exists():
        frame = basic_cache.build_basic_cache(seasons=seasons, force_refresh=force_refresh)
    else:
        frame = basic_cache.load_basic_cache()
    return _filter_by_seasons(frame, seasons)


def _load_impact_frame(
    *,
    seasons: Iterable[int] | None,
    force_refresh: bool,
) -> pl.DataFrame:
    if force_refresh or not player_impacts.cache_exists():
        frame = player_impacts.build_player_impacts_cache(seasons=seasons, force_refresh=force_refresh)
    else:
        frame = player_impacts.load_player_impacts()
    return _filter_by_seasons(frame, seasons)


def _filter_by_seasons(frame: pl.DataFrame, seasons: Iterable[int] | None) -> pl.DataFrame:
    if not seasons:
        return frame
    season_list = sorted({int(season) for season in seasons if season is not None})
    if not season_list:
        return frame
    return frame.filter(pl.col("season").is_in(season_list))


def _numeric_or_zero(frame: pl.DataFrame, column: str) -> pl.Expr:
    if column not in frame.columns:
        return pl.lit(0.0)
    return pl.col(column).cast(pl.Float64, strict=False).fill_null(0.0)


def _merge_with_impacts(base: pl.DataFrame, impacts: pl.DataFrame) -> pl.DataFrame:
    """Derive helper columns and join EPA/WPA totals onto the base cache."""

    if base.is_empty():
        return base

    derived = base.with_columns(
        [
            pl.sum_horizontal(
                [
                    _numeric_or_zero(base, "offense_snaps"),
                    _numeric_or_zero(base, "defense_snaps"),
                    _numeric_or_zero(base, "special_teams_snaps"),
                ]
            )
            .cast(pl.Int64, strict=False)
            .alias("snaps_total"),
            pl.sum_horizontal(
                [
                    _numeric_or_zero(base, "passing_tds"),
                    _numeric_or_zero(base, "rushing_tds"),
                    _numeric_or_zero(base, "receiving_tds"),
                ]
            )
            .cast(pl.Int32, strict=False)
            .alias("total_touchdowns"),
        ]
    )

    if impacts.is_empty():
        merged = derived
    else:
        impact_columns = [column for column in impacts.columns if column not in {"player_id", "season"}]
        trimmed = impacts.select(["player_id", "season", *impact_columns]) if impact_columns else impacts
        merged = derived.join(trimmed, on=["player_id", "season"], how="left")
        if impact_columns:
            merged = merged.with_columns(
                [
                    pl.col(column)
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .alias(column)
                    for column in impact_columns
                ]
            )

    ordered = merged.sort(["player_name", "season", "team"])
    return ordered


__all__ = [
    "CACHE_PATH",
    "build_player_summary_cache",
    "cache_exists",
    "load_player_summary_cache",
    "scan_player_summary_cache",
]


