"""Aggregated player-season cache used by the summary views."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Sequence
import logging

import polars as pl

try:  # pragma: no cover - optional dependency
    from nflreadpy import load_player_stats
except ImportError:  # pragma: no cover - handled at runtime
    load_player_stats = None  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIRECTORY = PROJECT_ROOT / "data" / "cache"
CACHE_PATH = CACHE_DIRECTORY / "basic_player_stats.parquet"
DEFAULT_SEASONS = list(range(1999, datetime.now().year + 1))

LOGGER = logging.getLogger(__name__)

STRING_COLUMN_SOURCES: Mapping[str, Sequence[str]] = {
    "_team": ("recent_team", "team", "current_team_abbr"),
    "_player_name": ("player_display_name", "player_name"),
    "_player_id": ("player_id", "gsis_id"),
    "_position": ("position", "player_position"),
    "_position_group": ("position_group", "player_position_group"),
}

NUMERIC_COLUMN_SOURCES: Mapping[str, Sequence[str]] = {
    "offense_snaps": ("offense_snaps",),
    "defense_snaps": ("defense_snaps",),
    "special_teams_snaps": ("special_teams_snaps",),
    "pass_completions": ("pass_completions", "completions"),
    "pass_attempts": ("pass_attempts", "attempts"),
    "passing_yards": ("passing_yards",),
    "passing_tds": ("passing_tds",),
    "passing_ints": ("passing_interceptions", "interceptions_thrown"),
    "sacks_taken": ("sacks_suffered",),
    "sack_yards": ("sack_yards_lost",),
    "rushing_attempts": ("rushing_attempts", "rush_attempts"),
    "rushing_yards": ("rushing_yards", "rush_yards"),
    "rushing_tds": ("rushing_tds", "rush_tds"),
    "receiving_targets": ("receiving_targets", "targets"),
    "receiving_receptions": ("receiving_receptions", "receptions"),
    "receiving_yards": ("receiving_yards", "rec_yards"),
    "receiving_tds": ("receiving_tds", "rec_tds"),
    "total_fumbles": ("total_fumbles", "fumbles"),
    "fumbles_lost": ("fumbles_lost",),
    "def_tackles_solo": ("def_tackles_solo", "solo_tackles"),
    "def_tackle_assists": ("def_tackle_assists", "assist_tackles"),
    "def_tackles_for_loss": ("def_tackles_for_loss", "tackles_for_loss"),
    "def_sacks": ("def_sacks", "sacks"),
    "def_qb_hits": ("def_qb_hits", "qb_hits"),
    "def_forced_fumbles": ("def_forced_fumbles", "forced_fumbles"),
    "def_fumble_recoveries": ("fumble_recovery_opp", "fumble_recoveries"),
    "def_safeties": ("def_safeties", "safeties"),
    "def_pass_defended": ("def_pass_defended", "passes_defended"),
    "def_interceptions": ("def_interceptions",),
    "def_tds": ("def_tds", "defensive_touchdowns"),
    "penalties": ("penalties", "total_penalties"),
    "penalties_declined": ("penalties_declined",),
    "penalties_offsetting": ("penalties_offsetting", "penalties_offset"),
    "penalties_holding": ("penalties_holding", "holding_penalties"),
    "penalties_false_start": ("penalties_false_start", "false_start_penalties"),
    "fgm": ("fgm", "field_goals_made"),
    "fga": ("fga", "field_goals_attempted"),
    "fg_long": ("fg_long", "field_goal_long"),
    "fgm_0_19": ("fgm_0_19", "field_goals_made_0_19"),
    "fga_0_19": ("fga_0_19", "field_goals_attempted_0_19"),
    "fgm_20_29": ("fgm_20_29", "field_goals_made_20_29"),
    "fga_20_29": ("fga_20_29", "field_goals_attempted_20_29"),
    "fgm_30_39": ("fgm_30_39", "field_goals_made_30_39"),
    "fga_30_39": ("fga_30_39", "field_goals_attempted_30_39"),
    "fgm_40_49": ("fgm_40_49", "field_goals_made_40_49"),
    "fga_40_49": ("fga_40_49", "field_goals_attempted_40_49"),
    "fgm_50_59": ("fgm_50_59", "field_goals_made_50_59"),
    "fga_50_59": ("fga_50_59", "field_goals_attempted_50_59"),
    "fgm_60_plus": ("fgm_60_plus", "field_goals_made_60_plus"),
    "fga_60_plus": ("fga_60_plus", "field_goals_attempted_60_plus"),
    "xpm": ("xpm", "extra_points_made"),
    "xpa": ("xpa", "extra_points_attempted"),
    "kickoffs": ("kickoffs", "kickoff_attempts"),
    "kickoff_touchbacks": ("kickoff_touchbacks", "touchbacks"),
    "punts": ("punts",),
    "punt_yards": ("punt_yards", "punting_yards"),
    "punt_long": ("punt_long", "long_punt"),
    "punt_return_yards_allowed": ("punt_return_yards_allowed", "opponent_punt_return_yards"),
    "net_punt_yards": ("net_punt_yards",),
    "punt_touchbacks": ("punt_touchbacks", "touchbacks"),
    "punts_inside_20": ("punts_inside_20",),
    "punts_blocked": ("punts_blocked",),
}


def cache_exists() -> bool:
    """Return ``True`` when the basic cache parquet already exists."""

    return CACHE_PATH.exists()


def load_basic_cache() -> pl.DataFrame:
    """Load the aggregated cache from disk."""

    return pl.read_parquet(CACHE_PATH)


def scan_basic_cache() -> pl.LazyFrame:
    """Return a lazy scan over the aggregated cache."""

    return pl.scan_parquet(CACHE_PATH)


def build_basic_cache(
    *,
    seasons: Iterable[int] | None = None,
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Build (or rebuild) the player-season cache."""

    if cache_exists() and not force_refresh:
        LOGGER.info("Basic cache already present at %s; skipping rebuild.", CACHE_PATH)
        return load_basic_cache()

    if load_player_stats is None:
        raise RuntimeError(
            "nflreadpy is not installed. Install project dependencies to rebuild the basic cache."
        )

    target_seasons = list(seasons) if seasons else list(DEFAULT_SEASONS)
    LOGGER.info("Fetching nflverse player stats for seasons %s-%s.", target_seasons[0], target_seasons[-1])
    raw = load_player_stats(seasons=target_seasons)
    frame = _to_polars(raw)
    prepared = _prepare_for_aggregation(frame, target_seasons)
    aggregated = _aggregate_player_seasons(prepared)

    CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    aggregated.write_parquet(CACHE_PATH, compression="zstd")
    LOGGER.info("Wrote %s aggregated rows to %s", aggregated.height, CACHE_PATH)
    return aggregated


def _to_polars(frame: object) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame
    try:
        return pl.DataFrame(frame)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:  # pragma: no cover - diagnostic
        raise TypeError(f"Unsupported frame type from nflreadpy.load_player_stats: {type(frame)!r}") from exc


def _first_available_expr(
    frame: pl.DataFrame,
    columns: Sequence[str],
    alias: str,
    *,
    default: str = "",
) -> pl.Expr:
    existing = [pl.col(name) for name in columns if name in frame.columns]
    if not existing:
        return pl.lit(default, dtype=pl.Utf8).alias(alias)
    expr = pl.coalesce(existing).fill_null(default)
    return expr.cast(pl.Utf8, strict=False).alias(alias)


def _numeric_expr(
    frame: pl.DataFrame,
    sources: Sequence[str],
    alias: str,
) -> pl.Expr:
    existing = [
        pl.col(name).cast(pl.Float64, strict=False).fill_null(0.0)
        for name in sources
        if name in frame.columns
    ]
    if not existing:
        return pl.lit(0.0).alias(alias)
    if len(existing) == 1:
        return existing[0].alias(alias)
    return pl.coalesce(existing).alias(alias)


def _prepare_for_aggregation(frame: pl.DataFrame, seasons: Sequence[int]) -> pl.DataFrame:
    filtered = frame.filter(pl.col("season").is_in(seasons))
    if "season_type" in filtered.columns:
        filtered = filtered.filter(pl.col("season_type") == "REG")

    string_exprs = [
        _first_available_expr(filtered, STRING_COLUMN_SOURCES["_team"], "_team"),
        _first_available_expr(filtered, STRING_COLUMN_SOURCES["_player_name"], "_player_name"),
        _first_available_expr(filtered, STRING_COLUMN_SOURCES["_player_id"], "_player_id"),
        _first_available_expr(filtered, STRING_COLUMN_SOURCES["_position"], "_position"),
        _first_available_expr(filtered, STRING_COLUMN_SOURCES["_position_group"], "_position_group"),
    ]

    numeric_exprs = [
        _numeric_expr(filtered, sources, alias)
        for alias, sources in NUMERIC_COLUMN_SOURCES.items()
    ]

    prepared = filtered.with_columns(
        string_exprs
        + numeric_exprs
        + [
            pl.col("season").cast(pl.Int16, strict=False).alias("season"),
            pl.col("week").cast(pl.Int16, strict=False).alias("_week"),
            _numeric_expr(filtered, ("games", "games_played"), "_games_raw"),
        ]
    )
    return prepared


def _aggregate_player_seasons(frame: pl.DataFrame) -> pl.DataFrame:
    numeric_columns = list(NUMERIC_COLUMN_SOURCES.keys())
    grouped = (
        frame.group_by(
            "_player_id",
            "_player_name",
            "_position",
            "_position_group",
            "_team",
            "season",
        )
        .agg(
            pl.col("_games_raw").max().alias("_games_raw"),
            pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
            *[pl.col(column).sum().alias(column) for column in numeric_columns],
        )
        .with_columns(
            pl.when(pl.col("_games_raw") > 0)
            .then(pl.col("_games_raw"))
            .otherwise(pl.col("_games_from_weeks"))
            .cast(pl.Int32, strict=False)
            .alias("games_played")
        )
        .drop(["_games_raw", "_games_from_weeks"])
    )

    result = grouped.select(
        [
            pl.col("_player_id").alias("player_id"),
            pl.col("_player_name").alias("player_name"),
            pl.col("_position").alias("position"),
            pl.col("_position_group").alias("position_group"),
            pl.col("_team").alias("team"),
            pl.col("season"),
            pl.col("games_played"),
            *[pl.col(column) for column in numeric_columns],
        ]
    ).sort(["player_name", "season", "team"])

    return result


__all__ = [
    "CACHE_PATH",
    "cache_exists",
    "scan_basic_cache",
    "load_basic_cache",
    "build_basic_cache",
]

