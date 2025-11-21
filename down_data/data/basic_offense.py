"""Local cache helpers for basic offensive season statistics."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
import logging

import polars as pl

try:  # pragma: no cover - runtime dependency
    from nflreadpy import load_player_stats
except ImportError:  # pragma: no cover - fallback for environments without nflreadpy
    load_player_stats = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIRECTORY = PROJECT_ROOT / "data" / "cache" / "nflverse"
CACHE_PATH = CACHE_DIRECTORY / "basic_offense_1999_2024.parquet"

OFFENSE_SEASONS = list(range(1999, 2025))
OFFENSE_POSITIONS = {
    "QB",
    "RB",
    "FB",
    "HB",
    "WR",
    "TE",
    "FL",
    "SL",
}

BASIC_OFFENSE_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "position": pl.Utf8,
    "position_group": pl.Utf8,
    "team": pl.Utf8,
    "season": pl.Int16,
    "games_played": pl.Int16,
    "pass_completions": pl.Int32,
    "pass_attempts": pl.Int32,
    "passing_yards": pl.Int64,
    "passing_tds": pl.Int32,
    "passing_ints": pl.Int32,
    "sacks_taken": pl.Int32,
    "sack_yards": pl.Int64,
    "rushing_attempts": pl.Int32,
    "rushing_yards": pl.Int64,
    "rushing_tds": pl.Int32,
    "receiving_targets": pl.Int32,
    "receiving_receptions": pl.Int32,
    "receiving_yards": pl.Int64,
    "receiving_tds": pl.Int32,
    "total_fumbles": pl.Int32,
    "fumbles_lost": pl.Int32,
}


def cache_exists() -> bool:
    """Return ``True`` when the cache file is present on disk."""

    return CACHE_PATH.exists()


def load_basic_offense_stats() -> pl.DataFrame:
    """Load the cached basic offensive stats into memory."""

    return pl.read_parquet(CACHE_PATH)


def scan_basic_offense_stats() -> pl.LazyFrame:
    """Return a lazy scanner over the cached basic offensive stats."""

    return pl.scan_parquet(CACHE_PATH)


def build_basic_offense_cache(*, force_refresh: bool = False) -> pl.DataFrame:
    """Build (or rebuild) the local basic offense cache.

    Args:
        force_refresh: When True, rebuild the cache even if it already exists.
    """

    if cache_exists() and not force_refresh:
        logger.info("Basic offense cache already exists; skipping rebuild.")
        return load_basic_offense_stats()

    if load_player_stats is None:
        raise RuntimeError(
            "nflreadpy is not available; install project dependencies to build the cache."
        )

    logger.info("Fetching nflverse player stats for seasons %s-%s.", OFFENSE_SEASONS[0], OFFENSE_SEASONS[-1])
    raw = load_player_stats(seasons=OFFENSE_SEASONS)
    frame = _to_polars(raw)
    prepared = _prepare_for_aggregation(frame)
    aggregated = _aggregate_player_seasons(prepared)

    CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    aggregated.write_parquet(CACHE_PATH, compression="zstd")
    logger.info("Wrote basic offense cache to %s", CACHE_PATH)
    return aggregated


def _to_polars(frame: object) -> pl.DataFrame:
    """Convert third-party dataframes into Polars."""

    if isinstance(frame, pl.DataFrame):
        return frame
    try:
        return pl.DataFrame(frame)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:  # pragma: no cover - debug helper
        raise TypeError(f"Unsupported frame type from nflreadpy.load_player_stats: {type(frame)!r}") from exc


def _first_available_expr(
    frame: pl.DataFrame,
    columns: Iterable[str],
    *,
    dtype: pl.DataType = pl.Utf8,
    default: str = "",
) -> pl.Expr:
    existing = [pl.col(name).cast(dtype, strict=False) for name in columns if name in frame.columns]
    if not existing:
        return pl.lit(default, dtype=dtype)
    return pl.coalesce(existing).fill_null(default)


def _prepare_for_aggregation(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalise the raw weekly stats ahead of aggregation."""

    if frame.is_empty():
        return frame

    filtered = frame.filter(pl.col("season").is_in(OFFENSE_SEASONS))
    if "season_type" in filtered.columns:
        filtered = filtered.filter(pl.col("season_type") == "REG")

    team_expr = _first_available_expr(
        filtered,
        ["recent_team", "team", "current_team_abbr"],
    ).str.to_uppercase().alias("_team")

    position_expr = _first_available_expr(
        filtered,
        ["player_position", "position", "position_group"],
    ).str.to_uppercase().alias("_position")

    position_group_expr = _first_available_expr(
        filtered,
        ["position_group", "player_position", "position"],
    ).str.to_uppercase().alias("_position_group")

    player_name_expr = _first_available_expr(
        filtered,
        ["player_name", "player_display_name"],
    ).alias("_player_name")

    prepared = (
        filtered.with_columns(
            [
                team_expr,
                position_expr,
                position_group_expr,
                player_name_expr,
                pl.col("week").cast(pl.Int16, strict=False).alias("_week"),
                pl.col("completions").cast(pl.Float64, strict=False).fill_null(0).alias("_pass_comp"),
                pl.col("attempts").cast(pl.Float64, strict=False).fill_null(0).alias("_pass_att"),
                pl.col("passing_yards").cast(pl.Float64, strict=False).fill_null(0).alias("_pass_yds"),
                pl.col("passing_tds").cast(pl.Float64, strict=False).fill_null(0).alias("_pass_td"),
                pl.col("passing_interceptions").cast(pl.Float64, strict=False).fill_null(0).alias("_pass_int"),
                pl.col("sacks_suffered").cast(pl.Float64, strict=False).fill_null(0).alias("_sacks_taken"),
                pl.col("sack_yards_lost").cast(pl.Float64, strict=False).fill_null(0).alias("_sack_yards"),
                pl.col("carries").cast(pl.Float64, strict=False).fill_null(0).alias("_rush_att"),
                pl.col("rushing_yards").cast(pl.Float64, strict=False).fill_null(0).alias("_rush_yds"),
                pl.col("rushing_tds").cast(pl.Float64, strict=False).fill_null(0).alias("_rush_td"),
                pl.col("targets").cast(pl.Float64, strict=False).fill_null(0).alias("_rec_tgt"),
                pl.col("receptions").cast(pl.Float64, strict=False).fill_null(0).alias("_rec_rec"),
                pl.col("receiving_yards").cast(pl.Float64, strict=False).fill_null(0).alias("_rec_yds"),
                pl.col("receiving_tds").cast(pl.Float64, strict=False).fill_null(0).alias("_rec_td"),
                _first_available_expr(filtered, ["rushing_fumbles"], dtype=pl.Float64, default="0.0")
                .cast(pl.Float64, strict=False)
                .alias("_rush_fum"),
                _first_available_expr(filtered, ["rushing_fumbles_lost"], dtype=pl.Float64, default="0.0")
                .cast(pl.Float64, strict=False)
                .alias("_rush_fum_lost"),
                _first_available_expr(filtered, ["receiving_fumbles"], dtype=pl.Float64, default="0.0")
                .cast(pl.Float64, strict=False)
                .alias("_rec_fum"),
                _first_available_expr(filtered, ["receiving_fumbles_lost"], dtype=pl.Float64, default="0.0")
                .cast(pl.Float64, strict=False)
                .alias("_rec_fum_lost"),
                _first_available_expr(filtered, ["sack_fumbles"], dtype=pl.Float64, default="0.0")
                .cast(pl.Float64, strict=False)
                .alias("_sack_fum"),
                _first_available_expr(filtered, ["sack_fumbles_lost"], dtype=pl.Float64, default="0.0")
                .cast(pl.Float64, strict=False)
                .alias("_sack_fum_lost"),
            ]
        )
    )

    prepared = prepared.filter(pl.col("_position").is_in(OFFENSE_POSITIONS))
    prepared = prepared.sort(["player_id", "season", "_week"])
    return prepared


def _aggregate_player_seasons(prepared: pl.DataFrame) -> pl.DataFrame:
    """Aggregate weekly stats into a player-season cache."""

    if prepared.is_empty():
        return pl.DataFrame(schema=BASIC_OFFENSE_SCHEMA)

    aggregated = (
        prepared.group_by(["player_id", "season"])
        .agg(
            pl.col("_player_name").drop_nulls().last().alias("player_name"),
            pl.col("_position").drop_nulls().last().alias("position"),
            pl.col("_position_group").drop_nulls().last().alias("position_group"),
            pl.col("_team").drop_nulls().last().alias("team"),
            pl.col("_week").drop_nulls().n_unique().alias("games_played"),
            pl.col("_pass_comp").sum().alias("pass_completions"),
            pl.col("_pass_att").sum().alias("pass_attempts"),
            pl.col("_pass_yds").sum().alias("passing_yards"),
            pl.col("_pass_td").sum().alias("passing_tds"),
            pl.col("_pass_int").sum().alias("passing_ints"),
            pl.col("_sacks_taken").sum().alias("sacks_taken"),
            pl.col("_sack_yards").sum().alias("sack_yards"),
            pl.col("_rush_att").sum().alias("rushing_attempts"),
            pl.col("_rush_yds").sum().alias("rushing_yards"),
            pl.col("_rush_td").sum().alias("rushing_tds"),
            pl.col("_rec_tgt").sum().alias("receiving_targets"),
            pl.col("_rec_rec").sum().alias("receiving_receptions"),
            pl.col("_rec_yds").sum().alias("receiving_yards"),
            pl.col("_rec_td").sum().alias("receiving_tds"),
            (
                pl.col("_rush_fum")
                + pl.col("_rec_fum")
                + pl.col("_sack_fum")
            )
            .sum()
            .alias("total_fumbles"),
            (
                pl.col("_rush_fum_lost")
                + pl.col("_rec_fum_lost")
                + pl.col("_sack_fum_lost")
            )
            .sum()
            .alias("fumbles_lost"),
        )
        .with_columns(
            pl.col("team")
            .fill_null("")
            .alias("team"),
            pl.col("position")
            .fill_null("")
            .alias("position"),
            pl.col("position_group")
            .fill_null("")
            .alias("position_group"),
        )
        .with_columns(
            pl.col("games_played").cast(pl.Int16, strict=False),
            pl.col("pass_completions").cast(pl.Int32, strict=False),
            pl.col("pass_attempts").cast(pl.Int32, strict=False),
            pl.col("passing_yards").cast(pl.Int64, strict=False),
            pl.col("passing_tds").cast(pl.Int32, strict=False),
            pl.col("passing_ints").cast(pl.Int32, strict=False),
            pl.col("sacks_taken").cast(pl.Int32, strict=False),
            pl.col("sack_yards").cast(pl.Int64, strict=False),
            pl.col("rushing_attempts").cast(pl.Int32, strict=False),
            pl.col("rushing_yards").cast(pl.Int64, strict=False),
            pl.col("rushing_tds").cast(pl.Int32, strict=False),
            pl.col("receiving_targets").cast(pl.Int32, strict=False),
            pl.col("receiving_receptions").cast(pl.Int32, strict=False),
            pl.col("receiving_yards").cast(pl.Int64, strict=False),
            pl.col("receiving_tds").cast(pl.Int32, strict=False),
            pl.col("total_fumbles").cast(pl.Int32, strict=False),
            pl.col("fumbles_lost").cast(pl.Int32, strict=False),
        )
    )

    ordered = aggregated.select(BASIC_OFFENSE_SCHEMA.keys()).sort(
        ["season", "player_name", "team"]
    )
    return ordered


__all__ = [
    "BASIC_OFFENSE_SCHEMA",
    "CACHE_PATH",
    "OFFENSE_SEASONS",
    "build_basic_offense_cache",
    "cache_exists",
    "load_basic_offense_stats",
    "scan_basic_offense_stats",
]

