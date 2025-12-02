"""Aggregated player-season cache used by the summary views."""

from __future__ import annotations

from datetime import datetime
import time
from pathlib import Path
from typing import Iterable, Mapping, Sequence
import logging

import polars as pl
from requests import HTTPError

try:  # pragma: no cover - optional dependency
    from nflreadpy import load_player_stats, load_rosters
except ImportError:  # pragma: no cover - handled at runtime
    load_player_stats = None  # type: ignore[assignment]
    load_rosters = None  # type: ignore[assignment]

from .pfr.client import PFRClient
from .pfr.snap_counts import fetch_team_snap_counts
from .pfr.players import fetch_player_bio_fields
from .player_bio_cache import (
    load_player_bio_cache,
    upsert_player_bio_entries,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIRECTORY = PROJECT_ROOT / "data" / "cache"
CACHE_PATH = CACHE_DIRECTORY / "basic_player_stats.parquet"
DEFAULT_SEASONS = list(range(1999, datetime.now().year + 1))

LOGGER = logging.getLogger(__name__)

PFR_SNAP_MIN_SEASON = 2012
TEAM_TO_PFR_SLUG: Mapping[str, str] = {
    "ARI": "crd",
    "ATL": "atl",
    "BAL": "rav",
    "BUF": "buf",
    "CAR": "car",
    "CHI": "chi",
    "CIN": "cin",
    "CLE": "cle",
    "DAL": "dal",
    "DEN": "den",
    "DET": "det",
    "GB": "gnb",
    "GNB": "gnb",
    "HOU": "htx",  # Texans (post-2002 seasons)
    "IND": "clt",
    "JAC": "jax",
    "JAX": "jax",
    "KC": "kan",
    "KAN": "kan",
    "LAC": "sdg",
    "SD": "sdg",
    "LAR": "ram",
    "LA": "ram",
    "STL": "ram",
    "LV": "rai",
    "LVR": "rai",
    "OAK": "rai",
    "MIA": "mia",
    "MIN": "min",
    "NE": "nwe",
    "NO": "nor",
    "NYG": "nyg",
    "NYJ": "nyj",
    "PHI": "phi",
    "PIT": "pit",
    "SEA": "sea",
    "SF": "sfo",
    "SFO": "sfo",
    "TB": "tam",
    "TEN": "oti",
    "WAS": "was",
}

STRING_COLUMN_SOURCES: Mapping[str, Sequence[str]] = {
    "_team": ("recent_team", "team", "current_team_abbr"),
    "_player_name": ("player_display_name", "player_name"),
    "_player_id": ("player_id", "gsis_id"),
    "_position": ("position", "player_position"),
    "_position_group": ("position_group", "player_position_group"),
}

NUMERIC_COLUMN_SOURCES: Mapping[str, Sequence[str]] = {
    "offense_snaps": ("offense_snaps", "offense_snaps_played", "offensive_snaps"),
    "offense_snaps_available": ("offense_snaps_available", "team_offense_snaps", "offense_total_snaps"),
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
    "rushing_fumbles": ("rushing_fumbles",),
    "rushing_fumbles_lost": ("rushing_fumbles_lost",),
    "receiving_fumbles": ("receiving_fumbles",),
    "receiving_fumbles_lost": ("receiving_fumbles_lost",),
    "sack_fumbles": ("sack_fumbles",),
    "sack_fumbles_lost": ("sack_fumbles_lost",),
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


def _map_team_to_pfr_slug(team: str | None) -> str | None:
    if not team:
        return None
    return TEAM_TO_PFR_SLUG.get(team.upper())


def _fetch_snap_counts_with_retry(
    client: PFRClient,
    *,
    team_abbr: str,
    team_slug: str,
    season: int,
    retries: int = 4,
) -> pl.DataFrame:
    for attempt in range(retries):
        try:
            return fetch_team_snap_counts(client, team_slug=team_slug, season=season)
        except HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            if status == 429 and attempt < retries - 1:
                wait = 5.0 * (attempt + 1)
                LOGGER.warning(
                    "PFR rate-limited snap counts for %s (%s); retrying in %.1fs.",
                    team_abbr,
                    season,
                    wait,
                )
                time.sleep(wait)
                continue
            raise
    # Should never reach here because loop either returns or raises
    raise RuntimeError("Exceeded retry attempts for PFR snap counts")


def _fetch_player_bio_with_retry(
    client: PFRClient,
    *,
    pfr_id: str,
    retries: int = 3,
) -> dict[str, str]:
    for attempt in range(retries):
        try:
            return fetch_player_bio_fields(client, pfr_id)
        except HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            if status == 429 and attempt < retries - 1:
                wait = 2.0 * (attempt + 1)
                LOGGER.warning(
                    "PFR rate-limited bio details for %s; retrying in %.1fs.",
                    pfr_id,
                    wait,
                )
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Exceeded retry attempts for PFR player bio")


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

    id_mapping = _build_player_id_mapping(target_seasons)

    # Merge snap counts from separate source
    aggregated = _merge_snap_counts(aggregated, target_seasons, id_mapping=id_mapping)

    # Merge player bio info from PFR
    aggregated = _merge_player_bio(aggregated, id_mapping=id_mapping)

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


def _build_player_id_mapping(seasons: Sequence[int]) -> pl.DataFrame | None:
    """Build a mapping from gsis_id to pfr_id using roster data."""

    if load_rosters is None:
        return None

    try:
        roster_raw = load_rosters(seasons=list(seasons))
        roster_frame = _to_polars(roster_raw)
    except Exception as exc:  # pragma: no cover - network/runtime
        LOGGER.warning("Failed to load rosters for ID mapping: %s", exc)
        return None

    if roster_frame.is_empty():
        return None

    if "gsis_id" not in roster_frame.columns or "pfr_id" not in roster_frame.columns:
        return None

    # Create unique mapping from gsis_id to pfr_id
    mapping = (
        roster_frame.select(["gsis_id", "pfr_id"])
        .filter(pl.col("gsis_id").is_not_null() & pl.col("pfr_id").is_not_null())
        .unique(subset=["gsis_id"])
    )
    return mapping


def _merge_snap_counts(
    aggregated: pl.DataFrame,
    seasons: Sequence[int],
    *,
    id_mapping: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Merge snap counts scraped from Pro-Football-Reference into the stats."""

    if aggregated.is_empty():
        return aggregated

    if id_mapping is None or id_mapping.is_empty():
        LOGGER.warning("Could not build player ID mapping; snap counts will remain at 0.")
        return aggregated

    target_seasons = sorted(
        {int(season) for season in seasons if season and season >= PFR_SNAP_MIN_SEASON}
    )
    if not target_seasons:
        LOGGER.info(
            "PFR snap counts are only available from %s onward; no merge performed.",
            PFR_SNAP_MIN_SEASON,
        )
        return aggregated

    season_team_frame = (
        aggregated.select(["season", "team"])
        .filter(
            pl.col("team").is_not_null()
            & (pl.col("team").str.strip_chars() != "")
        )
        .unique()
    )
    if season_team_frame.is_empty():
        return aggregated

    snap_frames: list[pl.DataFrame] = []
    with PFRClient(enable_cache=True, min_delay=1.5) as client:
        for season in target_seasons:
            teams = (
                season_team_frame.filter(pl.col("season") == season)["team"].to_list()
            )
            if not teams:
                continue
            for team in teams:
                slug = _map_team_to_pfr_slug(team)
                if not slug:
                    LOGGER.debug("No PFR slug mapping for team '%s'; skipping.", team)
                    continue
                try:
                    team_snaps = _fetch_snap_counts_with_retry(
                        client,
                        team_abbr=team.upper(),
                        team_slug=slug,
                        season=season,
                    )
                except Exception as exc:  # pragma: no cover - network/runtime
                    LOGGER.warning(
                        "Failed to fetch PFR snap counts for %s (%s): %s",
                        team,
                        season,
                        exc,
                    )
                    continue
                if team_snaps.is_empty():
                    continue
                snap_frames.append(
                    team_snaps.with_columns(
                        pl.lit(team.upper()).alias("team"),
                        pl.lit(season).alias("season"),
                    )
                )

    if not snap_frames:
        LOGGER.warning("PFR snap-count scrape returned no rows; leaving snaps at 0.")
        return aggregated

    snap_aggregated = (
        pl.concat(snap_frames, how="vertical_relaxed")
        .filter(pl.col("pfr_id").is_not_null() & (pl.col("pfr_id") != ""))
        .select(
            [
                pl.col("pfr_id").cast(pl.Utf8, strict=False),
                pl.col("season").cast(pl.Int64, strict=False),
                pl.col("team").cast(pl.Utf8, strict=False),
                pl.col("_snap_offense").cast(pl.Int64, strict=False),
                pl.col("_snap_defense").cast(pl.Int64, strict=False),
                pl.col("_snap_st").cast(pl.Int64, strict=False),
            ]
        )
    )

    if snap_aggregated.is_empty():
        LOGGER.warning("PFR snap-count data frame empty after filtering.")
        return aggregated

    aggregated_with_pfr = aggregated.join(
        id_mapping,
        left_on="player_id",
        right_on="gsis_id",
        how="left",
    )

    merged = aggregated_with_pfr.join(
        snap_aggregated,
        left_on=["pfr_id", "season", "team"],
        right_on=["pfr_id", "season", "team"],
        how="left",
    )

    # Update snap columns with merged values
    update_exprs = []
    if "_snap_offense" in merged.columns:
        update_exprs.append(
            pl.when(pl.col("_snap_offense").is_not_null() & (pl.col("_snap_offense") > 0))
            .then(pl.col("_snap_offense"))
            .otherwise(pl.col("offense_snaps"))
            .cast(pl.Float64, strict=False)
            .alias("offense_snaps")
        )
    if "_snap_defense" in merged.columns:
        update_exprs.append(
            pl.when(pl.col("_snap_defense").is_not_null() & (pl.col("_snap_defense") > 0))
            .then(pl.col("_snap_defense"))
            .otherwise(pl.col("defense_snaps"))
            .cast(pl.Float64, strict=False)
            .alias("defense_snaps")
        )
    if "_snap_st" in merged.columns:
        update_exprs.append(
            pl.when(pl.col("_snap_st").is_not_null() & (pl.col("_snap_st") > 0))
            .then(pl.col("_snap_st"))
            .otherwise(pl.col("special_teams_snaps"))
            .cast(pl.Float64, strict=False)
            .alias("special_teams_snaps")
        )

    if update_exprs:
        merged = merged.with_columns(update_exprs)

    # Drop temporary columns
    drop_cols = [c for c in merged.columns if c.startswith("_snap_") or c == "pfr_id"]
    if drop_cols:
        merged = merged.drop(drop_cols)

    return merged


def _merge_player_bio(
    aggregated: pl.DataFrame,
    *,
    id_mapping: pl.DataFrame | None,
) -> pl.DataFrame:
    """Attach PFR-derived bio info (handedness, birthplace) to the stats."""

    if aggregated.is_empty():
        return aggregated

    if id_mapping is None or id_mapping.is_empty():
        LOGGER.warning("Could not build player ID mapping; bio fields will remain 'N/A'.")
        return aggregated.with_columns(
            pl.lit("N/A").alias("handedness"),
            pl.lit("N/A").alias("birth_city"),
            pl.lit("N/A").alias("birth_state"),
            pl.lit("N/A").alias("birth_country"),
        )

    bio_cache = load_player_bio_cache()

    player_map = (
        aggregated.select(["player_id"])
        .unique()
        .join(id_mapping, left_on="player_id", right_on="gsis_id", how="left")
        .filter(pl.col("pfr_id").is_not_null() & (pl.col("pfr_id") != ""))
    )

    existing_ids = (
        bio_cache.select(["pfr_id"]).unique()
        if bio_cache.height > 0
        else None
    )
    if existing_ids is not None:
        missing = player_map.join(existing_ids, on="pfr_id", how="anti")
    else:
        missing = player_map

    new_entries: list[dict[str, str]] = []
    if missing.height > 0:
        with PFRClient(enable_cache=True, min_delay=1.0) as client:
            for row in missing.iter_rows(named=True):
                pfr_id = row.get("pfr_id")
                if not isinstance(pfr_id, str) or not pfr_id.strip():
                    continue
                try:
                    bio = _fetch_player_bio_with_retry(client, pfr_id=pfr_id)
                except Exception as exc:  # pragma: no cover - network/runtime
                    LOGGER.warning("Failed to fetch PFR bio for %s: %s", pfr_id, exc)
                    continue
                new_entries.append(
                    {
                        "pfr_id": pfr_id,
                        "handedness": bio.get("handedness") or "N/A",
                        "birth_city": bio.get("birth_city") or "N/A",
                        "birth_state": bio.get("birth_state") or "N/A",
                        "birth_country": bio.get("birth_country") or "N/A",
                    }
                )

    if new_entries:
        bio_cache = upsert_player_bio_entries(bio_cache, new_entries)

    if bio_cache.height == 0:
        return aggregated.with_columns(
            pl.lit("N/A").alias("handedness"),
            pl.lit("N/A").alias("birth_city"),
            pl.lit("N/A").alias("birth_state"),
            pl.lit("N/A").alias("birth_country"),
        )

    merged = aggregated.join(
        id_mapping,
        left_on="player_id",
        right_on="gsis_id",
        how="left",
    ).join(
        bio_cache,
        on="pfr_id",
        how="left",
    )

    merged = merged.with_columns(
        pl.col("handedness").fill_null("N/A"),
        pl.col("birth_city").fill_null("N/A"),
        pl.col("birth_state").fill_null("N/A"),
        pl.col("birth_country").fill_null("N/A"),
    )

    drop_cols = [c for c in merged.columns if c == "pfr_id"]
    if drop_cols:
        merged = merged.drop(drop_cols)
    return merged


__all__ = [
    "CACHE_PATH",
    "cache_exists",
    "scan_basic_cache",
    "load_basic_cache",
    "build_basic_cache",
]

