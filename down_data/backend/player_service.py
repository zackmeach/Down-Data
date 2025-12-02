"""Backend helpers that bridge the UI with the Player domain object."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
import logging
import math
import re

import polars as pl

from down_data.core import Player, PlayerProfile, PlayerQuery, PlayerNotFoundError
from down_data.core.ratings import RatingBreakdown
from down_data.data.player_bio_cache import (
    load_player_bio_cache,
    upsert_player_bio_entries,
    fetch_and_cache_player_bio,
)
from .offense_stats_repository import BasicOffenseStatsRepository
from .basic_player_stats_repository import BasicPlayerStatsRepository
from .player_impact_repository import PlayerImpactRepository
from .player_summary_repository import PlayerSummaryRepository
from .nfl_data_repository import NFLDataRepository

try:  # pragma: no cover - defensive import
    from nflreadpy import load_players, load_player_stats, load_schedules, load_contracts
except ImportError:  # pragma: no cover - imported dynamically in some environments
    load_players = None  # type: ignore
    load_player_stats = None  # type: ignore
    load_schedules = None  # type: ignore
    load_contracts = None  # type: ignore

logger = logging.getLogger(__name__)

_OFFENSE_POSITIONS = {
    "QB",
    "RB",
    "FB",
    "WR",
    "TE",
}

_DEFENSE_POSITIONS = {
    "DL",
    "DE",
    "DT",
    "NT",
    "LB",
    "ILB",
    "OLB",
    "MLB",
    "DB",
    "CB",
    "S",
    "SS",
    "FS",
}

_DEFAULT_RATING_SEASON_WINDOW = 3

RATING_CONFIG: dict[str, list[dict[str, object]]] = {
    "QB": [
        {
            "label": "Passing Production",
            "metrics": [
                {"label": "Pass Yards", "key": "pass_yards"},
                {"label": "Total TD", "key": "total_touchdowns"},
            ],
        },
        {
            "label": "Rushing Threat",
            "metrics": [
                {"label": "Rush Yards", "key": "rush_yards"},
            ],
        },
    ],
    "RB": [
        {
            "label": "Rushing Impact",
            "metrics": [
                {"label": "Rush Yards", "key": "rush_yards"},
                {"label": "Total TD", "key": "total_touchdowns"},
            ],
        },
        {
            "label": "Receiving Support",
            "metrics": [
                {"label": "Receiving Yards", "key": "receiving_yards"},
            ],
        },
    ],
    "WR": [
        {
            "label": "Receiving Production",
            "metrics": [
                {"label": "Receiving Yards", "key": "receiving_yards"},
                {"label": "Total TD", "key": "total_touchdowns"},
            ],
        },
        {
            "label": "Open Field Threat",
            "metrics": [
                {"label": "Rush Yards", "key": "rush_yards"},
            ],
        },
    ],
    "TE": [
        {
            "label": "Receiving Production",
            "metrics": [
                {"label": "Receiving Yards", "key": "receiving_yards"},
                {"label": "Total TD", "key": "total_touchdowns"},
            ],
        },
        {
            "label": "Versatility",
            "metrics": [
                {"label": "Rush Yards", "key": "rush_yards"},
            ],
        },
    ],
    "default_offense": [
        {
            "label": "Offensive Production",
            "metrics": [
                {"label": "Pass Yards", "key": "pass_yards"},
                {"label": "Rush Yards", "key": "rush_yards"},
                {"label": "Receiving Yards", "key": "receiving_yards"},
                {"label": "Total TD", "key": "total_touchdowns"},
            ],
        },
    ],
    "defense": [
        {
            "label": "Tackling",
            "metrics": [
                {"label": "Total Tackles", "key": "def_tackles_total"},
                {"label": "Solo Tackles", "key": "def_tackles_solo"},
                {"label": "Assisted Tackles", "key": "def_tackles_assisted"},
            ],
        },
        {
            "label": "Playmaking",
            "metrics": [
                {"label": "Interceptions", "key": "def_interceptions"},
            ],
        },
    ],
}

RATING_POSITION_GROUPS: dict[str, set[str]] = {
    "QB": {"QB"},
    "RB": {"RB", "FB"},
    "WR": {"WR"},
    "TE": {"TE"},
    "default_offense": _OFFENSE_POSITIONS,
    "defense": _DEFENSE_POSITIONS,
}


@dataclass(frozen=True)
class PlayerSummary:
    """Lightweight player record used by the UI search view."""

    profile: PlayerProfile
    team: str | None
    position: str | None

    def to_query(self) -> PlayerQuery:
        return PlayerQuery(
            name=self.profile.full_name,
            team=self.team,
            position=self.position,
            draft_year=self.profile.draft_year,
            draft_team=self.profile.draft_team,
        )


class PlayerDirectory:
    """Caches the full nflreadpy player directory for quick search access."""

    def __init__(self) -> None:
        self._frame: pl.DataFrame | None = None
        self._contracts: pl.DataFrame | None = None

    @staticmethod
    def _to_polars(frame: object) -> pl.DataFrame:
        """Coerce third-party dataframes into Polars instances."""

        if isinstance(frame, pl.DataFrame):
            return frame
        try:
            return pl.DataFrame(frame)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"Unsupported frame type returned by nflreadpy.load_players: {type(frame)!r}"
            ) from exc

    def _load_contracts_frame(self) -> pl.DataFrame:
        if self._contracts is not None:
            return self._contracts
        if load_contracts is None:
            logger.debug("nflreadpy.load_contracts unavailable; skipping contract enrichment")
            self._contracts = pl.DataFrame()
            return self._contracts
        try:
            loaded = load_contracts()
            contracts = self._to_polars(loaded)
        except Exception as exc:  # pragma: no cover - runtime fetch can fail without network
            logger.warning("Failed to load player contracts: %s", exc)
            self._contracts = pl.DataFrame()
            return self._contracts

        if "gsis_id" not in contracts.columns:
            self._contracts = pl.DataFrame()
            return self._contracts

        wanted_columns = [
            "gsis_id",
            "otc_id",
            "year_signed",
            "years",
            "value",
            "guaranteed",
            "apy",
            "apy_cap_pct",
            "inflated_value",
            "inflated_apy",
            "inflated_guaranteed",
            "player_page",
        ]
        available = [column for column in wanted_columns if column in contracts.columns]
        if not available:
            self._contracts = pl.DataFrame()
            return self._contracts

        shaped = contracts.select(
            [
                pl.col("gsis_id")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("gsis_id"),
                *(
                    pl.col(column).alias("contract_player_page" if column == "player_page" else ("contract_otc_id" if column == "otc_id" else column))
                    for column in available
                    if column != "gsis_id"
                ),
            ]
        ).filter(pl.col("gsis_id").is_not_null() & (pl.col("gsis_id") != ""))

        if "year_signed" in shaped.columns:
            shaped = shaped.sort("year_signed", nulls_last=True)
        shaped = shaped.unique(subset=["gsis_id"], keep="last")

        if {"year_signed", "years"}.issubset(set(shaped.columns)):
            shaped = shaped.with_columns(
                pl.when(
                    pl.col("year_signed").is_not_null() & pl.col("years").is_not_null()
                )
                .then(
                    (
                        pl.col("year_signed").cast(pl.Float64, strict=False)
                        + pl.col("years").cast(pl.Float64, strict=False)
                        - 1
                    ).cast(pl.Int64, strict=False)
                )
                .alias("signed_through")
            )

        self._contracts = shaped
        return self._contracts

    @cached_property
    def frame(self) -> pl.DataFrame:
        if load_players is None:
            logger.warning("nflreadpy.load_players is unavailable; returning empty frame")
            return pl.DataFrame()
        try:
            loaded = load_players()
            players = self._to_polars(loaded)
        except Exception as exc:  # pragma: no cover - runtime fetch can fail without network
            logger.warning("Failed to load player directory: %s", exc)
            return pl.DataFrame()

        # Harmonise column names the rest of the code expects.
        if "recent_team" not in players.columns and "latest_team" in players.columns:
            players = players.rename({"latest_team": "recent_team"})

        if "full_name" not in players.columns:
            players = players.with_columns(
                pl.when(pl.col("display_name").fill_null("").str.len_chars() > 0)
                .then(pl.col("display_name"))
                .otherwise(
                    pl.concat_str(
                        [
                            pl.col("first_name").fill_null(""),
                            pl.col("last_name").fill_null(""),
                        ],
                        separator=" ",
                    ).str.strip_chars()
                )
                .alias("full_name")
            )

        normalised = players.with_columns(
            pl.col("full_name").fill_null(""),
            pl.col("display_name").fill_null(""),
            pl.col("position").fill_null(""),
            pl.col("recent_team").fill_null(""),
        )

        contracts = self._load_contracts_frame()
        if contracts.height > 0 and "gsis_id" in normalised.columns:
            normalised = normalised.join(contracts, on="gsis_id", how="left")
            if "contract_otc_id" in normalised.columns and "otc_id" in normalised.columns:
                normalised = normalised.with_columns(
                    pl.when(pl.col("otc_id").is_null() | (pl.col("otc_id") == ""))
                    .then(pl.col("contract_otc_id"))
                    .otherwise(pl.col("otc_id"))
                    .alias("otc_id")
                ).drop("contract_otc_id")

        return normalised

    def search(
        self,
        *,
        name: str,
        team: str | None = None,
        position: str | None = None,
        limit: int = 25,
    ) -> list[PlayerSummary]:
        frame = self.frame
        if frame.height == 0:
            return []

        filters: list[pl.Expr] = []
        if name and (name_query := name.strip()):
            escaped = re.escape(name_query)
            pattern = fr"(?i){escaped}"
            filters.append(
                pl.col("full_name").str.contains(pattern) | pl.col("display_name").str.contains(pattern)
            )
        if team and (team_query := team.strip()):
            filters.append(pl.col("recent_team").str.to_uppercase() == team_query.upper())
        if position and (position_query := position.strip()):
            filters.append(pl.col("position").str.to_uppercase() == position_query.upper())

        filtered = frame.filter(pl.all_horizontal(filters)) if filters else frame
        trimmed = filtered.head(max(limit, 0))

        results: list[PlayerSummary] = []
        for row in trimmed.iter_rows(named=True):
            profile = PlayerProfile.from_row(row)
            results.append(
                PlayerSummary(
                    profile=profile,
                    team=row.get("recent_team") or row.get("team"),
                    position=profile.position,
                )
            )
        return results


class PlayerService:
    """Facade used by the UI layer to work with players."""

    def __init__(self, directory: PlayerDirectory | None = None) -> None:
        self.directory = directory or PlayerDirectory()
        self._stats_cache: dict[tuple[str, str, str, str], pl.DataFrame] = {}
        self._rating_baselines: dict[str, dict[str, tuple[float, float]]] = {}
        self._rating_seasons = self._compute_rating_seasons()
        self._schedule_cache: dict[int, pl.DataFrame] = {}
        self._team_record_cache: dict[tuple[str, int, str], tuple[int, int, int]] = {}
        self._qb_impact_cache: dict[tuple[str, str], dict[int, dict[str, float]]] = {}
        self._defense_impact_cache: dict[tuple[str, str], dict[int, dict[str, float]]] = {}
        self._skill_impact_cache: dict[tuple[str, str], dict[int, dict[str, float]]] = {}
        self._offensive_line_impact_cache: dict[tuple[str, str], dict[int, dict[str, float]]] = {}
        self._kicker_impact_cache: dict[tuple[str, str], dict[int, dict[str, float]]] = {}
        self._punter_impact_cache: dict[tuple[str, str], dict[int, dict[str, float]]] = {}
        self._offense_stats_repository = BasicOffenseStatsRepository()
        self._basic_player_stats_repository = BasicPlayerStatsRepository()
        self._player_impact_repository = PlayerImpactRepository()
        self._player_summary_repository = PlayerSummaryRepository()
        self._player_bio_cache: pl.DataFrame | None = None
        # New unified data repository (preferred data source)
        self._nfl_data_repository: NFLDataRepository | None = None

    @property
    def nfl_data(self) -> NFLDataRepository:
        """Get the NFL Data Repository instance (lazy initialization)."""
        if self._nfl_data_repository is None:
            self._nfl_data_repository = NFLDataRepository(auto_initialize=True)
        return self._nfl_data_repository

    def _use_nfl_datastore(self) -> bool:
        """Check if the new NFL data store is available and should be used."""
        try:
            from down_data.data.nfl_datastore import DATA_DIRECTORY
            return (DATA_DIRECTORY / "players.parquet").exists()
        except Exception:
            return False

    def get_all_players(self) -> pl.DataFrame:
        """Get the full player directory as a DataFrame for filtering."""
        return self.directory.frame

    def search_players(
        self,
        *,
        name: str,
        team: str | None = None,
        position: str | None = None,
        limit: int = 25,
    ) -> list[PlayerSummary]:
        """Search for player summaries matching the provided filters."""

        if not name and not team and not position:
            return []
        return self.directory.search(name=name, team=team, position=position, limit=limit)

    def load_player(self, query: PlayerQuery | PlayerSummary) -> Player:
        """Instantiate a full Player domain object."""

        resolved_query = query.to_query() if isinstance(query, PlayerSummary) else query
        try:
            return Player(
                name=resolved_query.name,
                team=resolved_query.team,
                draft_year=resolved_query.draft_year,
                draft_team=resolved_query.draft_team,
                position=resolved_query.position,
            )
        except PlayerNotFoundError:
            raise
        except Exception:
            logger.exception("Unexpected error initialising Player from query: %s", resolved_query)
            raise

    def load_player_profile(self, query: PlayerQuery | PlayerSummary) -> PlayerProfile:
        """Convenience method to fetch only the profile information."""

        player = self.load_player(query)
        return player.profile

    def _stats_cache_key(
        self,
        player: Player,
        seasons: Iterable[int] | bool | None,
        season_type: str | None,
        summary_level: str | None,
    ) -> tuple[str, str, str, str]:
        identifier = player.profile.gsis_id or player.profile.full_name
        if isinstance(seasons, bool) or seasons is None:
            seasons_repr = str(seasons)
        elif isinstance(seasons, Iterable):
            try:
                seasons_repr = ",".join(str(season) for season in seasons)  # type: ignore[arg-type]
            except TypeError:
                seasons_repr = str(seasons)
        else:
            seasons_repr = str(seasons)
        season_type_repr = (season_type or "ALL").upper()
        summary_level_repr = (summary_level or "WEEK").lower()
        return (identifier, seasons_repr, season_type_repr, summary_level_repr)

    def get_player_stats(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | bool | None = True,
        season_type: str | None = "REG",
        summary_level: str | None = "week",
    ) -> pl.DataFrame:
        """Fetch player stats with an internal cache."""

        key = self._stats_cache_key(player, seasons, season_type, summary_level)
        if key in self._stats_cache:
            return self._stats_cache[key].clone()

        stats = player.fetch_stats(
            seasons=seasons,
            season_type=season_type,
            summary_level=summary_level,
        )
        if stats is None:
            stats = pl.DataFrame()
        self._stats_cache[key] = stats.clone()
        return stats

    def fetch_player_stats(
        self,
        query: PlayerQuery | PlayerSummary,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str | None = None,
        summary_level: str | None = None,
    ) -> pl.DataFrame:
        player = self.load_player(query)
        kwargs: dict[str, object] = {}
        if seasons is not None:
            kwargs["seasons"] = seasons
        if season_type is not None:
            kwargs["season_type"] = season_type
        if summary_level is not None:
            kwargs["summary_level"] = summary_level
        return self.get_player_stats(player, **kwargs)

    def get_basic_offense_stats(
        self,
        *,
        player_id: str | None = None,
        player_ids: Iterable[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
        refresh_cache: bool = False,
    ) -> pl.DataFrame:
        """Return cached basic offense stats, optionally refreshing the cache."""

        try:
            identifiers: list[str] | None = None
            if player_id or player_ids:
                ids: list[str] = []
                if player_id:
                    ids.append(str(player_id))
                if player_ids:
                    ids.extend(str(pid) for pid in player_ids)
                identifiers = ids

            return self._offense_stats_repository.query(
                player_ids=identifiers,
                seasons=seasons,
                team=team,
                position=position,
                refresh=refresh_cache,
            )
        except Exception as exc:
            logger.warning(
                "Failed to load basic offense cache (refresh=%s): %s",
                refresh_cache,
                exc,
            )
            return pl.DataFrame()

    def get_player_summary_stats(
        self,
        *,
        player_id: str | None = None,
        seasons: Iterable[int] | None = None,
        refresh_cache: bool = False,
    ) -> pl.DataFrame:
        """Return season-level stats from the summary cache.
        
        Prefers the new NFL Data Store when available, falling back to
        the legacy cache system otherwise.
        """
        if not player_id:
            return pl.DataFrame()

        # Try new data store first
        if self._use_nfl_datastore():
            try:
                result = self.nfl_data.get_player_summary(player_id, seasons=seasons)
                if result.height > 0:
                    return result
            except Exception as exc:
                logger.debug(
                    "Failed to query NFL data store for %s: %s", player_id, exc
                )

        # Fall back to legacy repository
        try:
            return self._player_summary_repository.query(
                player_ids=[str(player_id)],
                seasons=seasons,
                refresh=refresh_cache,
            )
        except FileNotFoundError:
            logger.debug(
                "Player summary cache missing; falling back to legacy stat sources."
            )
        except Exception as exc:
            logger.warning(
                "Failed to load player summary cache (refresh=%s): %s",
                refresh_cache,
                exc,
            )
        return pl.DataFrame()

    def get_basic_player_stats(
        self,
        *,
        player_id: str | None = None,
        player_ids: Iterable[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
        refresh_cache: bool = False,
    ) -> pl.DataFrame:
        """Return cached multi-positional stats from the aggregated player cache.
        
        Prefers the new NFL Data Store when available.
        """
        identifiers: list[str] | None = None
        if player_id or player_ids:
            ids: list[str] = []
            if player_id:
                ids.append(str(player_id))
            if player_ids:
                ids.extend(str(pid) for pid in player_ids)
            identifiers = ids

        # Try new data store first
        if self._use_nfl_datastore():
            try:
                result = self.nfl_data.get_summary_stats(
                    player_ids=identifiers,
                    seasons=seasons,
                    team=team,
                    position=position,
                )
                if result.height > 0:
                    return result
            except Exception as exc:
                logger.debug(
                    "Failed to query NFL data store: %s", exc
                )

        # Fall back to legacy repository
        try:
            return self._basic_player_stats_repository.query(
                player_ids=identifiers or None,
                seasons=seasons,
                team=team,
                position=position,
                refresh=refresh_cache,
            )
        except Exception as exc:
            logger.warning(
                "Failed to load basic player cache (refresh=%s): %s",
                refresh_cache,
                exc,
            )
            return pl.DataFrame()

    def _load_cached_impacts(self, player: Player, seasons: Sequence[int]) -> pl.DataFrame:
        """Return cached impact rows for the player when available.
        
        Prefers the new NFL Data Store when available.
        """
        if not seasons:
            return pl.DataFrame()

        player_id = player.profile.gsis_id
        if not player_id:
            return pl.DataFrame()

        # Try new data store first
        if self._use_nfl_datastore():
            try:
                result = self.nfl_data.get_player_impacts(player_id, seasons=seasons)
                if result.height > 0:
                    return result
            except Exception as exc:
                logger.debug(
                    "Failed to query NFL data store impacts for %s: %s", player_id, exc
                )

        # Fall back to legacy repository
        repo = getattr(self, "_player_impact_repository", None)
        if repo is None:
            return pl.DataFrame()

        try:
            return repo.query(player_ids=[player_id], seasons=seasons)
        except FileNotFoundError:
            logger.debug("Player impact cache missing; falling back to live computations.")
        except Exception as exc:
            logger.debug(
                "Failed to query player impact cache for %s (%s): %s",
                player.profile.full_name,
                player_id,
                exc,
            )
        return pl.DataFrame()

    def _load_schedule(self, season: int) -> pl.DataFrame:
        """Fetch and cache the league schedule for the given season."""

        if season in self._schedule_cache:
            return self._schedule_cache[season]

        if load_schedules is None:
            schedule = pl.DataFrame()
        else:
            try:
                schedule = load_schedules(seasons=[season])
            except Exception as exc:  # pragma: no cover - runtime fetch can fail
                logger.debug("Failed to load schedule for %s: %s", season, exc)
                schedule = pl.DataFrame()

        if not isinstance(schedule, pl.DataFrame):
            schedule = pl.DataFrame(schedule)  # type: ignore[arg-type]

        self._schedule_cache[season] = schedule
        return schedule

    def _get_player_bio_cache(self) -> pl.DataFrame:
        if self._player_bio_cache is None:
            try:
                self._player_bio_cache = load_player_bio_cache()
            except Exception:
                self._player_bio_cache = pl.DataFrame(
                    {
                        "pfr_id": [],
                        "handedness": [],
                        "birth_city": [],
                        "birth_state": [],
                        "birth_country": [],
                    }
                )
        return self._player_bio_cache

    def fetch_player_bio_details(self, player: Player) -> dict[str, str]:
        """Return handedness and birthplace info, scraping PFR when needed.
        
        Prefers the new NFL Data Store when available for bio data that has
        already been fetched, falling back to PFR scraping for missing data.
        """
        gsis_id = player.profile.gsis_id
        pfr_id = player.profile.pfr_id

        # Try new data store first (for already-fetched bio data)
        if gsis_id and self._use_nfl_datastore():
            try:
                bio = self.nfl_data.get_player_bio(gsis_id)
                # Check if bio has real data (not just N/A placeholders)
                if bio and any(
                    v and v != "N/A" for k, v in bio.items() 
                    if k in ("handedness", "birth_city", "birth_state")
                ):
                    return bio
            except Exception as exc:
                logger.debug("Failed to get bio from data store for %s: %s", gsis_id, exc)

        # If no PFR ID, we can't fetch from PFR
        if not pfr_id:
            return {}

        # Try legacy cache
        cache = self._get_player_bio_cache()
        match = cache.filter(pl.col("pfr_id") == pfr_id)
        if match.height > 0:
            return match.to_dicts()[0]

        # Fetch from PFR
        try:
            payload, updated_cache = fetch_and_cache_player_bio(
                pfr_id=pfr_id,
                cache=cache,
            )
        except Exception:
            return {}

        self._player_bio_cache = updated_cache
        
        # Also update new data store if available
        if gsis_id and self._use_nfl_datastore():
            try:
                self.nfl_data.update_player_bio(gsis_id, payload)
            except Exception:
                pass
        
        return payload

    def get_team_record(
        self,
        team: str | None,
        season: int,
        *,
        season_type: str = "REG",
    ) -> tuple[int, int, int]:
        """Return (wins, losses, ties) for a team in the specified season."""

        if not team:
            return (0, 0, 0)

        team_key = team.strip().upper()
        if not team_key:
            return (0, 0, 0)

        cache_key = (team_key, season, season_type.upper())
        if cache_key in self._team_record_cache:
            return self._team_record_cache[cache_key]

        schedule = self._load_schedule(season)
        if schedule.is_empty():
            self._team_record_cache[cache_key] = (0, 0, 0)
            return (0, 0, 0)

        filtered = schedule.filter(
            (pl.col("season") == season)
            & (pl.col("game_type").str.to_uppercase() == season_type.upper())
            & (
                (pl.col("home_team").str.to_uppercase() == team_key)
                | (pl.col("away_team").str.to_uppercase() == team_key)
            )
        )

        wins = losses = ties = 0
        for row in filtered.iter_rows(named=True):
            home_team = str(row.get("home_team") or "").upper()
            away_team = str(row.get("away_team") or "").upper()
            home_score = row.get("home_score")
            away_score = row.get("away_score")

            if home_score is None or away_score is None:
                continue

            try:
                home_score = int(home_score)
                away_score = int(away_score)
            except (TypeError, ValueError):
                continue

            if home_team == team_key:
                if home_score > away_score:
                    wins += 1
                elif home_score < away_score:
                    losses += 1
                else:
                    ties += 1
            elif away_team == team_key:
                if away_score > home_score:
                    wins += 1
                elif away_score < home_score:
                    losses += 1
                else:
                    ties += 1

        self._team_record_cache[cache_key] = (wins, losses, ties)
        return (wins, losses, ties)

    def get_quarterback_epa_wpa(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Aggregate QB EPA/WPA totals per season using play-by-play data."""

        identifier = player.profile.gsis_id or player.profile.full_name
        normalized_type = season_type.upper()
        cache_key = (identifier, normalized_type)
        cache_map = self._qb_impact_cache.setdefault(cache_key, {})

        def _build_result(targets: list[int] | None) -> dict[int, dict[str, float]]:
            if targets:
                return {season: dict(cache_map[season]) for season in targets if season in cache_map}
            return {season: dict(values) for season, values in cache_map.items()}

        season_list: list[int] | None = None
        if seasons is not None:
            season_list = sorted({int(season) for season in seasons if season is not None})

        if season_list:
            missing = [season for season in season_list if season not in cache_map]
            if missing:
                repo_frame = self._load_cached_impacts(player, missing)
                repo_populated: set[int] = set()
                for row in repo_frame.iter_rows(named=True):
                    season_value = row.get("season")
                    if season_value is None:
                        continue
                    entry: dict[str, float] = {}
                    epa_value = row.get("qb_epa")
                    if epa_value is not None:
                        entry["epa"] = float(epa_value)
                    wpa_value = row.get("qb_wpa")
                    if wpa_value is not None:
                        entry["wpa"] = float(wpa_value)
                    if entry:
                        cache_map[int(season_value)] = entry
                        repo_populated.add(int(season_value))
                missing = [season for season in missing if season not in repo_populated]
            if not missing:
                return _build_result(season_list)
            pbp_seasons = missing
        else:
            if cache_map:
                return _build_result(None)
            pbp_seasons = True

        try:
            pbp = player.fetch_pbp(seasons=pbp_seasons)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to fetch play-by-play for %s: %s", identifier, exc)
            return _build_result(season_list)

        if pbp.is_empty() or "season" not in pbp.columns:
            return _build_result(season_list)

        filtered = pbp
        if "season_type" in filtered.columns:
            filtered = filtered.filter(pl.col("season_type").str.to_uppercase() == normalized_type)

        if filtered.is_empty():
            return _build_result(season_list)

        player_id = player.profile.gsis_id

        epa_expr: pl.Expr | None = None
        if "qb_epa" in filtered.columns:
            epa_expr = pl.col("qb_epa").cast(pl.Float64, strict=False).fill_null(0.0).alias("_qb_epa_value")
        elif player_id and "epa" in filtered.columns:
            epa_stat = pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0)
            qb_conditions = []
            if "passer_player_id" in filtered.columns:
                qb_conditions.append(pl.col("passer_player_id") == player_id)
            if "rusher_player_id" in filtered.columns:
                qb_conditions.append(pl.col("rusher_player_id") == player_id)
            if qb_conditions:
                epa_expr = (
                    pl.when(pl.any_horizontal(qb_conditions))
                    .then(epa_stat)
                    .otherwise(0.0)
                    .alias("_qb_epa_value")
                )

        wpa_expr: pl.Expr | None = None
        if "qb_wpa" in filtered.columns:
            wpa_expr = pl.col("qb_wpa").cast(pl.Float64, strict=False).fill_null(0.0).alias("_qb_wpa_value")
        elif player_id and "wpa" in filtered.columns:
            wpa_stat = pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0)
            qb_conditions = []
            if "passer_player_id" in filtered.columns:
                qb_conditions.append(pl.col("passer_player_id") == player_id)
            if "rusher_player_id" in filtered.columns:
                qb_conditions.append(pl.col("rusher_player_id") == player_id)
            if qb_conditions:
                wpa_expr = (
                    pl.when(pl.any_horizontal(qb_conditions))
                    .then(wpa_stat)
                    .otherwise(0.0)
                    .alias("_qb_wpa_value")
                )

        agg_exprs: list[pl.Expr] = []
        working = filtered
        if epa_expr is not None:
            working = working.with_columns(epa_expr)
            agg_exprs.append(pl.col("_qb_epa_value").sum().alias("_qb_epa_total"))
        if wpa_expr is not None:
            working = working.with_columns(wpa_expr)
            agg_exprs.append(pl.col("_qb_wpa_value").sum().alias("_qb_wpa_total"))

        if not agg_exprs:
            return _build_result(season_list)

        grouped = (
            working.group_by("season")
            .agg(agg_exprs)
            .sort("season")
        )

        for row in grouped.iter_rows(named=True):
            season_value = row.get("season")
            if season_value is None:
                continue
            entry: dict[str, float] = {}
            epa_total = row.get("_qb_epa_total")
            if epa_total is not None:
                entry["epa"] = float(epa_total)
            wpa_total = row.get("_qb_wpa_total")
            if wpa_total is not None:
                entry["wpa"] = float(wpa_total)
            if entry:
                cache_map[int(season_value)] = entry

        return _build_result(season_list)

    def get_skill_player_impacts(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Aggregate EPA/WPA and explosive-play counts for rushing/receiving roles."""

        identifier = player.profile.gsis_id or player.profile.full_name
        normalized_type = season_type.upper()
        cache_key = (identifier, normalized_type)
        cache_map = self._skill_impact_cache.setdefault(cache_key, {})

        def _build_result(targets: list[int] | None) -> dict[int, dict[str, float]]:
            if targets:
                return {season: dict(cache_map[season]) for season in targets if season in cache_map}
            return {season: dict(values) for season, values in cache_map.items()}

        season_list: list[int] | None = None
        if seasons is not None:
            season_list = sorted({int(season) for season in seasons if season is not None})

        if season_list:
            missing = [season for season in season_list if season not in cache_map]
            if missing:
                repo_frame = self._load_cached_impacts(player, missing)
                repo_populated: set[int] = set()
                for row in repo_frame.iter_rows(named=True):
                    season_value = row.get("season")
                    if season_value is None:
                        continue
                    entry = {
                        "epa": float(row.get("skill_epa") or 0.0),
                        "wpa": float(row.get("skill_wpa") or 0.0),
                        "rush_20_plus": float(row.get("skill_rush_20_plus") or 0.0),
                        "rec_20_plus": float(row.get("skill_rec_20_plus") or 0.0),
                        "rec_first_downs": float(row.get("skill_rec_first_downs") or 0.0),
                    }
                    cache_map[int(season_value)] = entry
                    repo_populated.add(int(season_value))
                missing = [season for season in missing if season not in repo_populated]
            if not missing:
                return _build_result(season_list)
            pbp_seasons = missing
        else:
            if cache_map:
                return _build_result(None)
            pbp_seasons = True

        player_id = player.profile.gsis_id
        if not player_id:
            return _build_result(season_list)

        try:
            pbp = player.fetch_pbp(seasons=pbp_seasons)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to fetch skill-impact play-by-play for %s: %s", identifier, exc)
            return _build_result(season_list)

        if pbp.is_empty() or "season" not in pbp.columns:
            return _build_result(season_list)

        filtered = pbp
        if "season_type" in filtered.columns:
            filtered = filtered.filter(pl.col("season_type").str.to_uppercase() == normalized_type)

        if filtered.is_empty():
            return _build_result(season_list)

        rusher_columns = (
            "rusher_player_id",
            "lateral_rusher_player_id",
        )
        receiver_columns = (
            "receiver_player_id",
            "lateral_receiver_player_id",
            "target_player_id",
            "targeted_player_id",
        )

        rusher_matchers = [pl.col(column) == player_id for column in rusher_columns if column in filtered.columns]
        receiver_matchers = [pl.col(column) == player_id for column in receiver_columns if column in filtered.columns]

        if not rusher_matchers and not receiver_matchers:
            return _build_result(season_list)

        working = filtered.with_columns(
            pl.any_horizontal(rusher_matchers).alias("_is_rusher") if rusher_matchers else pl.lit(False).alias("_is_rusher"),
            pl.any_horizontal(receiver_matchers).alias("_is_receiver") if receiver_matchers else pl.lit(False).alias("_is_receiver"),
        )

        impact_columns: list[pl.Expr] = []
        if "epa" in working.columns:
            impact_columns.append(
                pl.when(pl.col("_is_rusher") | pl.col("_is_receiver"))
                .then(pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0))
                .otherwise(0.0)
                .alias("_skill_epa")
            )
        else:
            impact_columns.append(pl.lit(0.0).alias("_skill_epa"))

        if "wpa" in working.columns:
            impact_columns.append(
                pl.when(pl.col("_is_rusher") | pl.col("_is_receiver"))
                .then(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0))
                .otherwise(0.0)
                .alias("_skill_wpa")
            )
        else:
            impact_columns.append(pl.lit(0.0).alias("_skill_wpa"))

        yards_col = "yards_gained" if "yards_gained" in working.columns else None
        complete_col = "complete_pass" if "complete_pass" in working.columns else None
        first_down_col = "first_down" if "first_down" in working.columns else None

        if yards_col:
            yards_expr = pl.col(yards_col).cast(pl.Float64, strict=False).fill_null(0.0)
            rush_condition = pl.col("_is_rusher") & (yards_expr >= 20)
            rec_condition = pl.col("_is_receiver") & (yards_expr >= 20)
            if complete_col in working.columns:
                rec_condition = rec_condition & (pl.col(complete_col) == 1)
            impact_columns.extend(
                [
                    pl.when(rush_condition).then(1).otherwise(0).alias("_rush_20_plus"),
                    pl.when(rec_condition).then(1).otherwise(0).alias("_rec_20_plus"),
                ]
            )
        else:
            impact_columns.extend([pl.lit(0).alias("_rush_20_plus"), pl.lit(0).alias("_rec_20_plus")])

        if first_down_col:
            impact_columns.append(
                pl.when(pl.col("_is_receiver") & (pl.col(first_down_col) == 1))
                .then(1)
                .otherwise(0)
                .alias("_rec_first_downs")
            )
        else:
            impact_columns.append(pl.lit(0).alias("_rec_first_downs"))

        working = working.with_columns(impact_columns)

        grouped = (
            working.group_by("season")
            .agg(
                pl.col("_skill_epa").sum().alias("_skill_epa_total"),
                pl.col("_skill_wpa").sum().alias("_skill_wpa_total"),
                pl.col("_rush_20_plus").sum().alias("_rush_20_plus"),
                pl.col("_rec_20_plus").sum().alias("_rec_20_plus"),
                pl.col("_rec_first_downs").sum().alias("_rec_first_downs"),
            )
            .sort("season")
        )

        for row in grouped.iter_rows(named=True):
            season_value = row.get("season")
            if season_value is None:
                continue
            cache_map[int(season_value)] = {
                "epa": float(row.get("_skill_epa_total") or 0.0),
                "wpa": float(row.get("_skill_wpa_total") or 0.0),
                "rush_20_plus": float(row.get("_rush_20_plus") or 0.0),
                "rec_20_plus": float(row.get("_rec_20_plus") or 0.0),
                "rec_first_downs": float(row.get("_rec_first_downs") or 0.0),
            }

        return _build_result(season_list)

    def get_defensive_player_impacts(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Aggregate defensive EPA/WPA for plays where the defender is credited with an action."""

        identifier = player.profile.gsis_id or player.profile.full_name
        normalized_type = season_type.upper()
        cache_key = (identifier, normalized_type)
        cache_map = self._defense_impact_cache.setdefault(cache_key, {})

        def _build_result(targets: list[int] | None) -> dict[int, dict[str, float]]:
            if targets:
                return {season: dict(cache_map[season]) for season in targets if season in cache_map}
            return {season: dict(values) for season, values in cache_map.items()}

        season_list: list[int] | None = None
        if seasons is not None:
            season_list = sorted({int(season) for season in seasons if season is not None})

        if season_list:
            missing = [season for season in season_list if season not in cache_map]
            if missing:
                repo_frame = self._load_cached_impacts(player, missing)
                repo_populated: set[int] = set()
                for row in repo_frame.iter_rows(named=True):
                    season_value = row.get("season")
                    if season_value is None:
                        continue
                    entry = {
                        "epa": float(row.get("def_epa") or 0.0),
                        "wpa": float(row.get("def_wpa") or 0.0),
                    }
                    if entry:
                        cache_map[int(season_value)] = entry
                        repo_populated.add(int(season_value))
                missing = [season for season in missing if season not in repo_populated]
            if not missing:
                return _build_result(season_list)
            pbp_seasons = missing
        else:
            if cache_map:
                return _build_result(None)
            pbp_seasons = True

        player_id = player.profile.gsis_id
        if not player_id:
            return _build_result(season_list)

        try:
            pbp = player.fetch_pbp(seasons=pbp_seasons)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to fetch defensive play-by-play for %s: %s", identifier, exc)
            return _build_result(season_list)

        if pbp.is_empty() or "season" not in pbp.columns:
            return _build_result(season_list)

        filtered = pbp
        if "season_type" in filtered.columns:
            filtered = filtered.filter(pl.col("season_type").str.to_uppercase() == normalized_type)

        if filtered.is_empty():
            return _build_result(season_list)

        defensive_columns = [
            "solo_tackle_1_player_id",
            "solo_tackle_2_player_id",
            "assist_tackle_1_player_id",
            "assist_tackle_2_player_id",
            "assist_tackle_3_player_id",
            "assist_tackle_4_player_id",
            "tackle_with_assist_1_player_id",
            "tackle_with_assist_2_player_id",
            "tackle_with_assist_3_player_id",
            "tackle_with_assist_4_player_id",
            "pass_defense_1_player_id",
            "pass_defense_2_player_id",
            "interception_player_id",
            "sack_player_id",
            "half_sack_1_player_id",
            "half_sack_2_player_id",
            "forced_fumble_player_1_player_id",
            "forced_fumble_player_2_player_id",
            "fumble_recovery_1_player_id",
            "fumble_recovery_2_player_id",
        ]

        involvement_exprs = [
            pl.col(column) == player_id for column in defensive_columns if column in filtered.columns
        ]
        if not involvement_exprs:
            return _build_result(season_list)

        working = filtered.with_columns(
            pl.any_horizontal(involvement_exprs).alias("_def_involved"),
        )

        impact_columns: list[pl.Expr] = []
        if "epa" in working.columns:
            impact_columns.append(
                pl.when(pl.col("_def_involved"))
                .then(pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0))
                .otherwise(0.0)
                .alias("_def_epa")
            )
        else:
            impact_columns.append(pl.lit(0.0).alias("_def_epa"))

        if "wpa" in working.columns:
            impact_columns.append(
                pl.when(pl.col("_def_involved"))
                .then(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0))
                .otherwise(0.0)
                .alias("_def_wpa")
            )
        else:
            impact_columns.append(pl.lit(0.0).alias("_def_wpa"))

        working = working.with_columns(impact_columns)

        grouped = (
            working.group_by("season")
            .agg(
                pl.col("_def_epa").sum().alias("_def_epa_total"),
                pl.col("_def_wpa").sum().alias("_def_wpa_total"),
            )
            .sort("season")
        )

        for row in grouped.iter_rows(named=True):
            season_value = row.get("season")
            if season_value is None:
                continue
            cache_map[int(season_value)] = {
                "epa": float(row.get("_def_epa_total") or 0.0),
                "wpa": float(row.get("_def_wpa_total") or 0.0),
            }

        return _build_result(season_list)

    def _collect_generic_impacts(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None,
        season_type: str,
        cache_store: dict[tuple[str, str], dict[int, dict[str, float]]],
        involvement_columns: Sequence[str],
        impact_prefix: str | None = None,
    ) -> dict[int, dict[str, float]]:
        identifier = player.profile.gsis_id or player.profile.full_name
        normalized_type = season_type.upper()
        cache_key = (identifier, normalized_type)
        cache_map = cache_store.setdefault(cache_key, {})

        def _build_result(targets: list[int] | None) -> dict[int, dict[str, float]]:
            if targets:
                return {season: dict(cache_map[season]) for season in targets if season in cache_map}
            return {season: dict(values) for season, values in cache_map.items()}

        season_list: list[int] | None = None
        if seasons is not None:
            season_list = sorted({int(season) for season in seasons if season is not None})

        if season_list:
            missing = [season for season in season_list if season not in cache_map]
            if missing and impact_prefix:
                repo_frame = self._load_cached_impacts(player, missing)
                repo_populated: set[int] = set()
                epa_column = f"{impact_prefix}_epa"
                wpa_column = f"{impact_prefix}_wpa"
                for row in repo_frame.iter_rows(named=True):
                    season_value = row.get("season")
                    if season_value is None:
                        continue
                    entry = {
                        "epa": float(row.get(epa_column) or 0.0),
                        "wpa": float(row.get(wpa_column) or 0.0),
                    }
                    if entry:
                        cache_map[int(season_value)] = entry
                        repo_populated.add(int(season_value))
                missing = [season for season in missing if season not in repo_populated]
            if not missing:
                return _build_result(season_list)
            pbp_seasons: bool | Iterable[int] = missing
        else:
            if cache_map:
                return _build_result(None)
            pbp_seasons = True

        player_id = player.profile.gsis_id
        if not player_id:
            return _build_result(season_list)

        try:
            pbp = player.fetch_pbp(seasons=pbp_seasons)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to fetch play-by-play for %s: %s", identifier, exc)
            return _build_result(season_list)

        if pbp.is_empty() or "season" not in pbp.columns:
            return _build_result(season_list)

        filtered = pbp
        if "season_type" in filtered.columns:
            filtered = filtered.filter(pl.col("season_type").str.to_uppercase() == normalized_type)

        if filtered.is_empty():
            return _build_result(season_list)

        involvement_exprs = [
            pl.col(column) == player_id for column in involvement_columns if column in filtered.columns
        ]

        if not involvement_exprs:
            return _build_result(season_list)

        working = filtered.with_columns(pl.any_horizontal(involvement_exprs).alias("_involved"))

        epa_expr = (
            pl.when(pl.col("_involved"))
            .then(pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0))
            .otherwise(0.0)
            .alias("_generic_epa")
            if "epa" in working.columns
            else pl.lit(0.0).alias("_generic_epa")
        )
        wpa_expr = (
            pl.when(pl.col("_involved"))
            .then(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0))
            .otherwise(0.0)
            .alias("_generic_wpa")
            if "wpa" in working.columns
            else pl.lit(0.0).alias("_generic_wpa")
        )

        working = working.with_columns([epa_expr, wpa_expr])

        grouped = (
            working.group_by("season")
            .agg(
                pl.col("_generic_epa").sum().alias("_generic_epa_total"),
                pl.col("_generic_wpa").sum().alias("_generic_wpa_total"),
            )
            .sort("season")
        )

        for row in grouped.iter_rows(named=True):
            season_value = row.get("season")
            if season_value is None:
                continue
            cache_map[int(season_value)] = {
                "epa": float(row.get("_generic_epa_total") or 0.0),
                "wpa": float(row.get("_generic_wpa_total") or 0.0),
            }

        return _build_result(season_list)

    def get_offensive_line_impacts(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Aggregate EPA/WPA for offensive linemen (primarily via penalties)."""

        involvement_columns = [
            "penalty_player_id",
            "penalty_player_id_1",
            "penalty_player_id_2",
        ]
        return self._collect_generic_impacts(
            player,
            seasons=seasons,
            season_type=season_type,
            cache_store=self._offensive_line_impact_cache,
            involvement_columns=involvement_columns,
            impact_prefix="ol",
        )

    def get_kicker_impacts(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Aggregate EPA/WPA for kickers (field goals, PATs, kickoffs)."""

        involvement_columns = [
            "kicker_player_id",
            "kickoff_player_id",
        ]
        return self._collect_generic_impacts(
            player,
            seasons=seasons,
            season_type=season_type,
            cache_store=self._kicker_impact_cache,
            involvement_columns=involvement_columns,
            impact_prefix="kicker",
        )

    def get_punter_impacts(
        self,
        player: Player,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Aggregate EPA/WPA for punters."""

        involvement_columns = [
            "punter_player_id",
        ]
        return self._collect_generic_impacts(
            player,
            seasons=seasons,
            season_type=season_type,
            cache_store=self._punter_impact_cache,
            involvement_columns=involvement_columns,
            impact_prefix="punter",
        )

    # --------------------------------------------------------------------- #
    # Rating helpers

    def get_basic_ratings(
        self,
        player: Player,
        *,
        summary: Mapping[str, float],
        is_defensive: bool,
    ) -> list[RatingBreakdown]:
        """Compute 20–80 style basic ratings for a player."""

        if not summary:
            return []

        position = (player.profile.position or player.profile.position_group or "").upper()
        rating_key = self._determine_rating_key(position, is_defensive)
        config = RATING_CONFIG.get(rating_key)
        if not config:
            fallback_key = "defense" if is_defensive else "default_offense"
            config = RATING_CONFIG[fallback_key]
            rating_key = fallback_key

        metric_keys = {
            metric_cfg["key"]  # type: ignore[index]
            for group_cfg in config
            for metric_cfg in group_cfg["metrics"]  # type: ignore[index]
        }
        baseline = self._get_rating_baseline(rating_key, metric_keys)

        ratings: list[RatingBreakdown] = []
        for group_cfg in config:
            group_label = str(group_cfg["label"])  # type: ignore[index]
            metric_cfgs: Sequence[Mapping[str, object]] = group_cfg["metrics"]  # type: ignore[index]
            subratings: list[RatingBreakdown] = []
            for metric_cfg in metric_cfgs:
                metric_key = str(metric_cfg["key"])
                metric_label = str(metric_cfg["label"])
                value = float(summary.get(metric_key, 0.0))
                mean_val, std_val = baseline.get(metric_key, (0.0, 1.0))
                score = self._calculate_rating_score(value, mean_val, std_val)
                potential = self._calculate_rating_potential(score)
                subratings.append(
                    RatingBreakdown(
                        label=metric_label,
                        current=score,
                        potential=potential,
                        subratings=(),
                    )
                )

            if not subratings:
                continue

            group_average = sum(sub.current for sub in subratings) / len(subratings)
            group_score = self._clamp_rating(group_average)
            group_potential = self._calculate_rating_potential(group_score)
            ratings.append(
                RatingBreakdown(
                    label=group_label,
                    current=group_score,
                    potential=group_potential,
                    subratings=tuple(subratings),
                )
            )

        return ratings

    def _compute_rating_seasons(self) -> list[int]:
        current_year = datetime.now().year
        start_year = max(1999, current_year - (_DEFAULT_RATING_SEASON_WINDOW - 1))
        return list(range(start_year, current_year + 1))

    def _determine_rating_key(self, position: str, is_defensive: bool) -> str:
        upper = position.upper() if position else ""
        if upper in RATING_CONFIG:
            return upper
        return "defense" if is_defensive else "default_offense"

    def _default_baseline(self, metrics: Iterable[str]) -> dict[str, tuple[float, float]]:
        return {metric: (0.0, 1.0) for metric in metrics}

    def _get_rating_baseline(
        self,
        key: str,
        metrics: Iterable[str],
    ) -> dict[str, tuple[float, float]]:
        metrics_tuple = tuple(metrics)
        cached = self._rating_baselines.get(key)
        if cached and all(metric in cached for metric in metrics_tuple):
            return cached

        baseline = self._build_rating_baseline(key, metrics_tuple)
        self._rating_baselines[key] = baseline
        return baseline

    def _build_rating_baseline(
        self,
        key: str,
        metrics: Sequence[str],
    ) -> dict[str, tuple[float, float]]:
        if load_player_stats is None:
            return self._default_baseline(metrics)

        positions = RATING_POSITION_GROUPS.get(key)
        if not positions:
            positions = _DEFENSE_POSITIONS if key == "defense" else _OFFENSE_POSITIONS

        try:
            stats = load_player_stats(seasons=self._rating_seasons)
        except Exception as exc:  # pragma: no cover - network or availability issues
            logger.debug("Failed to load rating baseline data (%s): %s", key, exc)
            return self._default_baseline(metrics)

        if stats.is_empty():
            return self._default_baseline(metrics)

        position_exprs = [pl.col(column) for column in ("player_position", "position") if column in stats.columns]
        if position_exprs:
            stats = stats.with_columns(
                pl.coalesce(position_exprs)
                .fill_null("")
                .str.to_uppercase()
                .alias("_pos"),
            )
        else:
            stats = stats.with_columns(pl.lit("").alias("_pos"))
        stats = stats.filter(pl.col("_pos").is_in(list(positions)))
        if stats.height == 0:
            return self._default_baseline(metrics)

        if "season_type" in stats.columns:
            stats = stats.filter(pl.col("season_type") == "REG")

        def sum_or_zero(column: str, alias: str) -> pl.Expr:
            if column in stats.columns:
                return pl.col(column).fill_null(0).sum().alias(alias)
            return pl.lit(0.0).alias(alias)

        aggregated = (
            stats.group_by("player_id")
            .agg(
                sum_or_zero("passing_yards", "_pass_yards"),
                sum_or_zero("rushing_yards", "_rush_yards"),
                sum_or_zero("receiving_yards", "_rec_yards"),
                sum_or_zero("passing_tds", "_passing_tds"),
                sum_or_zero("rushing_tds", "_rushing_tds"),
                sum_or_zero("receiving_tds", "_receiving_tds"),
                sum_or_zero("def_tackles_solo", "_def_solo"),
                sum_or_zero("def_tackle_assists", "_def_tackle_assists"),
                sum_or_zero("def_tackles_with_assist", "_def_tackles_with_assist"),
                sum_or_zero("def_interceptions", "_def_int"),
            )
            .fill_null(0)
        )

        aggregated = aggregated.with_columns(
            pl.col("_pass_yards").alias("pass_yards"),
            pl.col("_rush_yards").alias("rush_yards"),
            pl.col("_rec_yards").alias("receiving_yards"),
            (
                pl.col("_passing_tds")
                + pl.col("_rushing_tds")
                + pl.col("_receiving_tds")
            ).alias("total_touchdowns"),
            (
                pl.col("_def_solo")
                + pl.col("_def_tackle_assists")
                + pl.col("_def_tackles_with_assist")
            ).alias("def_tackles_total"),
            (
                pl.col("_def_tackle_assists")
                + pl.col("_def_tackles_with_assist")
            ).alias("def_tackles_assisted"),
            pl.col("_def_solo").alias("def_tackles_solo"),
            pl.col("_def_int").alias("def_interceptions"),
        )

        metrics_df = aggregated.select(
            [pl.col(metric) if metric in aggregated.columns else pl.lit(0.0).alias(metric) for metric in metrics]
        ).fill_null(0)

        baseline: dict[str, tuple[float, float]] = {}
        for metric in metrics:
            if metric not in metrics_df.columns:
                baseline[metric] = (0.0, 1.0)
                continue
            series = metrics_df[metric]
            if series.len() == 0:
                baseline[metric] = (0.0, 1.0)
                continue
            mean_val = float(series.mean())
            std_val = float(series.std())
            if not math.isfinite(std_val) or std_val <= 1e-6:
                std_val = max(abs(mean_val) * 0.25, 1.0)
            baseline[metric] = (mean_val, std_val)

        return baseline

    def _calculate_rating_score(self, value: float, mean: float, std: float) -> int:
        if not math.isfinite(std) or std <= 0:
            std = max(abs(mean) * 0.25, 1.0)
        z_score = (value - mean) / std if std else 0.0
        score = 50 + 10 * z_score
        return self._clamp_rating(score)

    def _calculate_rating_potential(self, score: int) -> int:
        return self._clamp_rating(score + 5)

    @staticmethod
    def _clamp_rating(value: float) -> int:
        rounded = int(round(value))
        bounded = max(20, min(80, rounded))
        remainder = bounded % 5
        if remainder:
            bounded = bounded - remainder if remainder < 3 else bounded + (5 - remainder)
        return max(20, min(80, bounded))
