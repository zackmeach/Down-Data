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
from .offense_stats_repository import BasicOffenseStatsRepository

try:  # pragma: no cover - defensive import
    from nflreadpy import load_players, load_player_stats, load_schedules
except ImportError:  # pragma: no cover - imported dynamically in some environments
    load_players = None  # type: ignore
    load_player_stats = None  # type: ignore
    load_schedules = None  # type: ignore

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
        self._offense_stats_repository = BasicOffenseStatsRepository()

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
