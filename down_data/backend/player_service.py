"""Backend helpers that bridge the UI with the Player domain object."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
import logging
import re

import polars as pl

from down_data.core import Player, PlayerProfile, PlayerQuery, PlayerNotFoundError

try:  # pragma: no cover - defensive import
    from nflreadpy import load_players
except ImportError:  # pragma: no cover - imported dynamically in some environments
    load_players = None  # type: ignore

logger = logging.getLogger(__name__)


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

    def fetch_player_stats(
        self,
        query: PlayerQuery | PlayerSummary,
        *,
        seasons: Iterable[int] | None = None,
        season_type: str | None = None,
    ) -> pl.DataFrame:
        player = self.load_player(query)
        return player.fetch_stats(seasons=seasons, season_type=season_type)
