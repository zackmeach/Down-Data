"""Backend helpers that bridge the UI with the Player domain object."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
import logging
import re
from typing import Iterable, List, Optional

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
    team: Optional[str]
    position: Optional[str]

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
        self._frame: Optional[pl.DataFrame] = None

    @staticmethod
    def _to_polars(frame: object) -> pl.DataFrame:
        if isinstance(frame, pl.DataFrame):
            return frame
        try:
            import pandas as pd  # type: ignore
        except ImportError:  # pragma: no cover - optional dependency
            pd = None
        if pd is not None and isinstance(frame, pd.DataFrame):
            return pl.from_pandas(frame)
        raise TypeError("Unsupported frame type returned by nflreadpy.load_players")

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
        team: Optional[str] = None,
        position: Optional[str] = None,
        limit: int = 25,
    ) -> List[PlayerSummary]:
        frame = self.frame
        if frame.height == 0:
            return []

        filters: List[pl.Expr] = []
        if name:
            escaped = re.escape(name.strip())
            pattern = fr"(?i){escaped}"
            filters.append(
                pl.col("full_name").str.contains(pattern) | pl.col("display_name").str.contains(pattern)
            )
        if team:
            filters.append(pl.col("recent_team").str.to_uppercase() == team.strip().upper())
        if position:
            filters.append(pl.col("position").str.to_uppercase() == position.strip().upper())

        filtered = frame
        for expr in filters:
            filtered = filtered.filter(expr)
        trimmed = filtered.head(limit)

        results: List[PlayerSummary] = []
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

    def __init__(self, directory: Optional[PlayerDirectory] = None) -> None:
        self.directory = directory or PlayerDirectory()

    def get_all_players(self) -> pl.DataFrame:
        """Get the full player directory as a DataFrame for filtering."""
        return self.directory.frame

    def search_players(
        self,
        *,
        name: str,
        team: Optional[str] = None,
        position: Optional[str] = None,
        limit: int = 25,
    ) -> List[PlayerSummary]:
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
        except Exception as exc:
            logger.error("Unexpected error initialising Player: %%s", exc)
            raise

    def load_player_profile(self, query: PlayerQuery | PlayerSummary) -> PlayerProfile:
        """Convenience method to fetch only the profile information."""

        player = self.load_player(query)
        return player.profile

    def fetch_player_stats(
        self,
        query: PlayerQuery | PlayerSummary,
        *,
        seasons: Optional[Iterable[int]] = None,
        season_type: Optional[str] = None,
    ) -> pl.DataFrame:
        player = self.load_player(query)
        return player.fetch_stats(seasons=seasons, season_type=season_type)
