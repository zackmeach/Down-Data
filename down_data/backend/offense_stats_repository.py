"""Repository for cached offensive player-season statistics."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

import polars as pl

from down_data.data import basic_offense

logger = logging.getLogger(__name__)


class BasicOffenseStatsRepository:
    """Provide cached offensive stats with optional automatic refresh."""

    def __init__(self, *, auto_build: bool = True) -> None:
        self._auto_build = auto_build

    def ensure_cache(self, *, refresh: bool = False) -> None:
        """Make sure the cache exists on disk."""

        if refresh:
            logger.debug("Refreshing basic offense cache.")
            basic_offense.build_basic_offense_cache(force_refresh=True)
            return

        if not basic_offense.cache_exists():
            if not self._auto_build:
                raise FileNotFoundError(
                    f"Basic offense cache missing at {basic_offense.CACHE_PATH}"
                )
            logger.info("Basic offense cache not found. Building now.")
            basic_offense.build_basic_offense_cache(force_refresh=False)

    def scan(self, *, refresh: bool = False) -> pl.LazyFrame:
        """Return a lazy scanner over the cached data."""

        self.ensure_cache(refresh=refresh)
        return basic_offense.scan_basic_offense_stats()

    def load(self, *, refresh: bool = False) -> pl.DataFrame:
        """Load the entire cache into memory."""

        return self.scan(refresh=refresh).collect()

    def query(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return filtered stats for the requested criteria."""

        lf = self.scan(refresh=refresh)

        if player_ids:
            lf = lf.filter(pl.col("player_id").is_in(list(player_ids)))

        if seasons is not None:
            season_list = list(seasons)
            if season_list:
                lf = lf.filter(pl.col("season").is_in(season_list))

        if team:
            lf = lf.filter(pl.col("team") == team.strip().upper())

        if position:
            lf = lf.filter(pl.col("position") == position.strip().upper())

        return lf.collect()

