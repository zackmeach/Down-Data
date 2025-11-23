"""Repository interface for the aggregated basic player season cache."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

import polars as pl

from down_data.data import basic_cache

logger = logging.getLogger(__name__)


class BasicPlayerStatsRepository:
    """Provide cached player-season stats covering offensive, defensive, and ST roles."""

    def __init__(self, *, auto_build: bool = True) -> None:
        self._auto_build = auto_build

    def ensure_cache(self, *, refresh: bool = False) -> None:
        """Ensure the aggregated cache exists."""

        if refresh:
            logger.debug("Refreshing basic player cache.")
            basic_cache.build_basic_cache(force_refresh=True)
            return

        if not basic_cache.cache_exists():
            if not self._auto_build:
                raise FileNotFoundError(f"Basic player cache missing at {basic_cache.CACHE_PATH}")
            logger.info("Basic player cache not found. Building now.")
            basic_cache.build_basic_cache(force_refresh=False)

    def scan(self, *, refresh: bool = False) -> pl.LazyFrame:
        """Return a lazy scanner over the cached player data."""

        self.ensure_cache(refresh=refresh)
        return basic_cache.scan_basic_cache()

    def load(self, *, refresh: bool = False) -> pl.DataFrame:
        """Load the entire player cache into memory."""

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
        """Return filtered player-season rows."""

        lf = self.scan(refresh=refresh)

        if player_ids:
            lf = lf.filter(pl.col("player_id").is_in([str(pid) for pid in player_ids]))

        if seasons is not None:
            season_list = list(seasons)
            if season_list:
                lf = lf.filter(pl.col("season").is_in(season_list))

        if team:
            lf = lf.filter(pl.col("team") == team.strip().upper())

        if position:
            lf = lf.filter(pl.col("position").str.to_uppercase() == position.strip().upper())

        return lf.collect()


__all__ = ["BasicPlayerStatsRepository"]

