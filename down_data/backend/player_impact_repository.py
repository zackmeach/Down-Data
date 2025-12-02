"""Repository for the pre-computed player impact cache."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

import polars as pl

from down_data.data import player_impacts

logger = logging.getLogger(__name__)


class PlayerImpactRepository:
    """Expose the seasonal EPA/WPA cache with optional automatic builds."""

    def __init__(self, *, auto_build: bool = False) -> None:
        self._auto_build = auto_build

    def ensure_cache(self, *, refresh: bool = False) -> None:
        """Ensure the parquet cache exists, building it when requested."""

        if refresh:
            logger.info("Refreshing player impact cache.")
            player_impacts.build_player_impacts_cache(force_refresh=True)
            return

        if player_impacts.cache_exists():
            return

        if not self._auto_build:
            raise FileNotFoundError(
                f"Player impact cache missing at {player_impacts.CACHE_PATH}. "
                "Run scripts/build_player_impacts.py to generate it."
            )

        logger.info("Player impact cache not found; building now.")
        player_impacts.build_player_impacts_cache(force_refresh=False)

    def scan(self, *, refresh: bool = False) -> pl.LazyFrame:
        """Return a lazy scanner over the cached impacts."""

        self.ensure_cache(refresh=refresh)
        return player_impacts.scan_player_impacts()

    def query(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Return filtered impact rows."""

        lf = self.scan(refresh=refresh)

        if player_ids:
            lf = lf.filter(pl.col("player_id").is_in([str(pid) for pid in player_ids]))

        if seasons is not None:
            season_list = list(seasons)
            if season_list:
                lf = lf.filter(pl.col("season").is_in(season_list))

        return lf.collect()


__all__ = ["PlayerImpactRepository"]


