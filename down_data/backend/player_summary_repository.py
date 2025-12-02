"""Repository for the aggregated player summary cache."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

import polars as pl

from down_data.data import player_summary_cache

logger = logging.getLogger(__name__)


class PlayerSummaryRepository:
    """Expose cached season-level stats used by the UI summary tables."""

    def __init__(self, *, auto_build: bool = False) -> None:
        self._auto_build = auto_build

    def ensure_cache(self, *, refresh: bool = False) -> None:
        """Make sure the summary cache is available on disk."""

        if refresh:
            logger.info("Refreshing player summary cache.")
            player_summary_cache.build_player_summary_cache(force_refresh=True)
            return

        if player_summary_cache.cache_exists():
            return

        if not self._auto_build:
            raise FileNotFoundError(
                f"Player summary cache missing at {player_summary_cache.CACHE_PATH}. "
                "Run scripts/build_player_summary_cache.py to generate it."
            )

        logger.info("Player summary cache not found; building now.")
        player_summary_cache.build_player_summary_cache(force_refresh=False)

    def scan(self, *, refresh: bool = False) -> pl.LazyFrame:
        """Return a lazy scanner over the summary cache."""

        self.ensure_cache(refresh=refresh)
        return player_summary_cache.scan_player_summary_cache()

    def query(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Fetch cached rows filtered by player IDs and seasons."""

        lf = self.scan(refresh=refresh)

        if player_ids:
            lf = lf.filter(pl.col("player_id").is_in([str(pid) for pid in player_ids]))

        if seasons is not None:
            season_list = list(seasons)
            if season_list:
                lf = lf.filter(pl.col("season").is_in(season_list))

        return lf.collect()


__all__ = ["PlayerSummaryRepository"]


