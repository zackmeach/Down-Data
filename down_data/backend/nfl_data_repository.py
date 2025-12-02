"""Repository for accessing the NFL Data Store.

This repository provides a clean interface for accessing the structured NFL data store,
replacing the legacy cache-based repositories with a unified data access layer.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Any

import polars as pl

from down_data.data.nfl_datastore import (
    NFLDataStore,
    NFLDataBuilder,
    get_default_store,
    PLAYERS_SCHEMA,
    PLAYER_SEASONS_SCHEMA,
    PLAYER_IMPACTS_SCHEMA,
)

logger = logging.getLogger(__name__)


class NFLDataRepository:
    """Unified repository for accessing NFL player data.
    
    This repository provides access to the structured NFL data store,
    which contains:
    - Player bio information (static/slowly-changing)
    - Player season statistics
    - Player impact metrics (EPA/WPA)
    
    It replaces the separate BasicPlayerStatsRepository, PlayerImpactRepository,
    and PlayerSummaryRepository with a single, unified interface.
    """
    
    def __init__(
        self,
        store: NFLDataStore | None = None,
        *,
        auto_initialize: bool = True,
    ) -> None:
        """Initialize the repository.
        
        Args:
            store: Optional NFLDataStore instance. Uses default if None.
            auto_initialize: If True, automatically initialize the store if needed.
        """
        self._store = store or get_default_store()
        self._auto_initialize = auto_initialize
        self._initialized = False
    
    def _ensure_initialized(self) -> None:
        """Ensure the data store is initialized."""
        if self._initialized:
            return
        
        if not self._store.players_path.exists():
            if self._auto_initialize:
                logger.info("NFL Data Store not found. Initializing...")
                self._store.initialize()
            else:
                raise FileNotFoundError(
                    f"NFL Data Store not initialized at {self._store.data_dir}. "
                    "Run scripts/build_nfl_datastore.py to create it."
                )
        
        self._initialized = True
    
    # -------------------------------------------------------------------------
    # Player Queries
    # -------------------------------------------------------------------------
    
    def get_player(self, player_id: str) -> dict[str, Any] | None:
        """Get static player information by ID.
        
        Returns bio info, physical attributes, draft info, etc.
        These are attributes that don't change year-to-year.
        """
        self._ensure_initialized()
        return self._store.get_player(player_id)
    
    def get_players(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        position: str | None = None,
    ) -> pl.DataFrame:
        """Query the players table.
        
        Args:
            player_ids: Filter to specific player IDs.
            position: Filter to specific position.
        
        Returns:
            DataFrame with player records.
        """
        self._ensure_initialized()
        return self._store.get_players(player_ids=player_ids, position=position)
    
    def search_players(
        self,
        *,
        name: str | None = None,
        team: str | None = None,
        position: str | None = None,
        limit: int = 50,
    ) -> pl.DataFrame:
        """Search players by name, team, or position.
        
        Args:
            name: Partial name match (case-insensitive).
            team: Team abbreviation filter.
            position: Position filter.
            limit: Maximum results to return.
        
        Returns:
            DataFrame with matching player records.
        """
        self._ensure_initialized()
        
        lf = self._store.scan_players()
        
        if name:
            # Case-insensitive partial match on full_name
            pattern = f"(?i){name}"
            lf = lf.filter(
                pl.col("full_name").str.contains(pattern) |
                pl.col("display_name").str.contains(pattern)
            )
        
        if team:
            # Need to join with player_seasons to get current team
            seasons = self._store.scan_player_seasons()
            latest_teams = (
                seasons.group_by("player_id")
                .agg(pl.col("team").last().alias("recent_team"))
            )
            lf = lf.join(latest_teams, on="player_id", how="left")
            lf = lf.filter(pl.col("recent_team").str.to_uppercase() == team.upper())
        
        if position:
            lf = lf.filter(pl.col("position").str.to_uppercase() == position.upper())
        
        return lf.head(limit).collect()
    
    # -------------------------------------------------------------------------
    # Player Season Queries
    # -------------------------------------------------------------------------
    
    def get_player_seasons(
        self,
        player_id: str | None = None,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
    ) -> pl.DataFrame:
        """Query player-season statistics.
        
        Args:
            player_id: Single player ID to filter.
            player_ids: Multiple player IDs to filter.
            seasons: Season years to include.
            team: Team abbreviation filter.
            position: Position filter.
        
        Returns:
            DataFrame with player-season statistics.
        """
        self._ensure_initialized()
        return self._store.get_player_seasons(
            player_id,
            player_ids=player_ids,
            seasons=seasons,
            team=team,
            position=position,
        )
    
    def get_career_stats(self, player_id: str) -> dict[str, Any]:
        """Get career totals for a player.
        
        Returns aggregated career statistics across all seasons.
        """
        self._ensure_initialized()
        
        seasons = self._store.get_player_seasons(player_id)
        if seasons.height == 0:
            return {}
        
        # Sum numeric columns
        numeric_cols = [
            "games_played", "offense_snaps", "defense_snaps", "special_teams_snaps",
            "snaps_total", "pass_completions", "pass_attempts", "passing_yards",
            "passing_tds", "passing_ints", "rushing_attempts", "rushing_yards",
            "rushing_tds", "receiving_targets", "receiving_receptions", "receiving_yards",
            "receiving_tds", "total_touchdowns", "def_tackles_solo", "def_tackle_assists",
            "def_sacks", "def_interceptions", "def_tds",
        ]
        
        totals = {}
        for col in numeric_cols:
            if col in seasons.columns:
                value = seasons[col].fill_null(0).sum()
                if value is not None and value > 0:
                    totals[col] = int(value) if isinstance(value, (int, float)) else value
        
        # Calculate years of service
        totals["years_of_service"] = seasons["season"].n_unique()
        
        return totals
    
    # -------------------------------------------------------------------------
    # Player Impact Queries
    # -------------------------------------------------------------------------
    
    def get_player_impacts(
        self,
        player_id: str | None = None,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
    ) -> pl.DataFrame:
        """Query player impact metrics (EPA/WPA).
        
        Args:
            player_id: Single player ID to filter.
            player_ids: Multiple player IDs to filter.
            seasons: Season years to include.
        
        Returns:
            DataFrame with player impact metrics.
        """
        self._ensure_initialized()
        return self._store.get_player_impacts(
            player_id,
            player_ids=player_ids,
            seasons=seasons,
        )
    
    # -------------------------------------------------------------------------
    # Combined Queries
    # -------------------------------------------------------------------------
    
    def get_player_summary(
        self,
        player_id: str,
        *,
        seasons: Iterable[int] | None = None,
    ) -> pl.DataFrame:
        """Get complete player data: bio + seasons + impacts.
        
        This is the primary method for getting all data needed for the UI.
        
        Args:
            player_id: Player ID to query.
            seasons: Optional season filter.
        
        Returns:
            DataFrame with all player data joined together.
        """
        self._ensure_initialized()
        return self._store.get_player_summary(player_id, seasons=seasons)
    
    def get_summary_stats(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
    ) -> pl.DataFrame:
        """Get summary stats suitable for the UI summary tables.
        
        This replaces PlayerSummaryRepository.query() with optimized
        access to the unified data store.
        """
        self._ensure_initialized()
        
        # Get player seasons
        player_seasons = self._store.get_player_seasons(
            player_ids=player_ids,
            seasons=seasons,
            team=team,
            position=position,
        )
        
        if player_seasons.height == 0:
            return player_seasons
        
        # Get player info
        unique_ids = player_seasons["player_id"].unique().to_list()
        players = self._store.get_players(player_ids=unique_ids)
        
        # Get impacts
        impacts = self._store.get_player_impacts(player_ids=unique_ids, seasons=seasons)
        
        # Join player info
        result = player_seasons.join(
            players.select([
                "player_id", "full_name", "birth_date", "birth_city", "birth_state",
                "birth_country", "college", "handedness", "height", "weight",
            ]),
            on="player_id",
            how="left",
            suffix="_player",
        )
        
        # Handle duplicate column from join
        if "full_name_player" in result.columns and "full_name" not in result.columns:
            result = result.rename({"full_name_player": "full_name"})
        
        # Add player_name alias for compatibility
        if "full_name" in result.columns and "player_name" not in result.columns:
            result = result.with_columns(pl.col("full_name").alias("player_name"))
        
        # Join impacts
        if impacts.height > 0:
            impact_cols = [c for c in impacts.columns if c not in {"player_id", "season", "_last_updated"}]
            if impact_cols:
                result = result.join(
                    impacts.select(["player_id", "season"] + impact_cols),
                    on=["player_id", "season"],
                    how="left",
                )
        
        return result.sort(["player_id", "season"])
    
    # -------------------------------------------------------------------------
    # Bio Data
    # -------------------------------------------------------------------------
    
    def get_player_bio(self, player_id: str) -> dict[str, str]:
        """Get bio details for a player (birthplace, handedness).
        
        Returns the bio fields that are fetched from PFR.
        """
        self._ensure_initialized()
        
        player = self._store.get_player(player_id)
        if player is None:
            return {}
        
        return {
            "handedness": player.get("handedness") or "N/A",
            "birth_city": player.get("birth_city") or "N/A",
            "birth_state": player.get("birth_state") or "N/A",
            "birth_country": player.get("birth_country") or "N/A",
        }
    
    def update_player_bio(
        self,
        player_id: str,
        bio_fields: dict[str, str],
    ) -> bool:
        """Update bio fields for a player.
        
        This allows incremental updates of bio data without rebuilding.
        """
        self._ensure_initialized()
        return self._store.update_player_bio(player_id, bio_fields)
    
    # -------------------------------------------------------------------------
    # Refresh Operations
    # -------------------------------------------------------------------------
    
    def refresh(
        self,
        *,
        seasons: Iterable[int] | None = None,
        force: bool = False,
        skip_bio: bool = False,
        skip_impacts: bool = False,
    ) -> dict[str, Any]:
        """Refresh data in the store.
        
        Args:
            seasons: Specific seasons to refresh. None means all.
            force: Force rebuild even if data exists.
            skip_bio: Skip PFR bio scraping.
            skip_impacts: Skip EPA/WPA calculation.
        
        Returns:
            Statistics about the refresh operation.
        """
        builder = NFLDataBuilder(self._store)
        return builder.build_all(
            seasons=seasons,
            force=force,
            skip_bio=skip_bio,
            skip_impacts=skip_impacts,
        )
    
    def get_status(self) -> dict[str, Any]:
        """Get current status of the data store."""
        return self._store.get_status()
    
    # -------------------------------------------------------------------------
    # Compatibility Methods (for existing code)
    # -------------------------------------------------------------------------
    
    def query(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
        refresh: bool = False,
    ) -> pl.DataFrame:
        """Query method for backward compatibility with legacy repositories.
        
        This method provides the same interface as BasicPlayerStatsRepository
        and PlayerSummaryRepository for easier migration.
        """
        if refresh:
            self.refresh(seasons=seasons, skip_bio=True, skip_impacts=True)
        
        return self.get_summary_stats(
            player_ids=player_ids,
            seasons=seasons,
            team=team,
            position=position,
        )
    
    def scan(self, *, refresh: bool = False) -> pl.LazyFrame:
        """Return a lazy scanner for backward compatibility."""
        self._ensure_initialized()
        if refresh:
            self.refresh(skip_bio=True, skip_impacts=True)
        return self._store.scan_player_seasons()


# Module-level singleton for convenience
_default_repository: NFLDataRepository | None = None


def get_repository() -> NFLDataRepository:
    """Get the default repository instance."""
    global _default_repository
    if _default_repository is None:
        _default_repository = NFLDataRepository()
    return _default_repository


__all__ = [
    "NFLDataRepository",
    "get_repository",
]

