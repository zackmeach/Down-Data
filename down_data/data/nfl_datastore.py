"""NFL Data Store - A robust, structured database system for NFL player data.

This module provides a persistent, well-organized local database using Parquet files.
Unlike a cache, this data store is designed to be:
- Persistent: Data is intentionally stored and managed, not temporary
- Structured: Well-defined schemas with known features and date ranges
- Updateable: Can expand date ranges, add/remove features
- Refreshable: Any subset of data can be updated independently
- Efficient: Avoids redundant data fetching (e.g., birthplace doesn't change yearly)
- Error-aware: Graceful error handling with flagging for review

Data Tables:
- players: Static/slowly-changing player attributes (bio, birthplace, college, etc.)
- player_seasons: Season-level statistics (games, snaps, stats)
- player_impacts: EPA/WPA metrics by player-season
- metadata.json: Schema version, date ranges, update timestamps, error log
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import polars as pl
from requests import HTTPError

logger = logging.getLogger(__name__)

# ============================================================================
# Constants and Paths
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIRECTORY = PROJECT_ROOT / "data" / "nflverse"

PLAYERS_PATH = DATA_DIRECTORY / "players.parquet"
PLAYER_SEASONS_PATH = DATA_DIRECTORY / "player_seasons.parquet"
PLAYER_IMPACTS_PATH = DATA_DIRECTORY / "player_impacts.parquet"
METADATA_PATH = DATA_DIRECTORY / "metadata.json"

# Data availability constants
DEFAULT_SEASON_START = 1999
DEFAULT_SEASON_END = 2024
PFR_SNAP_MIN_SEASON = 2012

SCHEMA_VERSION = "1.0.0"

# ============================================================================
# Schema Definitions
# ============================================================================

PLAYERS_SCHEMA: dict[str, pl.DataType] = {
    # Identifiers
    "player_id": pl.Utf8,  # gsis_id - primary key
    "pfr_id": pl.Utf8,
    "pff_id": pl.Utf8,
    "espn_id": pl.Utf8,
    "sportradar_id": pl.Utf8,
    "esb_id": pl.Utf8,
    "otc_id": pl.Utf8,
    # Names
    "full_name": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "display_name": pl.Utf8,
    # Bio - static attributes (won't change)
    "birth_date": pl.Date,
    "birth_city": pl.Utf8,
    "birth_state": pl.Utf8,
    "birth_country": pl.Utf8,
    "college": pl.Utf8,
    "handedness": pl.Utf8,
    # Physical - relatively static
    "height": pl.Int16,
    "weight": pl.Int16,
    # Draft info - static
    "draft_year": pl.Int16,
    "draft_round": pl.Int8,
    "draft_pick": pl.Int16,
    "draft_team": pl.Utf8,
    # Position info - can change but rarely
    "position": pl.Utf8,
    "position_group": pl.Utf8,
    # Metadata
    "_last_updated": pl.Datetime,
    "_bio_fetched": pl.Boolean,  # Whether PFR bio was fetched
}

PLAYER_SEASONS_SCHEMA: dict[str, pl.DataType] = {
    # Keys
    "player_id": pl.Utf8,
    "season": pl.Int16,
    # Team/Position for season
    "team": pl.Utf8,
    "position": pl.Utf8,
    "position_group": pl.Utf8,
    # Activity
    "games_played": pl.Int16,
    # Snap counts
    "offense_snaps": pl.Int32,
    "offense_snaps_available": pl.Int32,
    "defense_snaps": pl.Int32,
    "special_teams_snaps": pl.Int32,
    "snaps_total": pl.Int32,
    # Passing stats
    "pass_completions": pl.Int16,
    "pass_attempts": pl.Int16,
    "passing_yards": pl.Int32,
    "passing_tds": pl.Int8,
    "passing_ints": pl.Int8,
    "sacks_taken": pl.Int8,
    "sack_yards": pl.Int16,
    # Rushing stats
    "rushing_attempts": pl.Int16,
    "rushing_yards": pl.Int32,
    "rushing_tds": pl.Int8,
    # Receiving stats
    "receiving_targets": pl.Int16,
    "receiving_receptions": pl.Int16,
    "receiving_yards": pl.Int32,
    "receiving_tds": pl.Int8,
    # Fumble stats
    "total_fumbles": pl.Int8,
    "fumbles_lost": pl.Int8,
    "rushing_fumbles": pl.Int8,
    "rushing_fumbles_lost": pl.Int8,
    "receiving_fumbles": pl.Int8,
    "receiving_fumbles_lost": pl.Int8,
    "sack_fumbles": pl.Int8,
    "sack_fumbles_lost": pl.Int8,
    # Defensive stats
    "def_tackles_solo": pl.Int16,
    "def_tackle_assists": pl.Int16,
    "def_tackles_for_loss": pl.Int8,
    "def_sacks": pl.Float32,
    "def_qb_hits": pl.Int8,
    "def_forced_fumbles": pl.Int8,
    "def_fumble_recoveries": pl.Int8,
    "def_safeties": pl.Int8,
    "def_pass_defended": pl.Int8,
    "def_interceptions": pl.Int8,
    "def_tds": pl.Int8,
    # Penalties
    "penalties": pl.Int8,
    "penalties_declined": pl.Int8,
    "penalties_offsetting": pl.Int8,
    "penalties_holding": pl.Int8,
    "penalties_false_start": pl.Int8,
    # Kicking stats
    "fgm": pl.Int8,
    "fga": pl.Int8,
    "fg_long": pl.Int8,
    "fgm_0_19": pl.Int8,
    "fga_0_19": pl.Int8,
    "fgm_20_29": pl.Int8,
    "fga_20_29": pl.Int8,
    "fgm_30_39": pl.Int8,
    "fga_30_39": pl.Int8,
    "fgm_40_49": pl.Int8,
    "fga_40_49": pl.Int8,
    "fgm_50_59": pl.Int8,
    "fga_50_59": pl.Int8,
    "fgm_60_plus": pl.Int8,
    "fga_60_plus": pl.Int8,
    "xpm": pl.Int8,
    "xpa": pl.Int8,
    "kickoffs": pl.Int16,
    "kickoff_touchbacks": pl.Int8,
    # Punting stats
    "punts": pl.Int16,
    "punt_yards": pl.Int32,
    "punt_long": pl.Int8,
    "punt_return_yards_allowed": pl.Int16,
    "net_punt_yards": pl.Int32,
    "punt_touchbacks": pl.Int8,
    "punts_inside_20": pl.Int8,
    "punts_blocked": pl.Int8,
    # Derived
    "total_touchdowns": pl.Int8,
    # Metadata
    "_last_updated": pl.Datetime,
}

PLAYER_IMPACTS_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Utf8,
    "season": pl.Int16,
    # QB metrics
    "qb_epa": pl.Float64,
    "qb_wpa": pl.Float64,
    # Skill player metrics
    "skill_epa": pl.Float64,
    "skill_wpa": pl.Float64,
    "skill_rush_20_plus": pl.Int32,
    "skill_rec_20_plus": pl.Int32,
    "skill_rec_first_downs": pl.Int32,
    # Defensive metrics
    "def_epa": pl.Float64,
    "def_wpa": pl.Float64,
    # Offensive line metrics
    "ol_epa": pl.Float64,
    "ol_wpa": pl.Float64,
    # Kicker metrics
    "kicker_epa": pl.Float64,
    "kicker_wpa": pl.Float64,
    # Punter metrics
    "punter_epa": pl.Float64,
    "punter_wpa": pl.Float64,
    # Metadata
    "_last_updated": pl.Datetime,
}


# ============================================================================
# Metadata and Error Tracking
# ============================================================================

@dataclass
class RefreshError:
    """Record of an error that occurred during data refresh."""
    timestamp: str
    operation: str
    entity: str  # e.g., player_id, team, season
    error_type: str
    message: str
    resolved: bool = False


@dataclass
class DataStoreMetadata:
    """Metadata tracking for the NFL data store."""
    schema_version: str = SCHEMA_VERSION
    season_start: int = DEFAULT_SEASON_START
    season_end: int = DEFAULT_SEASON_END
    players_last_updated: str | None = None
    player_seasons_last_updated: str | None = None
    player_impacts_last_updated: str | None = None
    total_players: int = 0
    total_player_seasons: int = 0
    total_impacts: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataStoreMetadata":
        errors = data.pop("errors", [])
        instance = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        instance.errors = errors
        return instance
    
    def add_error(self, operation: str, entity: str, error_type: str, message: str) -> None:
        error = RefreshError(
            timestamp=datetime.now().isoformat(),
            operation=operation,
            entity=entity,
            error_type=error_type,
            message=message,
        )
        self.errors.append(asdict(error))
    
    def clear_resolved_errors(self) -> int:
        """Remove resolved errors and return count removed."""
        original_count = len(self.errors)
        self.errors = [e for e in self.errors if not e.get("resolved", False)]
        return original_count - len(self.errors)
    
    def get_unresolved_errors(self) -> list[dict[str, Any]]:
        return [e for e in self.errors if not e.get("resolved", False)]


# ============================================================================
# Helper Functions
# ============================================================================

def _empty_players_frame() -> pl.DataFrame:
    """Return an empty players DataFrame with the correct schema."""
    return pl.DataFrame(schema=PLAYERS_SCHEMA)


def _empty_player_seasons_frame() -> pl.DataFrame:
    """Return an empty player_seasons DataFrame with the correct schema."""
    return pl.DataFrame(schema=PLAYER_SEASONS_SCHEMA)


def _empty_player_impacts_frame() -> pl.DataFrame:
    """Return an empty player_impacts DataFrame with the correct schema."""
    return pl.DataFrame(schema=PLAYER_IMPACTS_SCHEMA)


def _to_polars(frame: object) -> pl.DataFrame:
    """Convert various frame types to Polars DataFrame."""
    if isinstance(frame, pl.DataFrame):
        return frame
    try:
        return pl.DataFrame(frame)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"Unsupported frame type: {type(frame)!r}") from exc


def _safe_cast(frame: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Safely cast DataFrame columns to match schema."""
    cast_exprs = []
    for col, dtype in schema.items():
        if col not in frame.columns:
            continue
        try:
            cast_exprs.append(pl.col(col).cast(dtype, strict=False))
        except Exception:
            pass
    if cast_exprs:
        frame = frame.with_columns(cast_exprs)
    return frame


# ============================================================================
# NFL Data Store
# ============================================================================

class NFLDataStore:
    """Central data store for NFL player data.
    
    This class manages persistent parquet-based storage of NFL player data,
    providing efficient access and update mechanisms.
    
    Usage:
        store = NFLDataStore()
        store.initialize()  # First-time setup
        store.refresh(seasons=[2024])  # Update with new season
        
        # Query data
        player = store.get_player("00-0033873")
        seasons = store.get_player_seasons("00-0033873", seasons=[2023, 2024])
    """
    
    def __init__(self, data_dir: Path | None = None) -> None:
        self._data_dir = data_dir or DATA_DIRECTORY
        self._metadata: DataStoreMetadata | None = None
        self._players_cache: pl.DataFrame | None = None
        self._seasons_cache: pl.DataFrame | None = None
        self._impacts_cache: pl.DataFrame | None = None
    
    @property
    def data_dir(self) -> Path:
        return self._data_dir
    
    @property
    def players_path(self) -> Path:
        return self._data_dir / "players.parquet"
    
    @property
    def player_seasons_path(self) -> Path:
        return self._data_dir / "player_seasons.parquet"
    
    @property
    def player_impacts_path(self) -> Path:
        return self._data_dir / "player_impacts.parquet"
    
    @property
    def metadata_path(self) -> Path:
        return self._data_dir / "metadata.json"
    
    # -------------------------------------------------------------------------
    # Initialization and Metadata
    # -------------------------------------------------------------------------
    
    def initialize(self, *, force: bool = False) -> None:
        """Initialize the data store directory and files.
        
        Args:
            force: If True, reinitialize even if files exist.
        """
        self._data_dir.mkdir(parents=True, exist_ok=True)
        
        if force or not self.metadata_path.exists():
            self._metadata = DataStoreMetadata()
            self._save_metadata()
        
        if force or not self.players_path.exists():
            _empty_players_frame().write_parquet(self.players_path, compression="zstd")
        
        if force or not self.player_seasons_path.exists():
            _empty_player_seasons_frame().write_parquet(self.player_seasons_path, compression="zstd")
        
        if force or not self.player_impacts_path.exists():
            _empty_player_impacts_frame().write_parquet(self.player_impacts_path, compression="zstd")
        
        logger.info("NFL Data Store initialized at %s", self._data_dir)
    
    def load_metadata(self) -> DataStoreMetadata:
        """Load metadata from disk."""
        if self._metadata is not None:
            return self._metadata
        
        if not self.metadata_path.exists():
            self._metadata = DataStoreMetadata()
            return self._metadata
        
        try:
            with open(self.metadata_path, "r") as f:
                data = json.load(f)
            self._metadata = DataStoreMetadata.from_dict(data)
        except Exception as exc:
            logger.warning("Failed to load metadata: %s", exc)
            self._metadata = DataStoreMetadata()
        
        return self._metadata
    
    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        if self._metadata is None:
            return
        with open(self.metadata_path, "w") as f:
            json.dump(self._metadata.to_dict(), f, indent=2)
    
    def get_status(self) -> dict[str, Any]:
        """Return current status of the data store."""
        metadata = self.load_metadata()
        return {
            "initialized": self.players_path.exists(),
            "schema_version": metadata.schema_version,
            "season_range": f"{metadata.season_start}-{metadata.season_end}",
            "total_players": metadata.total_players,
            "total_player_seasons": metadata.total_player_seasons,
            "total_impacts": metadata.total_impacts,
            "unresolved_errors": len(metadata.get_unresolved_errors()),
            "last_updated": {
                "players": metadata.players_last_updated,
                "player_seasons": metadata.player_seasons_last_updated,
                "player_impacts": metadata.player_impacts_last_updated,
            },
        }
    
    # -------------------------------------------------------------------------
    # Table Loading (with caching)
    # -------------------------------------------------------------------------
    
    def load_players(self, *, refresh: bool = False) -> pl.DataFrame:
        """Load the players table."""
        if self._players_cache is not None and not refresh:
            return self._players_cache
        
        if not self.players_path.exists():
            return _empty_players_frame()
        
        self._players_cache = pl.read_parquet(self.players_path)
        return self._players_cache
    
    def scan_players(self) -> pl.LazyFrame:
        """Return a lazy scanner over the players table."""
        if not self.players_path.exists():
            return _empty_players_frame().lazy()
        return pl.scan_parquet(self.players_path)
    
    def load_player_seasons(self, *, refresh: bool = False) -> pl.DataFrame:
        """Load the player_seasons table."""
        if self._seasons_cache is not None and not refresh:
            return self._seasons_cache
        
        if not self.player_seasons_path.exists():
            return _empty_player_seasons_frame()
        
        self._seasons_cache = pl.read_parquet(self.player_seasons_path)
        return self._seasons_cache
    
    def scan_player_seasons(self) -> pl.LazyFrame:
        """Return a lazy scanner over the player_seasons table."""
        if not self.player_seasons_path.exists():
            return _empty_player_seasons_frame().lazy()
        return pl.scan_parquet(self.player_seasons_path)
    
    def load_player_impacts(self, *, refresh: bool = False) -> pl.DataFrame:
        """Load the player_impacts table."""
        if self._impacts_cache is not None and not refresh:
            return self._impacts_cache
        
        if not self.player_impacts_path.exists():
            return _empty_player_impacts_frame()
        
        self._impacts_cache = pl.read_parquet(self.player_impacts_path)
        return self._impacts_cache
    
    def scan_player_impacts(self) -> pl.LazyFrame:
        """Return a lazy scanner over the player_impacts table."""
        if not self.player_impacts_path.exists():
            return _empty_player_impacts_frame().lazy()
        return pl.scan_parquet(self.player_impacts_path)
    
    def _invalidate_cache(self, tables: Iterable[str] | None = None) -> None:
        """Invalidate cached data."""
        if tables is None:
            self._players_cache = None
            self._seasons_cache = None
            self._impacts_cache = None
            return
        
        for table in tables:
            if table == "players":
                self._players_cache = None
            elif table == "player_seasons":
                self._seasons_cache = None
            elif table == "player_impacts":
                self._impacts_cache = None
    
    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------
    
    def get_player(self, player_id: str) -> dict[str, Any] | None:
        """Get a single player's static info by ID."""
        players = self.load_players()
        match = players.filter(pl.col("player_id") == player_id)
        if match.height == 0:
            return None
        return match.row(0, named=True)
    
    def get_players(
        self,
        *,
        player_ids: Sequence[str] | None = None,
        position: str | None = None,
    ) -> pl.DataFrame:
        """Query players table with optional filters."""
        lf = self.scan_players()
        
        if player_ids:
            lf = lf.filter(pl.col("player_id").is_in(list(player_ids)))
        
        if position:
            lf = lf.filter(pl.col("position").str.to_uppercase() == position.upper())
        
        return lf.collect()
    
    def get_player_seasons(
        self,
        player_id: str | None = None,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
        team: str | None = None,
        position: str | None = None,
    ) -> pl.DataFrame:
        """Query player_seasons table with optional filters."""
        lf = self.scan_player_seasons()
        
        if player_id:
            lf = lf.filter(pl.col("player_id") == player_id)
        elif player_ids:
            lf = lf.filter(pl.col("player_id").is_in(list(player_ids)))
        
        if seasons is not None:
            season_list = list(seasons)
            if season_list:
                lf = lf.filter(pl.col("season").is_in(season_list))
        
        if team:
            lf = lf.filter(pl.col("team").str.to_uppercase() == team.upper())
        
        if position:
            lf = lf.filter(pl.col("position").str.to_uppercase() == position.upper())
        
        return lf.collect()
    
    def get_player_impacts(
        self,
        player_id: str | None = None,
        *,
        player_ids: Sequence[str] | None = None,
        seasons: Iterable[int] | None = None,
    ) -> pl.DataFrame:
        """Query player_impacts table with optional filters."""
        lf = self.scan_player_impacts()
        
        if player_id:
            lf = lf.filter(pl.col("player_id") == player_id)
        elif player_ids:
            lf = lf.filter(pl.col("player_id").is_in(list(player_ids)))
        
        if seasons is not None:
            season_list = list(seasons)
            if season_list:
                lf = lf.filter(pl.col("season").is_in(season_list))
        
        return lf.collect()
    
    def get_player_summary(
        self,
        player_id: str,
        *,
        seasons: Iterable[int] | None = None,
    ) -> pl.DataFrame:
        """Get combined player info, seasons, and impacts for a single player."""
        players = self.get_players(player_ids=[player_id])
        if players.height == 0:
            return pl.DataFrame()
        
        player_seasons = self.get_player_seasons(player_id, seasons=seasons)
        if player_seasons.height == 0:
            return players
        
        impacts = self.get_player_impacts(player_id, seasons=seasons)
        
        # Join player info with seasons
        combined = player_seasons.join(
            players.select([
                "player_id", "full_name", "birth_date", "birth_city", "birth_state",
                "birth_country", "college", "handedness", "height", "weight",
                "draft_year", "draft_round", "draft_pick", "draft_team"
            ]),
            on="player_id",
            how="left",
        )
        
        # Join with impacts
        if impacts.height > 0:
            impact_cols = [c for c in impacts.columns if c not in {"player_id", "season", "_last_updated"}]
            combined = combined.join(
                impacts.select(["player_id", "season"] + impact_cols),
                on=["player_id", "season"],
                how="left",
            )
        
        return combined.sort("season")
    
    # -------------------------------------------------------------------------
    # Save Methods
    # -------------------------------------------------------------------------
    
    def _save_players(self, frame: pl.DataFrame) -> None:
        """Save players table to disk."""
        frame.write_parquet(self.players_path, compression="zstd")
        self._players_cache = frame
        
        metadata = self.load_metadata()
        metadata.players_last_updated = datetime.now().isoformat()
        metadata.total_players = frame.height
        self._save_metadata()
    
    def _save_player_seasons(self, frame: pl.DataFrame) -> None:
        """Save player_seasons table to disk."""
        frame.write_parquet(self.player_seasons_path, compression="zstd")
        self._seasons_cache = frame
        
        metadata = self.load_metadata()
        metadata.player_seasons_last_updated = datetime.now().isoformat()
        metadata.total_player_seasons = frame.height
        self._save_metadata()
    
    def _save_player_impacts(self, frame: pl.DataFrame) -> None:
        """Save player_impacts table to disk."""
        frame.write_parquet(self.player_impacts_path, compression="zstd")
        self._impacts_cache = frame
        
        metadata = self.load_metadata()
        metadata.player_impacts_last_updated = datetime.now().isoformat()
        metadata.total_impacts = frame.height
        self._save_metadata()
    
    # -------------------------------------------------------------------------
    # Upsert Methods
    # -------------------------------------------------------------------------
    
    def upsert_players(self, new_data: pl.DataFrame) -> int:
        """Upsert player records, returning count of changes."""
        if new_data.height == 0:
            return 0
        
        existing = self.load_players()
        
        # Add _last_updated if not present
        if "_last_updated" not in new_data.columns:
            new_data = new_data.with_columns(pl.lit(datetime.now()).alias("_last_updated"))
        
        if existing.height == 0:
            self._save_players(new_data)
            return new_data.height
        
        # Merge: new records override existing ones
        merged = pl.concat([existing, new_data], how="diagonal_relaxed")
        merged = merged.unique(subset=["player_id"], keep="last")
        
        changes = merged.height - existing.height + new_data.height
        self._save_players(merged)
        return changes
    
    def upsert_player_seasons(self, new_data: pl.DataFrame) -> int:
        """Upsert player-season records, returning count of changes."""
        if new_data.height == 0:
            return 0
        
        existing = self.load_player_seasons()
        
        if "_last_updated" not in new_data.columns:
            new_data = new_data.with_columns(pl.lit(datetime.now()).alias("_last_updated"))
        
        if existing.height == 0:
            self._save_player_seasons(new_data)
            return new_data.height
        
        merged = pl.concat([existing, new_data], how="diagonal_relaxed")
        merged = merged.unique(subset=["player_id", "season"], keep="last")
        
        changes = merged.height - existing.height + new_data.height
        self._save_player_seasons(merged)
        return changes
    
    def upsert_player_impacts(self, new_data: pl.DataFrame) -> int:
        """Upsert player-impact records, returning count of changes."""
        if new_data.height == 0:
            return 0
        
        existing = self.load_player_impacts()
        
        if "_last_updated" not in new_data.columns:
            new_data = new_data.with_columns(pl.lit(datetime.now()).alias("_last_updated"))
        
        if existing.height == 0:
            self._save_player_impacts(new_data)
            return new_data.height
        
        merged = pl.concat([existing, new_data], how="diagonal_relaxed")
        merged = merged.unique(subset=["player_id", "season"], keep="last")
        
        changes = merged.height - existing.height + new_data.height
        self._save_player_impacts(merged)
        return changes
    
    # -------------------------------------------------------------------------
    # Update Bio for Specific Players
    # -------------------------------------------------------------------------
    
    def update_player_bio(
        self,
        player_id: str,
        bio_fields: dict[str, str],
    ) -> bool:
        """Update bio fields for a specific player.
        
        This is used for slowly-changing attributes like birthplace that
        only need to be fetched once.
        """
        players = self.load_players()
        if players.height == 0:
            return False
        
        match_idx = players.filter(pl.col("player_id") == player_id)
        if match_idx.height == 0:
            return False
        
        update_exprs = []
        for field, value in bio_fields.items():
            if field in players.columns:
                update_exprs.append(
                    pl.when(pl.col("player_id") == player_id)
                    .then(pl.lit(value))
                    .otherwise(pl.col(field))
                    .alias(field)
                )
        
        if update_exprs:
            update_exprs.append(
                pl.when(pl.col("player_id") == player_id)
                .then(pl.lit(True))
                .otherwise(pl.col("_bio_fetched"))
                .alias("_bio_fetched")
            )
            update_exprs.append(
                pl.when(pl.col("player_id") == player_id)
                .then(pl.lit(datetime.now()))
                .otherwise(pl.col("_last_updated"))
                .alias("_last_updated")
            )
            players = players.with_columns(update_exprs)
            self._save_players(players)
            return True
        
        return False
    
    def get_players_missing_bio(self) -> pl.DataFrame:
        """Get players that haven't had their bio fetched yet."""
        players = self.load_players()
        return players.filter(
            (pl.col("_bio_fetched").is_null()) | (pl.col("_bio_fetched") == False)
        )


# ============================================================================
# Data Builder - Orchestrates data refresh from nflverse
# ============================================================================

class NFLDataBuilder:
    """Builder class for populating the NFL data store from nflverse data.
    
    This class handles the actual data fetching and transformation logic,
    including efficient updates that avoid redundant fetching.
    """
    
    def __init__(self, store: NFLDataStore | None = None) -> None:
        self._store = store or NFLDataStore()
        self._nflreadpy_available = self._check_nflreadpy()
    
    def _check_nflreadpy(self) -> bool:
        """Check if nflreadpy is available."""
        try:
            from nflreadpy import load_players
            return True
        except ImportError:
            return False
    
    def build_all(
        self,
        *,
        seasons: Iterable[int] | None = None,
        force: bool = False,
        skip_bio: bool = False,
        skip_impacts: bool = False,
    ) -> dict[str, Any]:
        """Build/refresh all data tables.
        
        Args:
            seasons: Specific seasons to build. None means full range.
            force: Force rebuild even if data exists.
            skip_bio: Skip fetching bio data from PFR.
            skip_impacts: Skip building impact metrics.
        
        Returns:
            Dictionary with build statistics.
        """
        if not self._nflreadpy_available:
            raise RuntimeError("nflreadpy is not installed. Install it to build the data store.")
        
        self._store.initialize(force=force)
        metadata = self._store.load_metadata()
        
        target_seasons = self._resolve_seasons(seasons, metadata)
        
        stats = {
            "seasons": target_seasons,
            "players_added": 0,
            "player_seasons_added": 0,
            "impacts_added": 0,
            "bio_updated": 0,
            "errors": [],
        }
        
        logger.info("Building NFL data store for seasons %s-%s", target_seasons[0], target_seasons[-1])
        
        # Step 1: Build players table
        try:
            players_added = self._build_players(target_seasons)
            stats["players_added"] = players_added
            logger.info("Added/updated %s players", players_added)
        except Exception as exc:
            logger.error("Failed to build players: %s", exc)
            metadata.add_error("build_players", "all", type(exc).__name__, str(exc))
            stats["errors"].append({"table": "players", "error": str(exc)})
        
        # Step 2: Build player_seasons table
        try:
            seasons_added = self._build_player_seasons(target_seasons)
            stats["player_seasons_added"] = seasons_added
            logger.info("Added/updated %s player-season records", seasons_added)
        except Exception as exc:
            logger.error("Failed to build player_seasons: %s", exc)
            metadata.add_error("build_player_seasons", "all", type(exc).__name__, str(exc))
            stats["errors"].append({"table": "player_seasons", "error": str(exc)})
        
        # Step 3: Build player_impacts table
        if not skip_impacts:
            try:
                impacts_added = self._build_player_impacts(target_seasons)
                stats["impacts_added"] = impacts_added
                logger.info("Added/updated %s impact records", impacts_added)
            except Exception as exc:
                logger.error("Failed to build player_impacts: %s", exc)
                metadata.add_error("build_player_impacts", "all", type(exc).__name__, str(exc))
                stats["errors"].append({"table": "player_impacts", "error": str(exc)})
        
        # Step 4: Update bio data
        if not skip_bio:
            try:
                bio_updated = self._update_bio_data()
                stats["bio_updated"] = bio_updated
                logger.info("Updated bio data for %s players", bio_updated)
            except Exception as exc:
                logger.error("Failed to update bio data: %s", exc)
                metadata.add_error("update_bio", "all", type(exc).__name__, str(exc))
                stats["errors"].append({"table": "bio", "error": str(exc)})
        
        # Update metadata
        metadata.season_start = min(target_seasons)
        metadata.season_end = max(target_seasons)
        self._store._save_metadata()
        
        return stats
    
    def _resolve_seasons(
        self,
        seasons: Iterable[int] | None,
        metadata: DataStoreMetadata,
    ) -> list[int]:
        """Resolve season range to build."""
        if seasons is not None:
            return sorted(set(int(s) for s in seasons))
        return list(range(metadata.season_start, metadata.season_end + 1))
    
    def _build_players(self, seasons: Sequence[int]) -> int:
        """Build/update the players table from nflverse data."""
        from nflreadpy import load_players, load_rosters, load_ff_playerids
        
        # Load player directory
        players_raw = _to_polars(load_players())
        
        # Load roster data for additional fields
        try:
            rosters = _to_polars(load_rosters(seasons=list(seasons)))
        except Exception:
            rosters = pl.DataFrame()
        
        # Load player ID crosswalk
        try:
            playerids = _to_polars(load_ff_playerids())
        except Exception:
            playerids = pl.DataFrame()
        
        # Build ID mapping from rosters
        id_mapping = self._build_id_mapping(rosters)
        
        # Process players
        if players_raw.height == 0:
            return 0
        
        # Normalize column names
        col_mapping = {
            "gsis_id": "player_id",
            "display_name": "display_name",
            "first_name": "first_name",
            "last_name": "last_name",
            "birth_date": "birth_date",
            "college_name": "college",
            "height": "height",
            "weight": "weight",
            "position": "position",
            "position_group": "position_group",
        }
        
        # Create full_name if not present
        if "full_name" not in players_raw.columns:
            players_raw = players_raw.with_columns(
                pl.concat_str([
                    pl.col("first_name").fill_null(""),
                    pl.lit(" "),
                    pl.col("last_name").fill_null("")
                ]).str.strip_chars().alias("full_name")
            )
        
        # Select and rename columns
        available_cols = [c for c in col_mapping.keys() if c in players_raw.columns]
        selected = players_raw.select([
            pl.col(c).alias(col_mapping.get(c, c)) for c in available_cols
        ] + [pl.col("full_name")] if "full_name" not in col_mapping.values() else [
            pl.col(c).alias(col_mapping.get(c, c)) for c in available_cols
        ])
        
        # Add additional ID columns from playerids crosswalk
        if playerids.height > 0 and "gsis_id" in playerids.columns:
            id_cols = ["pfr_id", "pff_id", "espn_id", "sportradar_id"]
            available_id_cols = ["gsis_id"] + [c for c in id_cols if c in playerids.columns]
            id_crosswalk = playerids.select(available_id_cols).unique(subset=["gsis_id"])
            selected = selected.join(
                id_crosswalk.rename({"gsis_id": "player_id"}),
                on="player_id",
                how="left",
            )
        
        # Add draft info from original data
        for col in ["draft_year", "draft_round", "draft_pick", "draft_team", "esb_id", "otc_id"]:
            if col in players_raw.columns and col not in selected.columns:
                mapping_col = "player_id" if "gsis_id" not in players_raw.columns else "gsis_id"
                temp = players_raw.select([
                    pl.col(mapping_col).alias("player_id"),
                    pl.col(col)
                ]).unique(subset=["player_id"])
                selected = selected.join(temp, on="player_id", how="left")
        
        # Add metadata columns
        selected = selected.with_columns([
            pl.lit(datetime.now()).alias("_last_updated"),
            pl.lit(False).alias("_bio_fetched"),
        ])
        
        # Ensure all schema columns exist
        for col, dtype in PLAYERS_SCHEMA.items():
            if col not in selected.columns:
                selected = selected.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        # Filter to only players with valid IDs
        selected = selected.filter(
            pl.col("player_id").is_not_null() & (pl.col("player_id") != "")
        )
        
        return self._store.upsert_players(selected)
    
    def _build_id_mapping(self, rosters: pl.DataFrame) -> pl.DataFrame:
        """Build a mapping from gsis_id to pfr_id using roster data."""
        if rosters.is_empty():
            return pl.DataFrame({"gsis_id": [], "pfr_id": []})
        
        if "gsis_id" not in rosters.columns or "pfr_id" not in rosters.columns:
            return pl.DataFrame({"gsis_id": [], "pfr_id": []})
        
        mapping = (
            rosters.select(["gsis_id", "pfr_id"])
            .filter(pl.col("gsis_id").is_not_null() & pl.col("pfr_id").is_not_null())
            .unique(subset=["gsis_id"])
        )
        return mapping
    
    def _build_player_seasons(self, seasons: Sequence[int]) -> int:
        """Build/update the player_seasons table."""
        from nflreadpy import load_player_stats, load_rosters
        
        # Load player stats
        stats_raw = _to_polars(load_player_stats(seasons=list(seasons)))
        if stats_raw.height == 0:
            return 0
        
        # Filter to regular season
        if "season_type" in stats_raw.columns:
            stats_raw = stats_raw.filter(pl.col("season_type") == "REG")
        
        # Aggregate to player-season level
        aggregated = self._aggregate_player_seasons(stats_raw)
        
        # Merge snap counts from PFR (for seasons 2012+)
        snap_seasons = [s for s in seasons if s >= PFR_SNAP_MIN_SEASON]
        if snap_seasons:
            try:
                id_mapping = self._get_id_mapping(seasons)
                aggregated = self._merge_snap_counts(aggregated, snap_seasons, id_mapping)
            except Exception as exc:
                logger.warning("Failed to merge snap counts: %s", exc)
        
        # Add derived columns
        aggregated = aggregated.with_columns([
            (
                pl.col("offense_snaps").fill_null(0) +
                pl.col("defense_snaps").fill_null(0) +
                pl.col("special_teams_snaps").fill_null(0)
            ).cast(pl.Int32).alias("snaps_total"),
            (
                pl.col("passing_tds").fill_null(0) +
                pl.col("rushing_tds").fill_null(0) +
                pl.col("receiving_tds").fill_null(0)
            ).cast(pl.Int8).alias("total_touchdowns"),
            pl.lit(datetime.now()).alias("_last_updated"),
        ])
        
        # Ensure all schema columns exist
        for col, dtype in PLAYER_SEASONS_SCHEMA.items():
            if col not in aggregated.columns:
                aggregated = aggregated.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        return self._store.upsert_player_seasons(aggregated)
    
    def _aggregate_player_seasons(self, stats: pl.DataFrame) -> pl.DataFrame:
        """Aggregate weekly stats to player-season level."""
        # Column mappings from source to target
        string_sources = {
            "team": ["recent_team", "team", "current_team_abbr"],
            "player_id": ["player_id", "gsis_id"],
            "position": ["position", "player_position"],
            "position_group": ["position_group", "player_position_group"],
        }
        
        numeric_sources = {
            "pass_completions": ["completions"],
            "pass_attempts": ["attempts"],
            "passing_yards": ["passing_yards"],
            "passing_tds": ["passing_tds"],
            "passing_ints": ["interceptions"],
            "sacks_taken": ["sacks_suffered"],
            "sack_yards": ["sack_yards_lost"],
            "rushing_attempts": ["carries", "rushing_attempts"],
            "rushing_yards": ["rushing_yards"],
            "rushing_tds": ["rushing_tds"],
            "receiving_targets": ["targets"],
            "receiving_receptions": ["receptions"],
            "receiving_yards": ["receiving_yards"],
            "receiving_tds": ["receiving_tds"],
            "total_fumbles": ["fumbles"],
            "fumbles_lost": ["fumbles_lost"],
            "def_tackles_solo": ["def_tackles_solo"],
            "def_tackle_assists": ["def_tackle_assists"],
            "def_sacks": ["def_sacks"],
            "def_interceptions": ["def_interceptions"],
            "def_pass_defended": ["def_pass_defended"],
            "def_tds": ["def_tds"],
        }
        
        # Build string expressions
        string_exprs = []
        for target, sources in string_sources.items():
            available = [pl.col(s) for s in sources if s in stats.columns]
            if available:
                string_exprs.append(
                    pl.coalesce(available).fill_null("").alias(f"_{target}")
                )
            else:
                string_exprs.append(pl.lit("").alias(f"_{target}"))
        
        # Build numeric expressions
        numeric_exprs = []
        for target, sources in numeric_sources.items():
            available = [
                pl.col(s).cast(pl.Float64, strict=False).fill_null(0.0)
                for s in sources if s in stats.columns
            ]
            if available:
                numeric_exprs.append(pl.coalesce(available).alias(target))
            else:
                numeric_exprs.append(pl.lit(0.0).alias(target))
        
        # Prepare frame
        prepared = stats.with_columns(
            string_exprs + numeric_exprs + [
                pl.col("season").cast(pl.Int16),
                pl.col("week").cast(pl.Int16).alias("_week"),
            ]
        )
        
        # Aggregate
        agg_exprs = [
            pl.col("_week").n_unique().alias("games_played"),
        ] + [
            pl.col(target).sum().alias(target)
            for target in numeric_sources.keys()
        ]
        
        grouped = prepared.group_by(
            ["_player_id", "_position", "_position_group", "_team", "season"]
        ).agg(agg_exprs)
        
        # Rename columns
        result = grouped.rename({
            "_player_id": "player_id",
            "_position": "position",
            "_position_group": "position_group",
            "_team": "team",
        })
        
        return result.sort(["player_id", "season"])
    
    def _get_id_mapping(self, seasons: Sequence[int]) -> pl.DataFrame:
        """Get player ID mapping from rosters."""
        try:
            from nflreadpy import load_rosters
            rosters = _to_polars(load_rosters(seasons=list(seasons)))
            return self._build_id_mapping(rosters)
        except Exception:
            return pl.DataFrame({"gsis_id": [], "pfr_id": []})
    
    def _merge_snap_counts(
        self,
        aggregated: pl.DataFrame,
        seasons: Sequence[int],
        id_mapping: pl.DataFrame,
    ) -> pl.DataFrame:
        """Merge snap counts from PFR into the aggregated stats."""
        from .pfr.client import PFRClient
        from .pfr.snap_counts import fetch_team_snap_counts
        
        TEAM_TO_PFR_SLUG: Mapping[str, str] = {
            "ARI": "crd", "ATL": "atl", "BAL": "rav", "BUF": "buf", "CAR": "car",
            "CHI": "chi", "CIN": "cin", "CLE": "cle", "DAL": "dal", "DEN": "den",
            "DET": "det", "GB": "gnb", "GNB": "gnb", "HOU": "htx", "IND": "clt",
            "JAC": "jax", "JAX": "jax", "KC": "kan", "KAN": "kan", "LAC": "sdg",
            "SD": "sdg", "LAR": "ram", "LA": "ram", "STL": "ram", "LV": "rai",
            "LVR": "rai", "OAK": "rai", "MIA": "mia", "MIN": "min", "NE": "nwe",
            "NO": "nor", "NYG": "nyg", "NYJ": "nyj", "PHI": "phi", "PIT": "pit",
            "SEA": "sea", "SF": "sfo", "SFO": "sfo", "TB": "tam", "TEN": "oti",
            "WAS": "was",
        }
        
        if id_mapping.is_empty():
            return aggregated
        
        season_teams = (
            aggregated.select(["season", "team"])
            .filter(pl.col("team").is_not_null() & (pl.col("team") != ""))
            .unique()
        )
        
        snap_frames = []
        with PFRClient(enable_cache=True, min_delay=1.5) as client:
            for season in seasons:
                teams = season_teams.filter(pl.col("season") == season)["team"].to_list()
                for team in teams:
                    slug = TEAM_TO_PFR_SLUG.get(team.upper())
                    if not slug:
                        continue
                    try:
                        snaps = fetch_team_snap_counts(client, team_slug=slug, season=season)
                        if snaps.height > 0:
                            snap_frames.append(snaps.with_columns([
                                pl.lit(team.upper()).alias("team"),
                                pl.lit(season).alias("season"),
                            ]))
                    except Exception as exc:
                        logger.debug("Failed to fetch snaps for %s %s: %s", team, season, exc)
        
        if not snap_frames:
            return aggregated
        
        snap_all = pl.concat(snap_frames, how="vertical_relaxed").filter(
            pl.col("pfr_id").is_not_null() & (pl.col("pfr_id") != "")
        )
        
        # Join with ID mapping
        aggregated_with_pfr = aggregated.join(
            id_mapping.rename({"gsis_id": "player_id"}),
            on="player_id",
            how="left",
        )
        
        # Merge snap data
        merged = aggregated_with_pfr.join(
            snap_all.select([
                "pfr_id", "season", "team",
                pl.col("_snap_offense").alias("_pfr_offense_snaps"),
                pl.col("_snap_defense").alias("_pfr_defense_snaps"),
                pl.col("_snap_st").alias("_pfr_st_snaps"),
            ]),
            on=["pfr_id", "season", "team"],
            how="left",
        )
        
        # Update snap columns
        merged = merged.with_columns([
            pl.coalesce([
                pl.col("_pfr_offense_snaps"),
                pl.col("offense_snaps")
            ]).cast(pl.Int32).alias("offense_snaps"),
            pl.coalesce([
                pl.col("_pfr_defense_snaps"),
                pl.col("defense_snaps")
            ]).cast(pl.Int32).alias("defense_snaps"),
            pl.coalesce([
                pl.col("_pfr_st_snaps"),
                pl.col("special_teams_snaps")
            ]).cast(pl.Int32).alias("special_teams_snaps"),
        ])
        
        # Drop temp columns
        drop_cols = [c for c in merged.columns if c.startswith("_pfr_") or c == "pfr_id"]
        return merged.drop(drop_cols)
    
    def _build_player_impacts(self, seasons: Sequence[int]) -> int:
        """Build/update the player_impacts table from play-by-play data."""
        from nflreadpy import load_pbp
        
        aggregated_frames = []
        
        for season in seasons:
            try:
                pbp = _to_polars(load_pbp(seasons=[season]))
                if pbp.is_empty():
                    continue
                
                # Filter to regular season
                if "season_type" in pbp.columns:
                    pbp = pbp.filter(pl.col("season_type").str.to_uppercase() == "REG")
                
                impacts = self._aggregate_impacts_from_pbp(pbp, season)
                if impacts.height > 0:
                    aggregated_frames.append(impacts)
                    logger.debug("Aggregated impacts for season %s: %s rows", season, impacts.height)
                    
            except Exception as exc:
                logger.warning("Failed to build impacts for season %s: %s", season, exc)
        
        if not aggregated_frames:
            return 0
        
        combined = pl.concat(aggregated_frames, how="diagonal_relaxed")
        combined = combined.with_columns(pl.lit(datetime.now()).alias("_last_updated"))
        
        # Ensure schema columns
        for col, dtype in PLAYER_IMPACTS_SCHEMA.items():
            if col not in combined.columns:
                combined = combined.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        return self._store.upsert_player_impacts(combined)
    
    def _aggregate_impacts_from_pbp(self, pbp: pl.DataFrame, season: int) -> pl.DataFrame:
        """Aggregate EPA/WPA metrics from play-by-play data."""
        # This reuses logic from player_impacts.py but simplified
        
        def _numeric_expr(column: str) -> pl.Expr:
            if column in pbp.columns:
                return pl.col(column).cast(pl.Float64, strict=False).fill_null(0.0)
            return pl.lit(0.0)
        
        frames = []
        
        # QB impacts
        if "passer_player_id" in pbp.columns:
            qb_data = (
                pbp.filter(pl.col("passer_player_id").is_not_null())
                .group_by("passer_player_id")
                .agg([
                    _numeric_expr("qb_epa").sum().alias("qb_epa"),
                    _numeric_expr("qb_wpa").sum().alias("qb_wpa"),
                ])
                .rename({"passer_player_id": "player_id"})
                .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
            )
            frames.append(qb_data)
        
        # Skill impacts (rusher + receiver)
        for role, col in [("rusher", "rusher_player_id"), ("receiver", "receiver_player_id")]:
            if col in pbp.columns:
                role_data = (
                    pbp.filter(pl.col(col).is_not_null())
                    .group_by(col)
                    .agg([
                        _numeric_expr("epa").sum().alias("skill_epa"),
                        _numeric_expr("wpa").sum().alias("skill_wpa"),
                    ])
                    .rename({col: "player_id"})
                    .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                )
                frames.append(role_data)
        
        # Defensive impacts
        def_cols = [
            "solo_tackle_1_player_id", "interception_player_id",
            "sack_player_id", "fumble_recovery_1_player_id"
        ]
        for col in def_cols:
            if col in pbp.columns:
                def_data = (
                    pbp.filter(pl.col(col).is_not_null())
                    .group_by(col)
                    .agg([
                        _numeric_expr("epa").sum().alias("def_epa"),
                        _numeric_expr("wpa").sum().alias("def_wpa"),
                    ])
                    .rename({col: "player_id"})
                    .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                )
                frames.append(def_data)
        
        # Kicker impacts
        if "kicker_player_id" in pbp.columns:
            kicker_data = (
                pbp.filter(pl.col("kicker_player_id").is_not_null())
                .group_by("kicker_player_id")
                .agg([
                    _numeric_expr("epa").sum().alias("kicker_epa"),
                    _numeric_expr("wpa").sum().alias("kicker_wpa"),
                ])
                .rename({"kicker_player_id": "player_id"})
                .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
            )
            frames.append(kicker_data)
        
        # Punter impacts
        if "punter_player_id" in pbp.columns:
            punter_data = (
                pbp.filter(pl.col("punter_player_id").is_not_null())
                .group_by("punter_player_id")
                .agg([
                    _numeric_expr("epa").sum().alias("punter_epa"),
                    _numeric_expr("wpa").sum().alias("punter_wpa"),
                ])
                .rename({"punter_player_id": "player_id"})
                .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
            )
            frames.append(punter_data)
        
        if not frames:
            return pl.DataFrame(schema=PLAYER_IMPACTS_SCHEMA)
        
        # Merge all frames
        merged = frames[0]
        for frame in frames[1:]:
            new_cols = [c for c in frame.columns if c not in {"player_id", "season"} and c not in merged.columns]
            if new_cols:
                merged = merged.join(
                    frame.select(["player_id", "season"] + new_cols),
                    on=["player_id", "season"],
                    how="full",
                    coalesce=True,
                )
            else:
                # Aggregate duplicate metrics
                merged = pl.concat([merged, frame], how="diagonal_relaxed").group_by(
                    ["player_id", "season"]
                ).agg([
                    pl.col(c).sum() if c not in {"player_id", "season"} else pl.col(c).first()
                    for c in merged.columns
                ])
        
        return merged.filter(pl.col("player_id").is_not_null() & (pl.col("player_id") != ""))
    
    def _update_bio_data(self, *, batch_size: int = 100) -> int:
        """Update bio data for players missing it.
        
        This fetches slowly-changing attributes like birthplace from PFR,
        but only for players that haven't been fetched yet.
        """
        from .pfr.client import PFRClient
        from .pfr.players import fetch_player_bio_fields
        
        players = self._store.load_players()
        
        # Get players missing bio data
        missing = players.filter(
            (pl.col("_bio_fetched").is_null() | (pl.col("_bio_fetched") == False)) &
            pl.col("pfr_id").is_not_null() &
            (pl.col("pfr_id") != "")
        )
        
        if missing.height == 0:
            return 0
        
        # Limit batch size
        to_fetch = missing.head(batch_size)
        updated_count = 0
        
        with PFRClient(enable_cache=True, min_delay=1.0) as client:
            for row in to_fetch.iter_rows(named=True):
                pfr_id = row.get("pfr_id")
                player_id = row.get("player_id")
                
                if not pfr_id or not player_id:
                    continue
                
                try:
                    bio = fetch_player_bio_fields(client, pfr_id)
                    if bio:
                        self._store.update_player_bio(player_id, {
                            "handedness": bio.get("handedness", "N/A"),
                            "birth_city": bio.get("birth_city", "N/A"),
                            "birth_state": bio.get("birth_state", "N/A"),
                            "birth_country": bio.get("birth_country", "N/A"),
                        })
                        updated_count += 1
                except HTTPError as exc:
                    if getattr(exc.response, "status_code", None) == 429:
                        logger.warning("Rate limited during bio fetch; stopping batch.")
                        break
                    logger.debug("Failed to fetch bio for %s: %s", pfr_id, exc)
                except Exception as exc:
                    logger.debug("Failed to fetch bio for %s: %s", pfr_id, exc)
        
        return updated_count


# ============================================================================
# Module-level convenience functions
# ============================================================================

_default_store: NFLDataStore | None = None


def get_default_store() -> NFLDataStore:
    """Get the default data store instance."""
    global _default_store
    if _default_store is None:
        _default_store = NFLDataStore()
    return _default_store


def initialize_store(*, force: bool = False) -> None:
    """Initialize the default data store."""
    get_default_store().initialize(force=force)


def build_store(
    *,
    seasons: Iterable[int] | None = None,
    force: bool = False,
    skip_bio: bool = False,
    skip_impacts: bool = False,
) -> dict[str, Any]:
    """Build/refresh the default data store."""
    store = get_default_store()
    builder = NFLDataBuilder(store)
    return builder.build_all(
        seasons=seasons,
        force=force,
        skip_bio=skip_bio,
        skip_impacts=skip_impacts,
    )


__all__ = [
    "NFLDataStore",
    "NFLDataBuilder",
    "DataStoreMetadata",
    "PLAYERS_SCHEMA",
    "PLAYER_SEASONS_SCHEMA", 
    "PLAYER_IMPACTS_SCHEMA",
    "get_default_store",
    "initialize_store",
    "build_store",
    "DATA_DIRECTORY",
]

