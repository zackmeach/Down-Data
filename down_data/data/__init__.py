"""Data access helpers and cache utilities for Down-Data."""

from __future__ import annotations

from .pfr import (
    DEFAULT_USER_AGENT as PFR_DEFAULT_USER_AGENT,
    PFRClient,
    fetch_player_tables as pfr_fetch_player_tables,
    flatten_columns as pfr_flatten_columns,
    list_table_ids as pfr_list_table_ids,
    read_all_tables as pfr_read_all_tables,
    read_commented_table_by_id as pfr_read_commented_table_by_id,
    read_table_by_id as pfr_read_table_by_id,
)

from .nfl_datastore import (
    NFLDataStore,
    NFLDataBuilder,
    get_default_store,
    initialize_store,
    build_store,
    DATA_DIRECTORY,
    PLAYERS_SCHEMA,
    PLAYER_SEASONS_SCHEMA,
    PLAYER_IMPACTS_SCHEMA,
)

__all__ = [
    # PFR utilities
    "PFRClient",
    "PFR_DEFAULT_USER_AGENT",
    "pfr_fetch_player_tables",
    "pfr_flatten_columns",
    "pfr_list_table_ids",
    "pfr_read_all_tables",
    "pfr_read_commented_table_by_id",
    "pfr_read_table_by_id",
    # NFL Data Store
    "NFLDataStore",
    "NFLDataBuilder",
    "get_default_store",
    "initialize_store",
    "build_store",
    "DATA_DIRECTORY",
    "PLAYERS_SCHEMA",
    "PLAYER_SEASONS_SCHEMA",
    "PLAYER_IMPACTS_SCHEMA",
]

