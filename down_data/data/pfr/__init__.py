"""Utilities for retrieving NFL data from Pro-Football-Reference."""

from __future__ import annotations

from .client import DEFAULT_USER_AGENT, PFRClient
from .html import (
    flatten_columns,
    list_table_ids,
    read_all_tables,
    read_commented_table_by_id,
    read_table_by_id,
)
from .players import fetch_player_page, fetch_player_tables

__all__ = [
    "DEFAULT_USER_AGENT",
    "PFRClient",
    "flatten_columns",
    "list_table_ids",
    "read_all_tables",
    "read_commented_table_by_id",
    "read_table_by_id",
    "fetch_player_page",
    "fetch_player_tables",
]

