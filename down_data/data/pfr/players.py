"""Player-specific scraping helpers for Pro-Football-Reference."""

from __future__ import annotations

from typing import Iterable

import polars as pl
from bs4 import BeautifulSoup

from .client import PFRClient
from .html import flatten_columns, list_table_ids, read_table_by_id


def _player_path(player_id: str) -> str:
    return f"/players/{player_id[0].upper()}/{player_id}.htm"


def fetch_player_page(client: PFRClient, player_id: str) -> str:
    """Return the raw HTML for a player's profile page."""

    response = client.get(_player_path(player_id))
    return response.text


def extract_player_tables(html: str, table_ids: Iterable[str] | None = None) -> dict[str, pl.DataFrame]:
    """Parse all available tables from ``html`` and return Polars frames keyed by id."""

    soup = BeautifulSoup(html, "lxml")
    ids = list(table_ids) if table_ids is not None else list_table_ids(html, soup=soup)
    tables: dict[str, pl.DataFrame] = {}

    for table_id in ids:
        try:
            table = read_table_by_id(html, table_id, soup=soup)
        except ValueError:
            continue
        flattened = flatten_columns(table)
        tables[table_id] = pl.from_pandas(flattened)

    return tables


def fetch_player_tables(client: PFRClient, player_id: str, table_ids: Iterable[str] | None = None) -> dict[str, pl.DataFrame]:
    """Fetch and parse all available tables for ``player_id``."""

    html = fetch_player_page(client, player_id)
    return extract_player_tables(html, table_ids=table_ids)

