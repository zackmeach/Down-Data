"""Helpers for league-wide tables hosted on Pro-Football-Reference."""

from __future__ import annotations

from typing import Optional

import polars as pl

from .client import PFRClient
from .html import flatten_columns, read_all_tables, read_table_by_id


def fetch_league_table(
    client: PFRClient,
    *,
    path: str,
    season: Optional[int] = None,
    table_id: Optional[str] = None,
    table_index: int = 0,
) -> pl.DataFrame:
    """Fetch a league-level table and return it as a :class:`polars.DataFrame`."""

    response = client.get(path)
    html = response.text

    if table_id:
        table = read_table_by_id(html, table_id)
    else:
        tables = read_all_tables(html)
        if table_index >= len(tables):
            raise IndexError(
                f"Table index {table_index} out of range for {path!r} "
                f"(found {len(tables)} tables)."
            )
        table = tables[table_index]

    table = flatten_columns(table)
    if season is not None and "season" not in table.columns:
        table = table.copy()
        table["season"] = season

    return pl.from_pandas(table)


def fetch_rushing_advanced(
    client: PFRClient, season: int, *, table_index: int = 0
) -> pl.DataFrame:
    """Convenience wrapper for ``years/{season}/rushing_advanced.htm``."""

    path = f"/years/{season}/rushing_advanced.htm"
    frame = fetch_league_table(client, path=path, season=season, table_index=table_index)
    return frame


def fetch_passing_advanced(
    client: PFRClient, season: int, *, table_index: int = 0
) -> pl.DataFrame:
    """Fetch advanced passing statistics for ``season``."""

    path = f"/years/{season}/passing_advanced.htm"
    return fetch_league_table(client, path=path, season=season, table_index=table_index)

