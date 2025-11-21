"""Team-specific scraping helpers for Pro-Football-Reference."""

from __future__ import annotations

from typing import Iterable

import polars as pl

from .client import PFRClient
from .html import (
    flatten_columns,
    normalise_column_names,
    read_all_tables,
    read_commented_table_by_id,
    select_table_with_columns,
)


def _team_path(team_abbr: str, season: int) -> str:
    return f"/teams/{team_abbr.lower()}/{season}.htm"


def fetch_team_season_html(client: PFRClient, team_abbr: str, season: int) -> str:
    """Return the raw HTML for a team season page."""

    response = client.get(_team_path(team_abbr, season))
    return response.text


def parse_schedule_table(html: str) -> pl.DataFrame:
    """Extract the schedule/results table from a team season page."""

    tables = read_all_tables(html)
    table = select_table_with_columns(tables, ["Week", "Date", "Opp"])
    table = flatten_columns(table)
    frame = pl.from_pandas(table)
    if "Opp" in frame.columns:
        frame = frame.rename({"Opp": "Opponent"})
    return frame


def parse_team_stats_table(html: str, table_id: str = "team_stats") -> pl.DataFrame:
    """Extract a comment-wrapped team stats table."""

    table = read_commented_table_by_id(html, table_id)
    table = flatten_columns(table)
    return pl.from_pandas(table)


def fetch_team_schedule(client: PFRClient, team_abbr: str, season: int) -> pl.DataFrame:
    """Fetch and parse the schedule/results table for ``team_abbr``."""

    html = fetch_team_season_html(client, team_abbr, season)
    frame = parse_schedule_table(html)
    existing_cols = {name.lower() for name in frame.columns}
    if "season" not in existing_cols:
        frame = frame.with_columns(pl.lit(season).alias("season"))
    if "team" not in existing_cols:
        frame = frame.with_columns(pl.lit(team_abbr.upper()).alias("team"))
    return frame


def fetch_team_stats(
    client: PFRClient,
    team_abbr: str,
    season: int,
    *,
    table_id: str = "team_stats",
) -> pl.DataFrame:
    """Fetch a team stats summary table commonly wrapped in HTML comments."""

    html = fetch_team_season_html(client, team_abbr, season)
    frame = parse_team_stats_table(html, table_id=table_id)
    return frame.with_columns(
        pl.lit(team_abbr.upper()).alias("team"),
        pl.lit(season).alias("season"),
    )

