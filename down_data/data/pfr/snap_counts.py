"""Scrapers for Pro-Football-Reference snap-count tables."""

from __future__ import annotations

from typing import Iterable

import polars as pl
from bs4 import BeautifulSoup, Comment, Tag

from .client import PFRClient

SNAP_COUNTS_TABLE_ID = "snap_counts"


def _parse_int(value: str | None) -> int:
    if value is None:
        return 0
    cleaned = value.replace(",", "").replace("%", "").strip()
    if not cleaned:
        return 0
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def _iter_snap_rows(table: Tag) -> Iterable[dict[str, object]]:
    body = table.find("tbody")
    if body is None:
        return []

    rows: list[dict[str, object]] = []
    for row in body.find_all("tr"):
        if "class" in row.attrs and "thead" in row["class"]:
            continue

        player_cell = row.find("th")
        if player_cell is None:
            continue
        player_name = player_cell.get_text(strip=True)
        if not player_name or player_name.lower().startswith("team total"):
            continue

        pfr_id = (player_cell.get("data-append-csv") or "").strip()
        if not pfr_id:
            link = player_cell.find("a")
            if link and link.get("href"):
                href = link["href"]
                pfr_id = href.rstrip("/").split("/")[-1].replace(".htm", "")
        if not pfr_id:
            continue

        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        pos = cells[0].get_text(strip=True)
        offense_snaps = _parse_int(cells[1].get_text(strip=True))
        defense_snaps = _parse_int(cells[3].get_text(strip=True))
        st_snaps = _parse_int(cells[5].get_text(strip=True))

        rows.append(
            {
                "pfr_id": pfr_id,
                "player_name": player_name,
                "position": pos,
                "_snap_offense": offense_snaps,
                "_snap_defense": defense_snaps,
                "_snap_st": st_snaps,
            }
        )
    return rows


def _extract_snap_table(html: str) -> Tag:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id=SNAP_COUNTS_TABLE_ID)
    if table is not None:
        return table

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        try:
            snippet = BeautifulSoup(comment, "lxml")
        except Exception:
            continue
        table = snippet.find("table", id=SNAP_COUNTS_TABLE_ID)
        if table is not None:
            return table
    raise ValueError("Snap-count table not found in supplied HTML.")


def fetch_team_snap_counts(
    client: PFRClient,
    *,
    team_slug: str,
    season: int,
) -> pl.DataFrame:
    """Return snap counts for ``team_slug`` (PFR team code) in ``season``."""

    path = f"/teams/{team_slug.lower()}/{season}-snap-counts.htm"
    response = client.get(path)
    table = _extract_snap_table(response.text)
    rows = list(_iter_snap_rows(table))
    if not rows:
        return pl.DataFrame(
            schema=[
                ("pfr_id", pl.Utf8),
                ("player_name", pl.Utf8),
                ("position", pl.Utf8),
                ("_snap_offense", pl.Int64),
                ("_snap_defense", pl.Int64),
                ("_snap_st", pl.Int64),
            ]
        )
    return pl.from_dicts(rows)


