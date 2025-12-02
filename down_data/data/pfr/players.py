"""Player-specific scraping helpers for Pro-Football-Reference."""

from __future__ import annotations

from typing import Iterable, Any

import polars as pl
from bs4 import BeautifulSoup
import re

from .client import PFRClient
from .html import flatten_columns, list_table_ids, read_table_by_id

_HAND_PATTERN = re.compile(r"(Throws|Shoots|Kicks):\s*([A-Za-z]+)", re.IGNORECASE)


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


def parse_player_bio_fields(html: str) -> dict[str, str]:
    """Extract handedness and birth location fields from a player page."""

    soup = BeautifulSoup(html, "lxml")
    meta = soup.find("div", id="meta")
    if meta is None:
        return {}

    bio: dict[str, str] = {}

    for paragraph in meta.find_all("p"):
        text = " ".join(paragraph.stripped_strings).replace("\xa0", " ").strip()
        if not text:
            continue

        if text.startswith("Position"):
            matcher = _HAND_PATTERN.search(text)
            if matcher:
                bio["handedness"] = matcher.group(2).title()
        elif text.startswith("Born:"):
            location = None
            parts = text.split(" in ", 1)
            if len(parts) == 2:
                location = parts[1]
            if location:
                location = location.split("(")[0].strip()
                segments = [segment.strip() for segment in location.split(",") if segment.strip()]
                if segments:
                    bio["birth_city"] = segments[0]
                if len(segments) >= 2:
                    bio["birth_state"] = segments[1]
                if len(segments) >= 3:
                    bio["birth_country"] = segments[2]
                elif len(segments) == 2 and segments[1] and len(segments[1]) > 2:
                    # When only city + country present (e.g., London, England)
                    bio.setdefault("birth_country", segments[1])
    return bio


def fetch_player_bio_fields(client: PFRClient, player_id: str) -> dict[str, str]:
    """Fetch and parse bio fields for ``player_id``."""

    html = fetch_player_page(client, player_id)
    return parse_player_bio_fields(html)

