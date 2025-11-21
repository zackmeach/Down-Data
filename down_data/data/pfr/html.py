"""HTML parsing helpers tailored for Pro-Football-Reference pages."""

from __future__ import annotations

import io
from typing import Iterable, Sequence, Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment


def read_all_tables(html: str) -> list[pd.DataFrame]:
    """Return every table found in ``html`` using :func:`pandas.read_html`."""

    if not html.strip():
        return []
    return pd.read_html(io.StringIO(html))


def _read_single_table(table_html: str) -> pd.DataFrame:
    tables = pd.read_html(io.StringIO(table_html))
    if not tables:
        raise ValueError("No tables parsed from supplied HTML fragment.")
    return tables[0]


def _ensure_soup(html: str, soup: Optional[BeautifulSoup]) -> BeautifulSoup:
    return soup if soup is not None else BeautifulSoup(html, "lxml")


def _collect_table_ids_from_soup(source: BeautifulSoup, ids: set[str]) -> None:
    """Populate ``ids`` with table identifiers discovered in ``source``."""

    for container in source.find_all(
        "div", id=lambda value: isinstance(value, str) and value.startswith("div_")
    ):
        ids.add(container["id"][4:])

    for table in source.find_all("table"):
        table_id = table.get("id")
        if table_id:
            ids.add(table_id)

    for comment in source.find_all(string=lambda text: isinstance(text, Comment)):
        try:
            snippet = comment.strip()
            if "div_" not in snippet and "<table" not in snippet:
                continue
            comment_soup = BeautifulSoup(comment, "lxml")
        except Exception:
            continue
        _collect_table_ids_from_soup(comment_soup, ids)


def read_table_by_id(
    html: str,
    table_id: str,
    *,
    soup: BeautifulSoup | None = None,
) -> pd.DataFrame:
    """Return a table whose ``id`` attribute matches ``table_id``.

    The helper checks both regular tables and tables wrapped in HTML comments,
    mirroring the layout conventions used across Sports Reference sites.
    """

    soup = _ensure_soup(html, soup)
    table = soup.find("table", id=table_id)
    if table is not None:
        return _read_single_table(str(table))

    return read_commented_table_by_id(html, table_id, soup=soup)


def read_commented_table_by_id(
    html: str,
    table_id: str,
    *,
    soup: BeautifulSoup | None = None,
) -> pd.DataFrame:
    """Return a comment-wrapped table matching ``table_id``."""

    soup = _ensure_soup(html, soup)
    container = soup.find("div", id=f"div_{table_id}")
    comments: Iterable[str]

    if container is not None:
        comments = container.find_all(
            string=lambda text: isinstance(text, Comment)
        )
    else:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))

    for comment in comments:
        comment_soup = BeautifulSoup(comment, "lxml")
        table = comment_soup.find("table", id=table_id)
        if table is not None:
            return _read_single_table(str(table))

    raise ValueError(f"Table '{table_id}' not found in HTML comments.")


def flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns emitted by :func:`read_html`."""

    frame = frame.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        new_columns = [
            "_".join(
                str(level).strip()
                for level in col
                if str(level).strip() and not str(level).startswith("Unnamed")
            )
            for col in frame.columns
        ]
    else:
        new_columns = [str(col).strip() for col in frame.columns]

    # Deduplicate and fill empty names
    seen: dict[str, int] = {}
    final_columns = []
    for idx, name in enumerate(new_columns):
        if not name:
            name = f"col_{idx}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        final_columns.append(name)

    frame.columns = final_columns
    return frame


def normalise_column_names(frame: pd.DataFrame) -> list[str]:
    """Return simple string column names for matching/inspection."""

    if isinstance(frame.columns, pd.MultiIndex):
        flattened = [
            " ".join(
                str(level).strip()
                for level in col
                if str(level).strip() and not str(level).startswith("Unnamed")
            ).strip()
            for col in frame.columns
        ]
        return [name or f"__col_{idx}__" for idx, name in enumerate(flattened)]
    return [str(name).strip() for name in frame.columns]


def select_table_with_columns(
    tables: Sequence[pd.DataFrame],
    required_columns: Iterable[str],
) -> pd.DataFrame:
    """Return the first table whose columns include ``required_columns``."""

    required = {col.lower() for col in required_columns}
    for table in tables:
        columns = {name.lower() for name in normalise_column_names(table)}
        if required.issubset(columns):
            return table
    raise ValueError(
        f"Could not find table containing columns {sorted(required_columns)!r}"
    )


def list_table_ids(html: str, *, soup: BeautifulSoup | None = None) -> list[str]:
    """Return the table identifiers exposed by ``div_*`` wrappers on the page."""

    soup = _ensure_soup(html, soup)
    table_ids: set[str] = set()
    _collect_table_ids_from_soup(soup, table_ids)
    return sorted(table_ids)

