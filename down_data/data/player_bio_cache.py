"""Local cache utilities for PFR-sourced player bio information."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

from .pfr.players import fetch_player_bio_fields
from .pfr.client import PFRClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIRECTORY = PROJECT_ROOT / "data" / "cache"
PLAYER_BIO_CACHE_PATH = CACHE_DIRECTORY / "pfr_player_bio.parquet"

_BIO_SCHEMA = {
    "pfr_id": pl.Utf8,
    "handedness": pl.Utf8,
    "birth_city": pl.Utf8,
    "birth_state": pl.Utf8,
    "birth_country": pl.Utf8,
}


def _empty_bio_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_BIO_SCHEMA)


def load_player_bio_cache() -> pl.DataFrame:
    """Load the cached PFR bio details from disk."""

    if PLAYER_BIO_CACHE_PATH.exists():
        return pl.read_parquet(PLAYER_BIO_CACHE_PATH)
    return _empty_bio_frame()


def save_player_bio_cache(frame: pl.DataFrame) -> None:
    """Persist the supplied bio cache to disk."""

    CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(PLAYER_BIO_CACHE_PATH, compression="zstd")


def upsert_player_bio_entries(
    cache: pl.DataFrame,
    entries: Iterable[dict[str, str]],
) -> pl.DataFrame:
    """Merge new bio entries into the cache."""

    entries_list = list(entries)
    if not entries_list:
        return cache

    new_frame = pl.DataFrame(entries_list)
    merged = (
        pl.concat([cache, new_frame], how="vertical_relaxed")
        if cache.height > 0
        else new_frame
    )
    merged = merged.unique(subset=["pfr_id"], keep="last")
    save_player_bio_cache(merged)
    return merged


def fetch_and_cache_player_bio(
    *,
    pfr_id: str,
    cache: pl.DataFrame | None = None,
    client: PFRClient | None = None,
) -> tuple[dict[str, str], pl.DataFrame]:
    """Fetch bio info for ``pfr_id`` and update the cache."""

    owns_client = False
    if client is None:
        client = PFRClient(enable_cache=True, min_delay=1.0)
        owns_client = True
    try:
        bio = fetch_player_bio_fields(client, pfr_id)
    finally:
        if owns_client:
            client.close()

    payload = {
        "pfr_id": pfr_id,
        "handedness": bio.get("handedness") or "N/A",
        "birth_city": bio.get("birth_city") or "N/A",
        "birth_state": bio.get("birth_state") or "N/A",
        "birth_country": bio.get("birth_country") or "N/A",
    }

    cache_frame = cache if cache is not None else load_player_bio_cache()
    updated_cache = upsert_player_bio_entries(cache_frame, [payload])
    return payload, updated_cache


__all__ = [
    "PLAYER_BIO_CACHE_PATH",
    "load_player_bio_cache",
    "save_player_bio_cache",
    "upsert_player_bio_entries",
    "fetch_and_cache_player_bio",
]


