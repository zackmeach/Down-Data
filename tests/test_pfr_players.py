"""Tests for player-level scraping helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import types
import unittest

import polars as pl

from down_data.data.pfr.client import PFRClient
from down_data.data.pfr.players import (
    extract_player_tables,
    fetch_player_tables,
)


FIXTURES = Path(__file__).resolve().parent / "data" / "pfr"


class _StubResponse(types.SimpleNamespace):
    def raise_for_status(self) -> None:  # pragma: no cover
        return None


class PFRPlayersTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.player_html = (FIXTURES / "player_sample.html").read_text(encoding="utf-8")

    def test_extract_player_tables_parses_all(self) -> None:
        tables = extract_player_tables(self.player_html)
        self.assertSetEqual(set(tables.keys()), {"games_played", "receiving_and_rushing"})
        self.assertIsInstance(tables["games_played"], pl.DataFrame)

    def test_fetch_player_tables_uses_client(self) -> None:
        client = self._make_stub_client(self.player_html)
        tables = fetch_player_tables(client, "PittKy00")
        self.assertIn("receiving_and_rushing", tables)
        self.assertEqual(tables["receiving_and_rushing"]["Receiving_Rec"][0], 68)

    def _make_stub_client(self, html: str) -> PFRClient:
        client = PFRClient(enable_cache=False)

        def fake_get(_path: str, **_kwargs: Any) -> _StubResponse:
            return _StubResponse(text=html)

        client.get = fake_get  # type: ignore[assignment]
        return client


if __name__ == "__main__":
    unittest.main()

