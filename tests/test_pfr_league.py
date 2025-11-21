"""Tests for league-level PFR scraping helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import types
import unittest

import polars as pl

from down_data.data.pfr.client import PFRClient
from down_data.data.pfr.league import fetch_league_table, fetch_rushing_advanced


FIXTURES = Path(__file__).resolve().parent / "data" / "pfr"


class _StubResponse(types.SimpleNamespace):
    def raise_for_status(self) -> None:  # pragma: no cover - provided for compatibility
        return None


class PFRLeagueTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.league_html = (FIXTURES / "rushing_advanced.html").read_text(encoding="utf-8")

    def test_fetch_league_table_returns_polars_frame(self) -> None:
        client = self._make_stub_client(self.league_html)
        frame = fetch_league_table(
            client,
            path="/years/2025/rushing_advanced.htm",
            table_id="rushing_advanced",
            season=2025,
        )
        self.assertIsInstance(frame, pl.DataFrame)
        self.assertIn("season", frame.columns)
        self.assertEqual(frame["season"][0], 2025)

    def test_fetch_rushing_advanced_uses_default_index(self) -> None:
        client = self._make_stub_client(self.league_html)
        frame = fetch_rushing_advanced(client, 2025)
        self.assertEqual(frame.height, 1)
        self.assertIn("Player", frame.columns)

    def _make_stub_client(self, html: str) -> PFRClient:
        client = PFRClient(enable_cache=False)

        def fake_get(_path: str, **_kwargs: Any) -> _StubResponse:
            return _StubResponse(text=html)

        client.get = fake_get  # type: ignore[assignment]
        return client


if __name__ == "__main__":
    unittest.main()

