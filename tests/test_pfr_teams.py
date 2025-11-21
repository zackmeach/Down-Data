"""Tests for team-level scraping helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import types
import unittest

import polars as pl

from down_data.data.pfr.client import PFRClient
from down_data.data.pfr.teams import (
    fetch_team_schedule,
    fetch_team_stats,
    parse_schedule_table,
    parse_team_stats_table,
)


FIXTURES = Path(__file__).resolve().parent / "data" / "pfr"


class _StubResponse(types.SimpleNamespace):
    def raise_for_status(self) -> None:  # pragma: no cover - provided for compatibility
        return None


class PFRTeamsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.team_html = (FIXTURES / "team_page.html").read_text(encoding="utf-8")

    def test_parse_schedule_table_returns_polars_dataframe(self) -> None:
        frame = parse_schedule_table(self.team_html)
        self.assertIsInstance(frame, pl.DataFrame)
        self.assertListEqual(frame.columns, ["Week", "Date", "Result", "Opponent"])
        self.assertEqual(frame.height, 2)

    def test_parse_team_stats_table_handles_comment_wrapped_table(self) -> None:
        frame = parse_team_stats_table(self.team_html)
        self.assertEqual(frame["Team Offense_Points For"][0], 454)

    def test_fetch_team_schedule_annotates_team_and_season(self) -> None:
        client = self._make_stub_client(self.team_html)
        frame = fetch_team_schedule(client, "NE", 2025)
        self.assertIn("season", frame.columns)
        self.assertIn("team", frame.columns)
        self.assertSetEqual(set(frame["team"].to_list()), {"NE"})
        self.assertSetEqual(set(frame["season"].to_list()), {2025})

    def test_fetch_team_stats_adds_team_and_season(self) -> None:
        client = self._make_stub_client(self.team_html)
        frame = fetch_team_stats(client, "NE", 2025)
        self.assertIn("team", frame.columns)
        self.assertIn("season", frame.columns)
        self.assertEqual(frame["team"][0], "NE")
        self.assertEqual(frame["season"][0], 2025)

    def _make_stub_client(self, html: str) -> PFRClient:
        client = PFRClient(enable_cache=False)

        def fake_get(_path: str, **_kwargs: Any) -> _StubResponse:
            return _StubResponse(text=html)

        client.get = fake_get  # type: ignore[assignment]
        return client


if __name__ == "__main__":
    unittest.main()

