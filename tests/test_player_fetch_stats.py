"""Unit tests for the Player.fetch_stats helper."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import polars as pl

from down_data.core.player import Player


class PlayerFetchStatsTestCase(unittest.TestCase):
    """Validate robustness of Player.fetch_stats across backend behaviours."""

    def setUp(self) -> None:
        self._player_row = {
            "gsis_id": "00TEST",
            "full_name": "Test Player",
            "display_name": "Test Player",
            "position": "QB",
            "position_group": "QB",
        }
        finder_patch = patch(
            "down_data.core.player.PlayerFinder.resolve",
            return_value=self._player_row,
        )
        self.addCleanup(finder_patch.stop)
        finder_patch.start()
        self.player = Player(name="Test Player")

    def test_fetch_stats_falls_back_when_summary_level_not_supported(self) -> None:
        """Ensure a TypeError for summary_level does not break stat loading."""

        data = pl.DataFrame(
            {
                "player_id": ["00TEST"],
                "season": [2023],
                "season_type": ["REG"],
                "games": [1],
            }
        )

        def fake_loader(**kwargs):
            if "summary_level" in kwargs:
                raise TypeError("unexpected keyword argument 'summary_level'")
            return data

        with patch("down_data.core.player.load_player_stats", side_effect=fake_loader):
            result = self.player.fetch_stats(seasons=True)

        self.assertEqual(result.height, 1)
        self.assertListEqual(result["season"].to_list(), [2023])

    def test_fetch_stats_filters_by_display_name_when_player_id_missing(self) -> None:
        """Verify name-based fallback when player_id column is unavailable."""

        data = pl.DataFrame(
            {
                "player_display_name": ["Test Player"],
                "season": [2022],
                "season_type": ["REG"],
                "games": [5],
            }
        )

        with patch("down_data.core.player.load_player_stats", return_value=data):
            result = self.player.fetch_stats(seasons=True)

        self.assertEqual(result.height, 1)
        self.assertListEqual(result["season"].to_list(), [2022])

    def test_fetch_stats_applies_season_type_filter(self) -> None:
        """Ensure season_type filtering removes non-matching rows."""

        data = pl.DataFrame(
            {
                "player_id": ["00TEST", "00TEST"],
                "season_type": ["REG", "POST"],
                "season": [2021, 2021],
            }
        )

        with patch("down_data.core.player.load_player_stats", return_value=data):
            result = self.player.fetch_stats(seasons=True, season_type="REG")

        self.assertEqual(result.height, 1)
        self.assertListEqual(result["season_type"].to_list(), ["REG"])


if __name__ == "__main__":
    unittest.main()

