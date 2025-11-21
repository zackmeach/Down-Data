from __future__ import annotations

from datetime import date
import math
import unittest

import polars as pl

from PySide6.QtWidgets import QApplication

from down_data.ui.pages.player_detail_page import PlayerDetailPage


class _StubProfile:
    def __init__(self) -> None:
        self.full_name = "Test Quarterback"
        self.position = "QB"
        self.position_group = "QB"
        self.birth_date = date(1996, 12, 10)
        self.gsis_id = "00TEST"


class _StubPlayer:
    def __init__(self) -> None:
        self.profile = _StubProfile()

    def is_defensive(self) -> bool:
        return False


class _StubService:
    def __init__(self, cached_df: pl.DataFrame) -> None:
        self._cached = cached_df

    def load_player(self, query) -> _StubPlayer:
        return _StubPlayer()

    def get_basic_offense_stats(
        self,
        *,
        player_id: str | None = None,
        player_ids=None,
        seasons=None,
        team=None,
        position=None,
        refresh_cache: bool = False,
    ) -> pl.DataFrame:
        return self._cached

    def get_basic_ratings(self, player, *, summary, is_defensive):
        return []

    def get_team_record(self, team: str | None, season: int, *, season_type: str = "REG") -> tuple[int, int, int]:
        return (10, 7, 0)

    def get_player_stats(self, player, **kwargs) -> pl.DataFrame:
        return pl.DataFrame()


class PlayerDetailQuarterbackTableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._app = QApplication.instance() or QApplication([])

    def test_qb_table_rows_include_expected_metrics(self) -> None:
        cached = pl.DataFrame(
            {
                "player_id": ["00TEST"],
                "player_name": ["Test Quarterback"],
                "position": ["QB"],
                "position_group": ["QB"],
                "team": ["CIN"],
                "season": [2024],
                "games_played": [2],
                "pass_completions": [45],
                "pass_attempts": [65],
                "passing_yards": [550],
                "passing_tds": [5],
                "passing_ints": [1],
                "sacks_taken": [3],
                "sack_yards": [23],
                "rushing_attempts": [7],
                "rushing_yards": [45],
                "rushing_tds": [1],
                "receiving_targets": [0],
                "receiving_receptions": [0],
                "receiving_yards": [0],
                "receiving_tds": [0],
                "total_fumbles": [2],
                "fumbles_lost": [1],
            }
        )

        service = _StubService(cached)
        page = PlayerDetailPage(service=service)
        payload = {"full_name": "Test Quarterback", "team": "CIN", "position": "QB"}
        page.display_player(payload)

        self.assertEqual(len(page._season_rows), 1)
        row = page._season_rows[0]

        expected_columns = [
            "Season",
            "Age",
            "Team",
            "Team Record",
            "Games Played",
            "Snaps Played",
            "QB Rating",
            "Total Touchdowns",
            "Total Turnovers",
            "Total Yards",
            "Completions",
            "Attempts",
            "Completion Percentage",
            "Yards",
            "Touchdowns",
            "Touchdown %",
            "Interceptions",
            "Interceptions %",
            "Yards/Attempt",
            "Yards/Completion",
            "Sacks Taken",
            "Sack %",
            "Sack Yards",
        ]

        self.assertEqual(len(row), len(expected_columns))

        season_index = 0
        age_index = 1
        team_record_index = 3
        qb_rating_index = 6
        total_td_index = 7
        completion_pct_index = 12

        self.assertEqual(row[season_index], "2024")
        self.assertEqual(row[age_index], "27")  # Age as of Sept 1, 2024
        self.assertEqual(row[team_record_index], "10-7")
        self.assertEqual(row[total_td_index], "6")
        self.assertEqual(row[expected_columns.index("Snaps Played")], "75")
        self.assertEqual(row[expected_columns.index("Total Yards")], "595")
        self.assertEqual(row[expected_columns.index("Sack Yards")], "-23")

        qb_rating = float(row[qb_rating_index])
        completion_pct = float(row[completion_pct_index])

        expected_rating = page._calculate_passer_rating(45.0, 65.0, 550.0, 5.0, 1.0)
        expected_completion = 45.0 / 65.0 * 100.0

        self.assertTrue(math.isclose(qb_rating, expected_rating, rel_tol=0.01))
        self.assertTrue(math.isclose(completion_pct, expected_completion, rel_tol=0.01))

        self.assertIsNotNone(page._current_player)

        _, summary_dict = page._build_table_rows_from_cached(page._current_player, cached)
        self.assertEqual(summary_dict["pass_yards"], 550.0)
        self.assertEqual(summary_dict["rush_yards"], 45.0)
        self.assertEqual(summary_dict["games"], 2.0)
        self.assertEqual(summary_dict["total_touchdowns"], 6.0)
        self.assertEqual(summary_dict["snaps"], 75.0)


if __name__ == "__main__":
    unittest.main()

