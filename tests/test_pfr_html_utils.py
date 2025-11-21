"""Unit tests for lightweight PFR HTML helpers."""

from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from down_data.data.pfr import (
    flatten_columns,
    list_table_ids,
    read_all_tables,
    read_commented_table_by_id,
    read_table_by_id,
)


FIXTURES = Path(__file__).resolve().parent / "data" / "pfr"


class PFRHtmlUtilsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.team_html = (FIXTURES / "team_page.html").read_text(encoding="utf-8")

    def test_read_all_tables_returns_tables(self) -> None:
        tables = read_all_tables(self.team_html)
        self.assertEqual(len(tables), 1)
        self.assertListEqual(list(tables[0].columns), ["Week", "Date", "Result", "Opp"])

    def test_read_table_by_id_handles_comment_wrapped_case(self) -> None:
        stats_df = flatten_columns(read_table_by_id(self.team_html, "team_stats"))
        self.assertEqual(stats_df.iloc[0]["Team Offense_Points For"], 454)

    def test_read_commented_table_by_id_raises_for_missing_table(self) -> None:
        with self.assertRaises(ValueError):
            read_commented_table_by_id(self.team_html, "nonexistent")

    def test_flatten_columns_resolves_multiindex(self) -> None:
        frame = pd.DataFrame(
            columns=pd.MultiIndex.from_product([["A", "B"], ["Foo", "Bar"]])
        )
        flattened = flatten_columns(frame)
        self.assertListEqual(flattened.columns.tolist(), ["A_Foo", "A_Bar", "B_Foo", "B_Bar"])

    def test_list_table_ids_discovers_comment_wrapped_tables(self) -> None:
        ids = list_table_ids(self.team_html)
        self.assertIn("games", ids)
        self.assertIn("team_stats", ids)


if __name__ == "__main__":
    unittest.main()

