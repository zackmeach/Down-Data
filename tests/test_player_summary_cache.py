import polars as pl

from down_data.data import player_summary_cache


def test_merge_with_impacts_adds_totals_and_impacts():
    base = pl.DataFrame(
        {
            "player_id": ["p1"],
            "player_name": ["Test Player"],
            "team": ["ABC"],
            "season": [2020],
            "games_played": [16],
            "offense_snaps": [600],
            "defense_snaps": [20],
            "special_teams_snaps": [10],
            "passing_tds": [20],
            "rushing_tds": [5],
            "receiving_tds": [0],
        }
    )
    impacts = pl.DataFrame(
        {
            "player_id": ["p1"],
            "season": [2020],
            "qb_epa": [12.5],
            "qb_wpa": [1.4],
            "skill_epa": [0.0],
        }
    )

    merged = player_summary_cache._merge_with_impacts(base, impacts)
    row = merged.row(0, named=True)

    assert row["snaps_total"] == 630
    assert row["total_touchdowns"] == 25
    assert row["qb_epa"] == 12.5
    assert row["qb_wpa"] == 1.4
    # Missing columns still default to zero via fill_null
    assert row["skill_epa"] == 0.0


def test_filter_by_seasons_limits_rows():
    frame = pl.DataFrame({"season": [2019, 2020, 2021], "value": [1, 2, 3]})
    filtered = player_summary_cache._filter_by_seasons(frame, seasons=[2020, 2021])
    assert filtered["season"].to_list() == [2020, 2021]


