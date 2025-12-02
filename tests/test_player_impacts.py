import polars as pl
import pytest

from down_data.data import player_impacts


def _sample_pbp() -> pl.DataFrame:
    base = pl.DataFrame(
        {
            "season": [2020, 2020, 2020, 2020, 2020],
            "season_type": ["REG"] * 5,
            "game_id": ["G1"] * 5,
            "play_id": [1, 2, 3, 4, 5],
            "epa": [0.5, 0.2, -0.3, 0.1, -0.05],
            "wpa": [0.05, 0.02, -0.03, 0.01, -0.005],
            "qb_epa": [0.5, 0.2, 0.0, None, None],
            "qb_wpa": [0.05, 0.02, 0.0, None, None],
            "passer_player_id": ["QB1", "QB1", None, None, None],
            "rusher_player_id": ["RB1", "QB1", "RB1", None, None],
            "receiver_player_id": ["WR1", None, None, None, None],
            "yards_gained": [25, 5, 30, 0, 45],
            "complete_pass": [1, 0, 0, 0, 0],
            "first_down": [1, 0, 0, 0, 0],
            "solo_tackle_1_player_id": ["DEF1", "DEF2", "DEF1", None, None],
            "penalty_player_id": [None, "OL1", None, None, None],
            "kicker_player_id": [None, None, None, "K1", None],
            "punter_player_id": [None, None, None, None, "P1"],
        }
    )
    return base.with_row_count("_pbp_row_id")


def test_aggregate_player_impacts_returns_expected_metrics():
    frame = _sample_pbp()
    aggregated = player_impacts.aggregate_player_impacts(frame)

    qb_row = aggregated.filter(pl.col("player_id") == "QB1").row(0, named=True)
    assert pytest.approx(qb_row["qb_epa"]) == 0.7
    assert pytest.approx(qb_row["qb_wpa"]) == 0.07

    wr_row = aggregated.filter(pl.col("player_id") == "WR1").row(0, named=True)
    assert pytest.approx(wr_row["skill_epa"]) == 0.5
    assert pytest.approx(wr_row["skill_wpa"]) == 0.05
    assert wr_row["skill_rec_20_plus"] == 1
    assert wr_row["skill_rush_20_plus"] == 0
    assert wr_row["skill_rec_first_downs"] == 1

    rb_row = aggregated.filter(pl.col("player_id") == "RB1").row(0, named=True)
    assert pytest.approx(rb_row["skill_epa"]) == -0.3
    assert rb_row["skill_rush_20_plus"] == 1

    def_row = aggregated.filter(pl.col("player_id") == "DEF1").row(0, named=True)
    assert pytest.approx(def_row["def_epa"]) == 0.2
    assert pytest.approx(def_row["def_wpa"]) == 0.02

    ol_row = aggregated.filter(pl.col("player_id") == "OL1").row(0, named=True)
    assert pytest.approx(ol_row["ol_epa"]) == 0.2

    kicker_row = aggregated.filter(pl.col("player_id") == "K1").row(0, named=True)
    assert pytest.approx(kicker_row["kicker_epa"]) == 0.1
    assert pytest.approx(kicker_row["kicker_wpa"]) == 0.01

    punter_row = aggregated.filter(pl.col("player_id") == "P1").row(0, named=True)
    assert pytest.approx(punter_row["punter_epa"]) == -0.05
    assert pytest.approx(punter_row["punter_wpa"]) == -0.005

