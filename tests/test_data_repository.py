from __future__ import annotations

import pandas as pd

from data.storage import DataRepository


def test_write_and_read_roundtrip(tmp_path):
    repo = DataRepository(root=tmp_path)
    df = pd.DataFrame({"player": ["Test Player"], "yards": [100]})
    repo.write_dataframe(df, "raw", "nfl", "2023", "weekly")

    latest = repo.latest_path("raw", "nfl", "2023", "weekly")
    assert latest.exists()

    roundtrip = repo.read_dataframe("raw", "nfl", "2023", "weekly")
    pd.testing.assert_frame_equal(df, roundtrip)

    versions = repo.list_versions("raw", "nfl", "2023", "weekly")
    assert versions and versions[-1].suffix == ".parquet"
