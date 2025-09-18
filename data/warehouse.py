"""DuckDB caching helpers for analytical queries."""

from __future__ import annotations

from typing import Iterable, Mapping, Optional

import duckdb
import pandas as pd

from .storage import DataRepository


class DuckDBWarehouse:
    """Thin wrapper around DuckDB for query caching and materialization."""

    def __init__(self, repository: DataRepository) -> None:
        self.repository = repository
        self._connection = duckdb.connect(repository.duckdb_file.as_posix())
        self._connection.execute("PRAGMA threads=system;")

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None  # type: ignore[assignment]

    def register_parquet_view(
        self,
        stage: str,
        league: str,
        season: str,
        dataset: str,
        *,
        view_name: Optional[str] = None,
    ) -> str:
        path = self.repository.latest_path(stage, league, season, dataset)
        view = view_name or f"{stage}_{league}_{season}_{dataset}".replace("-", "_")
        sql = (
            "CREATE OR REPLACE VIEW {view} AS SELECT * FROM parquet_scan('{path}');"
        ).format(view=view, path=path.as_posix())
        self._connection.execute(sql)
        return view

    def query(
        self, sql: str, params: Optional[Mapping[str, object]] = None
    ) -> pd.DataFrame:
        if params is None:
            result = self._connection.execute(sql)
        else:
            result = self._connection.execute(sql, params)
        return result.fetch_df()

    def materialize(self, name: str, sql: str) -> None:
        self._connection.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")

    def drop_objects(self, names: Iterable[str]) -> None:
        for name in names:
            self._connection.execute(f"DROP VIEW IF EXISTS {name}")
            self._connection.execute(f"DROP TABLE IF EXISTS {name}")

    def invalidate_dependents(self, names: Iterable[str]) -> None:
        self.drop_objects(names)

    def __del__(self) -> None:  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:
            pass


__all__ = ["DuckDBWarehouse"]
