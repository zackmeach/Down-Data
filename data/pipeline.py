"""ETL primitives built on top of :mod:`nfl_data_py`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Optional

import nfl_data_py as nfl
import pandas as pd

from .storage import DataRepository
from .warehouse import DuckDBWarehouse

DatasetLoader = Callable[[Iterable[int]], pd.DataFrame]

_DEFAULT_DATASETS: Mapping[str, str] = {
    "schedules": "import_schedules",
    "weekly": "import_weekly_data",
    "seasonal": "import_seasonal_data",
    "pbp": "import_pbp_data",
    "rosters": "import_roster_data",
}


@dataclass
class DataPipeline:
    """Coordinate data ingestion and modeling for the application."""

    repository: DataRepository
    warehouse: DuckDBWarehouse

    def _resolve_loader(
        self, dataset: str, loader: Optional[DatasetLoader] = None
    ) -> DatasetLoader:
        if loader is not None:
            return loader
        try:
            attr = _DEFAULT_DATASETS[dataset]
        except KeyError as exc:  # pragma: no cover - protective branch
            raise ValueError(
                f"Unknown dataset '{dataset}'. Provide a custom loader callable."
            ) from exc
        try:
            func = getattr(nfl, attr)
        except AttributeError as exc:  # pragma: no cover - library mismatch safety
            raise ValueError(
                f"The nfl_data_py module does not provide '{attr}'. "
                "Upgrade the dependency or supply a custom loader."
            ) from exc

        def _wrapper(seasons: Iterable[int]) -> pd.DataFrame:
            try:
                return func(list(seasons))
            except TypeError:
                try:
                    return func(seasons=list(seasons))
                except TypeError as err:  # pragma: no cover - user error guard
                    raise TypeError(
                        f"Could not call loader '{attr}'. Inspect the nfl_data_py signature."
                    ) from err

        return _wrapper

    def ingest_raw(
        self,
        dataset: str,
        league: str,
        season: int,
        *,
        loader: Optional[DatasetLoader] = None,
    ) -> pd.DataFrame:
        """Download a dataset from nfl_data_py and persist it to the raw zone."""

        seasons = [int(season)]
        loader_fn = self._resolve_loader(dataset, loader)
        df = loader_fn(seasons)
        self.repository.write_dataframe(df, "raw", league, str(season), dataset)
        return df

    def promote_to_clean(
        self,
        dataset: str,
        league: str,
        season: int,
        transform: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> pd.DataFrame:
        """Transform raw data into the clean layer and persist the result."""

        raw_df = self.repository.read_dataframe("raw", league, str(season), dataset)
        clean_df = transform(raw_df.copy())
        self.repository.write_dataframe(clean_df, "clean", league, str(season), dataset)
        self.warehouse.invalidate_dependents([f"{dataset}_clean_cache"])
        return clean_df

    def build_modeled_dataset(
        self,
        dataset: str,
        league: str,
        season: int,
        sql: str,
        *,
        cache_name: Optional[str] = None,
        invalidate: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """Execute a DuckDB query and persist the modeled result to Parquet."""

        result = self.warehouse.query(sql)
        self.repository.write_dataframe(result, "modeled", league, str(season), dataset)
        if cache_name:
            self.warehouse.materialize(cache_name, sql)
        if invalidate:
            self.warehouse.invalidate_dependents(invalidate)
        return result


__all__ = ["DataPipeline"]
