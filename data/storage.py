"""Persistent storage helpers for the Down Data application."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

_STAGE_NAMES = ("raw", "clean", "modeled")


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S")


@dataclass
class DataRepository:
    """Manage the Parquet based data lake that powers the application.

    Parquet datasets are versioned and partitioned by ``league`` and ``season``
    following the ``raw → clean → modeled`` lifecycle described in the project
    requirements.
    """

    root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "var" / "data"
    )

    def __post_init__(self) -> None:
        for stage in _STAGE_NAMES:
            (self.root / stage).mkdir(parents=True, exist_ok=True)
        self.warehouse_path.mkdir(parents=True, exist_ok=True)

    @property
    def warehouse_path(self) -> Path:
        """Directory that contains the DuckDB cache file."""

        return self.root / "warehouse"

    @property
    def duckdb_file(self) -> Path:
        return self.warehouse_path / "warehouse.duckdb"

    def _stage_root(self, stage: str) -> Path:
        if stage not in _STAGE_NAMES:
            raise ValueError(f"Unknown stage '{stage}'. Expected one of {_STAGE_NAMES}.")
        return self.root / stage

    def _dataset_dir(self, stage: str, league: str, season: str, dataset: str) -> Path:
        league_dir = league.lower().replace(" ", "_")
        dataset_dir = (
            self._stage_root(stage)
            / league_dir
            / f"season={season}"
            / dataset.lower().replace(" ", "_")
        )
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return dataset_dir

    def _version_filename(self, dataset: str, version: Optional[str]) -> str:
        base = dataset.lower().replace(" ", "_")
        if version:
            return f"{base}-{version}.parquet"
        return f"{base}.parquet"

    def write_dataframe(
        self,
        df: pd.DataFrame,
        stage: str,
        league: str,
        season: str,
        dataset: str,
        *,
        versioned: bool = True,
    ) -> Path:
        """Persist a dataframe to a Parquet dataset, optionally versioned."""

        dataset_dir = self._dataset_dir(stage, league, season, dataset)
        version = _timestamp() if versioned else None
        filename = self._version_filename(dataset, version)
        path = dataset_dir / filename
        df.to_parquet(path, index=False)

        if versioned:
            latest_path = dataset_dir / self._version_filename(dataset, None)
            df.to_parquet(latest_path, index=False)

        return path

    def list_versions(
        self, stage: str, league: str, season: str, dataset: str
    ) -> List[Path]:
        dataset_dir = self._dataset_dir(stage, league, season, dataset)
        files = [p for p in dataset_dir.glob("*.parquet") if "-" in p.name]
        return sorted(files)

    def latest_path(self, stage: str, league: str, season: str, dataset: str) -> Path:
        dataset_dir = self._dataset_dir(stage, league, season, dataset)
        latest = dataset_dir / self._version_filename(dataset, None)
        if latest.exists():
            return latest
        versions = self.list_versions(stage, league, season, dataset)
        if not versions:
            raise FileNotFoundError(
                f"No parquet files found for {stage}/{league}/{season}/{dataset}."
            )
        return versions[-1]

    def read_dataframe(
        self,
        stage: str,
        league: str,
        season: str,
        dataset: str,
        version: Optional[str] = None,
        columns: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        if version is None:
            path = self.latest_path(stage, league, season, dataset)
        else:
            filename = self._version_filename(dataset, version)
            path = self._dataset_dir(stage, league, season, dataset) / filename
        return pd.read_parquet(path, columns=list(columns) if columns else None)


__all__ = ["DataRepository"]
