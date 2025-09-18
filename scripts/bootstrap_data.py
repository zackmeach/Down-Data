"""Simple CLI to ingest NFL data into the local lakehouse."""

from __future__ import annotations

import argparse
from typing import Callable

import pandas as pd

from data import DataPipeline, DataRepository, DuckDBWarehouse


TRANSFORMS: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    "weekly": lambda df: df,  # placeholder hook for user-defined cleaning
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("season", type=int, help="Season to ingest, e.g. 2023")
    parser.add_argument(
        "--league",
        default="nfl",
        help="League identifier used for partitioning Parquet data (default: nfl)",
    )
    parser.add_argument(
        "--dataset",
        default="weekly",
        choices=["weekly", "schedules", "seasonal", "pbp", "rosters"],
        help="Which dataset loader to run",
    )
    parser.add_argument(
        "--materialize",
        metavar="SQL",
        help="Optional DuckDB SQL to materialise into the modeled zone",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repository = DataRepository()
    warehouse = DuckDBWarehouse(repository)
    pipeline = DataPipeline(repository, warehouse)

    raw_df = pipeline.ingest_raw(args.dataset, args.league, args.season)
    transform = TRANSFORMS.get(args.dataset)
    if transform is not None:
        pipeline.promote_to_clean(args.dataset, args.league, args.season, transform)

    if args.materialize:
        pipeline.build_modeled_dataset(
            f"{args.dataset}_modeled",
            args.league,
            args.season,
            args.materialize,
        )

    print(
        f"Ingested {len(raw_df):,} rows of {args.dataset} data for {args.season} into {repository.root}"
    )


if __name__ == "__main__":
    main()
