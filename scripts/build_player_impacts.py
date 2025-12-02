"""CLI helper to pre-compute the seasonal player impact cache."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from down_data.data import player_impacts

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the seasonal player impact cache.")
    parser.add_argument(
        "--seasons",
        nargs="*",
        type=int,
        default=None,
        help="Specific seasons to rebuild (defaults to the full 1999-2024 range).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force a rebuild even when the cache already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seasons: Iterable[int] | None = args.seasons if args.seasons else None

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info(
        "Starting player impact cache build (force=%s, seasons=%s).",
        args.force,
        seasons or "full range",
    )
    player_impacts.build_player_impacts_cache(seasons=seasons, force_refresh=args.force)


if __name__ == "__main__":
    main()


