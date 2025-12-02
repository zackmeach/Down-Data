"""Build the comprehensive cache used by player summary tables."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from down_data.data.player_summary_cache import (  # noqa: E402
    CACHE_PATH,
    build_player_summary_cache,
)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Rebuild every source cache before generating the summary parquet.",
    )
    parser.add_argument(
        "--season",
        type=int,
        dest="seasons",
        action="append",
        help="Restrict the build to specific seasons (repeat for multiple years).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging for troubleshooting.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    _configure_logging(args.verbose)

    try:
        frame = build_player_summary_cache(seasons=args.seasons, force_refresh=args.refresh)
    except Exception as exc:  # pragma: no cover - CLI surface
        logging.getLogger(__name__).exception("Failed to build player summary cache: %s", exc)
        return 1

    logging.info("Player summary cache ready with %s rows at %s", frame.height, CACHE_PATH)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())


