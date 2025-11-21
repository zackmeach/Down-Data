"""Build the local basic offensive stats cache."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from down_data.data.basic_offense import build_basic_offense_cache, CACHE_PATH


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Rebuild the cache even if it already exists.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging for debugging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    configure_logging(args.verbose)

    try:
        frame = build_basic_offense_cache(force_refresh=args.refresh)
    except Exception as exc:  # pragma: no cover - CLI surface
        logging.getLogger(__name__).exception("Failed to build cache: %s", exc)
        return 1

    logging.info("Cache ready with %s rows at %s", frame.height, CACHE_PATH)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI hook
    raise SystemExit(main())

