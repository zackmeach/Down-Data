#!/usr/bin/env python3
"""Build or refresh the NFL Data Store.

This script populates the structured NFL data store with player data from nflverse.
Unlike caches, this data store is designed to be persistent and well-organized.

Usage:
    python scripts/build_nfl_datastore.py                    # Full build (1999-2024)
    python scripts/build_nfl_datastore.py --seasons 2023 2024  # Specific seasons
    python scripts/build_nfl_datastore.py --force            # Force full rebuild
    python scripts/build_nfl_datastore.py --skip-bio         # Skip PFR bio scraping
    python scripts/build_nfl_datastore.py --skip-impacts     # Skip EPA/WPA (faster)
    python scripts/build_nfl_datastore.py --status           # Show current status
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from down_data.data.nfl_datastore import (
    NFLDataStore,
    NFLDataBuilder,
    get_default_store,
    DATA_DIRECTORY,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build or refresh the NFL Data Store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build full data store (1999-2024)
  python scripts/build_nfl_datastore.py

  # Update with just 2024 season data
  python scripts/build_nfl_datastore.py --seasons 2024

  # Force complete rebuild
  python scripts/build_nfl_datastore.py --force

  # Quick build without bio/impacts (for testing)
  python scripts/build_nfl_datastore.py --skip-bio --skip-impacts

  # Check current status
  python scripts/build_nfl_datastore.py --status
""",
    )
    
    parser.add_argument(
        "--seasons",
        nargs="*",
        type=int,
        default=None,
        help="Specific seasons to build (defaults to full 1999-2024 range)",
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force a complete rebuild, replacing existing data",
    )
    
    parser.add_argument(
        "--skip-bio",
        action="store_true",
        help="Skip fetching bio data from PFR (faster, less data)",
    )
    
    parser.add_argument(
        "--skip-impacts",
        action="store_true",
        help="Skip building EPA/WPA impact metrics (faster)",
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current data store status and exit",
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )
    
    return parser.parse_args()


def show_status(store: NFLDataStore) -> None:
    """Display current data store status."""
    status = store.get_status()
    
    print("\n" + "=" * 60)
    print("NFL DATA STORE STATUS")
    print("=" * 60)
    print(f"  Location:        {store.data_dir}")
    print(f"  Initialized:     {status['initialized']}")
    print(f"  Schema Version:  {status['schema_version']}")
    print(f"  Season Range:    {status['season_range']}")
    print()
    print("Record Counts:")
    print(f"  Players:         {status['total_players']:,}")
    print(f"  Player Seasons:  {status['total_player_seasons']:,}")
    print(f"  Impact Records:  {status['total_impacts']:,}")
    print()
    print("Last Updated:")
    for table, timestamp in status['last_updated'].items():
        print(f"  {table:17} {timestamp or 'Never'}")
    print()
    
    if status['unresolved_errors'] > 0:
        print(f"⚠️  Unresolved Errors: {status['unresolved_errors']}")
        metadata = store.load_metadata()
        for error in metadata.get_unresolved_errors()[:5]:
            print(f"     - {error['operation']}: {error['message'][:50]}...")
    else:
        print("✓ No unresolved errors")
    
    print("=" * 60 + "\n")


def main() -> int:
    args = parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    store = get_default_store()
    
    # Status-only mode
    if args.status:
        if not store.players_path.exists():
            print("\nData store not initialized. Run without --status to build.")
            return 1
        show_status(store)
        return 0
    
    # Build mode
    print("\n" + "=" * 60)
    print("NFL DATA STORE BUILD")
    print("=" * 60)
    
    seasons = args.seasons if args.seasons else None
    if seasons:
        print(f"  Seasons:     {', '.join(str(s) for s in sorted(seasons))}")
    else:
        print(f"  Seasons:     Full range (1999-2024)")
    
    print(f"  Force:       {args.force}")
    print(f"  Skip Bio:    {args.skip_bio}")
    print(f"  Skip Impacts:{args.skip_impacts}")
    print("=" * 60 + "\n")
    
    try:
        builder = NFLDataBuilder(store)
        stats = builder.build_all(
            seasons=seasons,
            force=args.force,
            skip_bio=args.skip_bio,
            skip_impacts=args.skip_impacts,
        )
        
        print("\n" + "=" * 60)
        print("BUILD COMPLETE")
        print("=" * 60)
        print(f"  Players Added:        {stats['players_added']:,}")
        print(f"  Player Seasons Added: {stats['player_seasons_added']:,}")
        print(f"  Impacts Added:        {stats['impacts_added']:,}")
        print(f"  Bio Records Updated:  {stats['bio_updated']:,}")
        
        if stats['errors']:
            print(f"\n⚠️  Errors encountered: {len(stats['errors'])}")
            for error in stats['errors']:
                print(f"     - {error['table']}: {error['error'][:50]}...")
        else:
            print("\n✓ Build completed successfully with no errors")
        
        print("=" * 60 + "\n")
        
        # Show final status
        show_status(store)
        
        return 0 if not stats['errors'] else 1
        
    except Exception as exc:
        logger.exception("Build failed: %s", exc)
        print(f"\n❌ Build failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
