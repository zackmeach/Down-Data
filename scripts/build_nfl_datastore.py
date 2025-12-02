#!/usr/bin/env python3
"""Build or refresh the NFL Data Store with a beautiful Rich terminal UI.

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
import logging
import sys
import time
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Rich imports
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.align import Align
from rich import box

console = Console()

# Suppress logging to console (we'll use Rich instead)
logging.basicConfig(level=logging.WARNING, handlers=[logging.NullHandler()])

# PFR team slug mapping
TEAM_TO_PFR_SLUG: Mapping[str, str] = {
    "ARI": "crd", "ATL": "atl", "BAL": "rav", "BUF": "buf", "CAR": "car",
    "CHI": "chi", "CIN": "cin", "CLE": "cle", "DAL": "dal", "DEN": "den",
    "DET": "det", "GB": "gnb", "GNB": "gnb", "HOU": "htx", "IND": "clt",
    "JAC": "jax", "JAX": "jax", "KC": "kan", "KAN": "kan", "LAC": "sdg",
    "SD": "sdg", "LAR": "ram", "LA": "ram", "STL": "ram", "LV": "rai",
    "LVR": "rai", "OAK": "rai", "MIA": "mia", "MIN": "min", "NE": "nwe",
    "NO": "nor", "NYG": "nyg", "NYJ": "nyj", "PHI": "phi", "PIT": "pit",
    "SEA": "sea", "SF": "sfo", "SFO": "sfo", "TB": "tam", "TEN": "oti",
    "WAS": "was",
}

PFR_SNAP_MIN_SEASON = 2012


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build or refresh the NFL Data Store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        "--skip-snaps",
        action="store_true",
        help="Skip fetching snap counts from PFR (faster)",
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current data store status and exit",
    )
    
    parser.add_argument(
        "--bio-batch",
        type=int,
        default=100,
        help="Number of players to fetch bio data for (default: 100, use 0 for all)",
    )
    
    return parser.parse_args()


def create_header() -> Panel:
    """Create the header panel."""
    header_text = Text()
    header_text.append("🏈 ", style="bold")
    header_text.append("NFL DATA STORE BUILDER", style="bold cyan")
    header_text.append(" 🏈", style="bold")
    
    return Panel(
        Align.center(header_text),
        box=box.DOUBLE,
        style="cyan",
        padding=(0, 2),
    )


def create_config_table(
    seasons: list[int] | None,
    force: bool,
    skip_bio: bool,
    skip_impacts: bool,
    skip_snaps: bool,
) -> Table:
    """Create a table showing build configuration."""
    table = Table(
        title="Build Configuration",
        box=box.ROUNDED,
        show_header=False,
        title_style="bold white",
        border_style="dim cyan",
    )
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="white")
    
    if seasons:
        season_str = ", ".join(str(s) for s in sorted(seasons))
    else:
        season_str = "1999 - 2024 (Full Range)"
    
    table.add_row("📅 Seasons", season_str)
    table.add_row("🔄 Force Rebuild", "Yes" if force else "No")
    table.add_row("👤 Fetch Bio Data", "No (Skipped)" if skip_bio else "Yes")
    table.add_row("📊 Build Impacts", "No (Skipped)" if skip_impacts else "Yes")
    table.add_row("🏃 Fetch Snap Counts", "No (Skipped)" if skip_snaps else "Yes (PFR)")
    
    return table


def create_status_panel(store: Any) -> Panel:
    """Create a panel showing current data store status."""
    status = store.get_status()
    
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white", justify="right")
    
    table.add_row("📁 Location", str(store.data_dir))
    table.add_row("📋 Schema Version", status['schema_version'])
    table.add_row("📅 Season Range", status['season_range'])
    table.add_row("", "")
    table.add_row("👥 Players", f"{status['total_players']:,}")
    table.add_row("📈 Player Seasons", f"{status['total_player_seasons']:,}")
    table.add_row("⚡ Impact Records", f"{status['total_impacts']:,}")
    
    if status['unresolved_errors'] > 0:
        table.add_row("", "")
        table.add_row("⚠️  Errors", f"{status['unresolved_errors']}", style="yellow")
    
    return Panel(
        table,
        title="[bold white]Current Status[/]",
        border_style="green" if status['initialized'] else "yellow",
        box=box.ROUNDED,
    )


def create_results_panel(stats: dict[str, Any]) -> Panel:
    """Create a panel showing build results."""
    table = Table(box=box.SIMPLE, show_header=False)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="bold white", justify="right")
    
    table.add_row("👥 Players Added/Updated", f"{stats['players_added']:,}")
    table.add_row("📈 Player Seasons Added", f"{stats['player_seasons_added']:,}")
    table.add_row("⚡ Impact Records Added", f"{stats['impacts_added']:,}")
    table.add_row("👤 Bio Records Updated", f"{stats['bio_updated']:,}")
    table.add_row("🏃 Snap Count Teams", f"{stats.get('snap_teams_fetched', 0):,}")
    
    if stats['errors']:
        table.add_row("", "")
        table.add_row("⚠️  Errors", f"{len(stats['errors'])}", style="yellow")
    
    border_style = "green" if not stats['errors'] else "yellow"
    title = "[bold green]✓ Build Complete[/]" if not stats['errors'] else "[bold yellow]⚠ Build Complete with Errors[/]"
    
    return Panel(
        table,
        title=title,
        border_style=border_style,
        box=box.DOUBLE,
    )


class RichDataBuilder:
    """Data builder with Rich progress UI."""
    
    def __init__(
        self,
        store: Any,
        seasons: list[int] | None,
        force: bool,
        skip_bio: bool,
        skip_impacts: bool,
        skip_snaps: bool,
        bio_batch: int = 100,
    ):
        self.store = store
        self.seasons = seasons
        self.force = force
        self.skip_bio = skip_bio
        self.skip_impacts = skip_impacts
        self.skip_snaps = skip_snaps
        self.bio_batch = bio_batch
        
        self.stats = {
            "players_added": 0,
            "player_seasons_added": 0,
            "impacts_added": 0,
            "bio_updated": 0,
            "snap_teams_fetched": 0,
            "errors": [],
        }
    
    def run(self) -> dict[str, Any]:
        """Run the build with Rich progress display."""
        import polars as pl
        from down_data.data.nfl_datastore import (
            DEFAULT_SEASON_START,
            DEFAULT_SEASON_END,
            PLAYERS_SCHEMA,
            PLAYER_SEASONS_SCHEMA,
            PLAYER_IMPACTS_SCHEMA,
            _to_polars,
        )
        
        # Determine target seasons
        if self.seasons:
            target_seasons = sorted(self.seasons)
        else:
            target_seasons = list(range(DEFAULT_SEASON_START, DEFAULT_SEASON_END + 1))
        
        # Calculate total steps
        total_steps = 3  # init + players + seasons
        if not self.skip_impacts:
            total_steps += len(target_seasons)
        if not self.skip_snaps:
            total_steps += 1  # snap counts
        if not self.skip_bio:
            total_steps += 1
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40, complete_style="green", finished_style="green"),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=False,
        ) as progress:
            
            main_task = progress.add_task("[cyan]Building NFL Data Store...", total=total_steps)
            
            # Step 1: Initialize
            progress.update(main_task, description="[cyan]Initializing data store...")
            self.store.initialize(force=self.force)
            progress.advance(main_task)
            
            # Step 2: Build players
            progress.update(main_task, description="[cyan]Loading player directory...")
            try:
                players_added = self._build_players(target_seasons, progress)
                self.stats["players_added"] = players_added
            except Exception as exc:
                self.stats["errors"].append({"table": "players", "error": str(exc)})
                console.print(f"[red]Error building players: {exc}[/]")
            progress.advance(main_task)
            
            # Step 3: Build player seasons (without snaps first)
            progress.update(main_task, description="[cyan]Building player seasons...")
            try:
                seasons_added = self._build_player_seasons(target_seasons, progress)
                self.stats["player_seasons_added"] = seasons_added
            except Exception as exc:
                self.stats["errors"].append({"table": "player_seasons", "error": str(exc)})
                console.print(f"[red]Error building player seasons: {exc}[/]")
            progress.advance(main_task)
            
            # Step 4: Fetch snap counts from PFR (if not skipped)
            if not self.skip_snaps:
                progress.update(main_task, description="[cyan]Fetching snap counts from PFR...")
                try:
                    snap_teams = self._fetch_snap_counts(target_seasons, progress)
                    self.stats["snap_teams_fetched"] = snap_teams
                except Exception as exc:
                    self.stats["errors"].append({"table": "snap_counts", "error": str(exc)})
                    console.print(f"[yellow]Warning fetching snap counts: {exc}[/]")
                progress.advance(main_task)
            
            # Step 5: Build impacts (if not skipped)
            if not self.skip_impacts:
                impacts_task = progress.add_task(
                    "[yellow]Building impact metrics...",
                    total=len(target_seasons)
                )
                try:
                    impacts_added = self._build_player_impacts(target_seasons, progress, impacts_task)
                    self.stats["impacts_added"] = impacts_added
                except Exception as exc:
                    self.stats["errors"].append({"table": "player_impacts", "error": str(exc)})
                    console.print(f"[red]Error building impacts: {exc}[/]")
                progress.update(main_task, advance=len(target_seasons))
            
            # Step 6: Fetch bio data (if not skipped)
            if not self.skip_bio:
                progress.update(main_task, description="[cyan]Fetching bio data from PFR...")
                try:
                    bio_updated = self._update_bio_data(progress, batch_size=self.bio_batch)
                    self.stats["bio_updated"] = bio_updated
                except Exception as exc:
                    self.stats["errors"].append({"table": "bio", "error": str(exc)})
                    console.print(f"[yellow]Warning fetching bio data: {exc}[/]")
                progress.advance(main_task)
            
            # Update metadata
            metadata = self.store.load_metadata()
            metadata.season_start = min(target_seasons)
            metadata.season_end = max(target_seasons)
            self.store._save_metadata()
            
            progress.update(main_task, description="[green]✓ Build complete!")
        
        return self.stats
    
    def _build_players(self, seasons: Sequence[int], progress: Progress) -> int:
        """Build players table."""
        from nflreadpy import load_players, load_rosters, load_ff_playerids
        import polars as pl
        from down_data.data.nfl_datastore import PLAYERS_SCHEMA, _to_polars
        
        players_raw = _to_polars(load_players())
        
        try:
            rosters = _to_polars(load_rosters(seasons=list(seasons)))
        except Exception:
            rosters = pl.DataFrame()
        
        try:
            playerids = _to_polars(load_ff_playerids())
        except Exception:
            playerids = pl.DataFrame()
        
        if players_raw.height == 0:
            return 0
        
        # Create full_name if not present
        if "full_name" not in players_raw.columns:
            players_raw = players_raw.with_columns(
                pl.concat_str([
                    pl.col("first_name").fill_null(""),
                    pl.lit(" "),
                    pl.col("last_name").fill_null("")
                ]).str.strip_chars().alias("full_name")
            )
        
        # Normalize to schema
        col_mapping = {
            "gsis_id": "player_id",
            "display_name": "display_name",
            "first_name": "first_name",
            "last_name": "last_name",
            "birth_date": "birth_date",
            "college_name": "college",
            "height": "height",
            "weight": "weight",
            "position": "position",
            "position_group": "position_group",
        }
        
        available_cols = [c for c in col_mapping.keys() if c in players_raw.columns]
        selected = players_raw.select([
            pl.col(c).alias(col_mapping.get(c, c)) for c in available_cols
        ])
        
        if "full_name" in players_raw.columns and "full_name" not in selected.columns:
            selected = selected.with_columns(players_raw["full_name"])
        
        # Add ID crosswalk
        if playerids.height > 0 and "gsis_id" in playerids.columns:
            id_cols = ["pfr_id", "pff_id", "espn_id", "sportradar_id"]
            available_id_cols = ["gsis_id"] + [c for c in id_cols if c in playerids.columns]
            id_crosswalk = playerids.select(available_id_cols).unique(subset=["gsis_id"])
            selected = selected.join(
                id_crosswalk.rename({"gsis_id": "player_id"}),
                on="player_id",
                how="left",
            )
        
        # Add draft info
        for col in ["draft_year", "draft_round", "draft_pick", "draft_team", "esb_id", "otc_id"]:
            if col in players_raw.columns and col not in selected.columns:
                mapping_col = "gsis_id" if "gsis_id" in players_raw.columns else "player_id"
                if mapping_col in players_raw.columns:
                    temp = players_raw.select([
                        pl.col(mapping_col).alias("player_id"),
                        pl.col(col)
                    ]).unique(subset=["player_id"])
                    selected = selected.join(temp, on="player_id", how="left")
        
        # Add metadata
        selected = selected.with_columns([
            pl.lit(datetime.now()).alias("_last_updated"),
            pl.lit(False).alias("_bio_fetched"),
        ])
        
        # Ensure schema columns
        for col, dtype in PLAYERS_SCHEMA.items():
            if col not in selected.columns:
                selected = selected.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        # Filter valid
        selected = selected.filter(
            pl.col("player_id").is_not_null() & (pl.col("player_id") != "")
        )
        
        return self.store.upsert_players(selected)
    
    def _build_player_seasons(self, seasons: Sequence[int], progress: Progress) -> int:
        """Build player seasons table."""
        from nflreadpy import load_player_stats, load_rosters
        import polars as pl
        from down_data.data.nfl_datastore import PLAYER_SEASONS_SCHEMA, _to_polars
        
        stats_raw = _to_polars(load_player_stats(seasons=list(seasons)))
        if stats_raw.height == 0:
            return 0
        
        # Filter to regular season ONLY
        if "season_type" in stats_raw.columns:
            stats_raw = stats_raw.filter(pl.col("season_type") == "REG")
        
        # Filter to valid seasons only (no future seasons)
        if "season" in stats_raw.columns:
            stats_raw = stats_raw.filter(pl.col("season").is_in(list(seasons)))
        
        # Aggregate to player-season
        string_sources = {
            "team": ["recent_team", "team", "current_team_abbr"],
            "player_id": ["player_id", "gsis_id"],
            "position": ["position", "player_position"],
            "position_group": ["position_group", "player_position_group"],
        }
        
        numeric_sources = {
            "pass_completions": ["completions"],
            "pass_attempts": ["attempts"],
            "passing_yards": ["passing_yards"],
            "passing_tds": ["passing_tds"],
            "passing_ints": ["interceptions"],
            "sacks_taken": ["sacks_suffered"],
            "sack_yards": ["sack_yards_lost"],
            "rushing_attempts": ["carries", "rushing_attempts"],
            "rushing_yards": ["rushing_yards"],
            "rushing_tds": ["rushing_tds"],
            "receiving_targets": ["targets"],
            "receiving_receptions": ["receptions"],
            "receiving_yards": ["receiving_yards"],
            "receiving_tds": ["receiving_tds"],
            "total_fumbles": ["fumbles"],
            "fumbles_lost": ["fumbles_lost"],
            "def_tackles_solo": ["def_tackles_solo"],
            "def_tackle_assists": ["def_tackle_assists"],
            "def_sacks": ["def_sacks"],
            "def_interceptions": ["def_interceptions"],
            "def_pass_defended": ["def_pass_defended"],
            "def_tds": ["def_tds"],
            "def_forced_fumbles": ["def_fumbles_forced"],
            "def_qb_hits": ["def_qb_hits"],
        }
        
        string_exprs = []
        for target, sources in string_sources.items():
            available = [pl.col(s) for s in sources if s in stats_raw.columns]
            if available:
                string_exprs.append(pl.coalesce(available).fill_null("").alias(f"_{target}"))
            else:
                string_exprs.append(pl.lit("").alias(f"_{target}"))
        
        numeric_exprs = []
        for target, sources in numeric_sources.items():
            available = [
                pl.col(s).cast(pl.Float64, strict=False).fill_null(0.0)
                for s in sources if s in stats_raw.columns
            ]
            if available:
                numeric_exprs.append(pl.coalesce(available).alias(target))
            else:
                numeric_exprs.append(pl.lit(0.0).alias(target))
        
        prepared = stats_raw.with_columns(
            string_exprs + numeric_exprs + [
                pl.col("season").cast(pl.Int16),
                pl.col("week").cast(pl.Int16).alias("_week"),
            ]
        )
        
        agg_exprs = [
            pl.col("_week").n_unique().alias("games_played"),
        ] + [
            pl.col(target).sum().alias(target)
            for target in numeric_sources.keys()
        ]
        
        grouped = prepared.group_by(
            ["_player_id", "_position", "_position_group", "_team", "season"]
        ).agg(agg_exprs)
        
        result = grouped.rename({
            "_player_id": "player_id",
            "_position": "position",
            "_position_group": "position_group",
            "_team": "team",
        })
        
        # Add derived columns (snaps will be 0 initially, updated by snap fetch)
        result = result.with_columns([
            pl.lit(0).cast(pl.Int32).alias("offense_snaps"),
            pl.lit(0).cast(pl.Int32).alias("defense_snaps"),
            pl.lit(0).cast(pl.Int32).alias("special_teams_snaps"),
            pl.lit(0).cast(pl.Int32).alias("snaps_total"),
            (
                pl.col("passing_tds").fill_null(0) +
                pl.col("rushing_tds").fill_null(0) +
                pl.col("receiving_tds").fill_null(0)
            ).cast(pl.Int8).alias("total_touchdowns"),
            pl.lit(datetime.now()).alias("_last_updated"),
        ])
        
        # Ensure schema
        for col, dtype in PLAYER_SEASONS_SCHEMA.items():
            if col not in result.columns:
                result = result.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        return self.store.upsert_player_seasons(result.sort(["player_id", "season"]))
    
    def _fetch_snap_counts(self, seasons: Sequence[int], progress: Progress) -> int:
        """Fetch snap counts from PFR and merge into player_seasons."""
        import polars as pl
        from down_data.data.nfl_datastore import _to_polars
        
        # Only fetch for seasons 2012+
        snap_seasons = [s for s in seasons if s >= PFR_SNAP_MIN_SEASON]
        if not snap_seasons:
            return 0
        
        # Build ID mapping from the players table (more complete than rosters)
        players = self.store.load_players()
        if players.height == 0 or "player_id" not in players.columns or "pfr_id" not in players.columns:
            console.print("[yellow]Cannot fetch snap counts: no player ID mapping available[/]")
            return 0
        
        id_mapping = (
            players.select(["player_id", "pfr_id"])
            .filter(pl.col("player_id").is_not_null() & pl.col("pfr_id").is_not_null() & (pl.col("pfr_id") != ""))
            .unique(subset=["player_id"])
            .rename({"player_id": "gsis_id"})  # Keep consistent naming for join
        )
        
        if id_mapping.height == 0:
            console.print("[yellow]No player ID mappings found[/]")
            return 0
        
        console.print(f"[dim]Using {id_mapping.height} player ID mappings for snap count merge[/]")
        
        # Get unique team-seasons from player_seasons
        player_seasons = self.store.load_player_seasons()
        season_teams = (
            player_seasons.select(["season", "team"])
            .filter(
                pl.col("team").is_not_null() & 
                (pl.col("team") != "") &
                pl.col("season").is_in(snap_seasons)
            )
            .unique()
        )
        
        if season_teams.height == 0:
            return 0
        
        # Fetch snap counts from PFR
        from down_data.data.pfr.client import PFRClient
        from down_data.data.pfr.snap_counts import fetch_team_snap_counts
        
        snap_frames = []
        teams_fetched = 0
        
        with PFRClient(enable_cache=True, min_delay=1.5) as client:
            for season in snap_seasons:
                teams = season_teams.filter(pl.col("season") == season)["team"].unique().to_list()
                for team in teams:
                    slug = TEAM_TO_PFR_SLUG.get(team.upper())
                    if not slug:
                        continue
                    try:
                        snaps = fetch_team_snap_counts(client, team_slug=slug, season=season)
                        if snaps.height > 0:
                            snap_frames.append(snaps.with_columns([
                                pl.lit(team.upper()).alias("team"),
                                pl.lit(season).alias("season"),
                            ]))
                            teams_fetched += 1
                    except Exception as exc:
                        pass  # Skip failed teams
        
        if not snap_frames:
            return teams_fetched
        
        # Combine all snap data
        snap_all = pl.concat(snap_frames, how="vertical_relaxed").filter(
            pl.col("pfr_id").is_not_null() & (pl.col("pfr_id") != "")
        )
        
        # Join with ID mapping to get gsis_id
        snap_with_gsis = snap_all.join(
            id_mapping.rename({"gsis_id": "player_id"}),
            on="pfr_id",
            how="inner",
        )
        
        # Aggregate snap data by player-season-team
        snap_agg = (
            snap_with_gsis.group_by(["player_id", "season", "team"])
            .agg([
                pl.col("_snap_offense").sum().alias("offense_snaps"),
                pl.col("_snap_defense").sum().alias("defense_snaps"),
                pl.col("_snap_st").sum().alias("special_teams_snaps"),
            ])
        )
        
        # Update player_seasons with snap data
        existing = player_seasons
        
        # Join snap data
        merged = existing.join(
            snap_agg,
            on=["player_id", "season", "team"],
            how="left",
            suffix="_snap",
        )
        
        # Update snap columns
        merged = merged.with_columns([
            pl.coalesce([
                pl.col("offense_snaps_snap"),
                pl.col("offense_snaps")
            ]).fill_null(0).cast(pl.Int32).alias("offense_snaps"),
            pl.coalesce([
                pl.col("defense_snaps_snap"),
                pl.col("defense_snaps")
            ]).fill_null(0).cast(pl.Int32).alias("defense_snaps"),
            pl.coalesce([
                pl.col("special_teams_snaps_snap"),
                pl.col("special_teams_snaps")
            ]).fill_null(0).cast(pl.Int32).alias("special_teams_snaps"),
        ])
        
        # Recalculate total snaps
        merged = merged.with_columns([
            (
                pl.col("offense_snaps") +
                pl.col("defense_snaps") +
                pl.col("special_teams_snaps")
            ).cast(pl.Int32).alias("snaps_total"),
        ])
        
        # Drop temporary columns
        drop_cols = [c for c in merged.columns if c.endswith("_snap")]
        if drop_cols:
            merged = merged.drop(drop_cols)
        
        # Save updated data
        self.store._save_player_seasons(merged)
        
        return teams_fetched
    
    def _build_player_impacts(
        self,
        seasons: Sequence[int],
        progress: Progress,
        task: Any,
    ) -> int:
        """Build player impacts table."""
        from nflreadpy import load_pbp
        import polars as pl
        from down_data.data.nfl_datastore import PLAYER_IMPACTS_SCHEMA, _to_polars
        
        aggregated_frames = []
        
        for season in seasons:
            progress.update(task, description=f"[yellow]Processing {season} play-by-play...")
            
            try:
                pbp = _to_polars(load_pbp(seasons=[season]))
                if pbp.is_empty():
                    progress.advance(task)
                    continue
                
                if "season_type" in pbp.columns:
                    pbp = pbp.filter(pl.col("season_type").str.to_uppercase() == "REG")
                
                if pbp.is_empty():
                    progress.advance(task)
                    continue
                
                frames = []
                
                # QB impacts - only if columns exist
                if "passer_player_id" in pbp.columns and "qb_epa" in pbp.columns:
                    qb_agg = [pl.col("qb_epa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("qb_epa")]
                    if "qb_wpa" in pbp.columns:
                        qb_agg.append(pl.col("qb_wpa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("qb_wpa"))
                    
                    qb_data = (
                        pbp.filter(pl.col("passer_player_id").is_not_null())
                        .group_by("passer_player_id")
                        .agg(qb_agg)
                        .rename({"passer_player_id": "player_id"})
                        .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                    )
                    if qb_data.height > 0:
                        frames.append(qb_data)
                
                # Skill impacts (rusher/receiver) - use general epa/wpa
                if "epa" in pbp.columns:
                    for player_col in ["rusher_player_id", "receiver_player_id"]:
                        if player_col not in pbp.columns:
                            continue
                        
                        skill_agg = [pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("skill_epa")]
                        if "wpa" in pbp.columns:
                            skill_agg.append(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("skill_wpa"))
                        
                        skill_data = (
                            pbp.filter(pl.col(player_col).is_not_null())
                            .group_by(player_col)
                            .agg(skill_agg)
                            .rename({player_col: "player_id"})
                            .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                        )
                        if skill_data.height > 0:
                            frames.append(skill_data)
                
                # Defensive impacts
                if "epa" in pbp.columns:
                    def_cols = ["solo_tackle_1_player_id", "interception_player_id", "sack_player_id"]
                    for player_col in def_cols:
                        if player_col not in pbp.columns:
                            continue
                        
                        def_agg = [pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("def_epa")]
                        if "wpa" in pbp.columns:
                            def_agg.append(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("def_wpa"))
                        
                        def_data = (
                            pbp.filter(pl.col(player_col).is_not_null())
                            .group_by(player_col)
                            .agg(def_agg)
                            .rename({player_col: "player_id"})
                            .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                        )
                        if def_data.height > 0:
                            frames.append(def_data)
                
                # Kicker impacts
                if "kicker_player_id" in pbp.columns and "epa" in pbp.columns:
                    kick_agg = [pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("kicker_epa")]
                    if "wpa" in pbp.columns:
                        kick_agg.append(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("kicker_wpa"))
                    
                    kicker_data = (
                        pbp.filter(pl.col("kicker_player_id").is_not_null())
                        .group_by("kicker_player_id")
                        .agg(kick_agg)
                        .rename({"kicker_player_id": "player_id"})
                        .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                    )
                    if kicker_data.height > 0:
                        frames.append(kicker_data)
                
                # Punter impacts
                if "punter_player_id" in pbp.columns and "epa" in pbp.columns:
                    punt_agg = [pl.col("epa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("punter_epa")]
                    if "wpa" in pbp.columns:
                        punt_agg.append(pl.col("wpa").cast(pl.Float64, strict=False).fill_null(0.0).sum().alias("punter_wpa"))
                    
                    punter_data = (
                        pbp.filter(pl.col("punter_player_id").is_not_null())
                        .group_by("punter_player_id")
                        .agg(punt_agg)
                        .rename({"punter_player_id": "player_id"})
                        .with_columns(pl.lit(season).cast(pl.Int16).alias("season"))
                    )
                    if punter_data.height > 0:
                        frames.append(punter_data)
                
                if frames:
                    # Merge all frames for this season
                    merged = frames[0]
                    for frame in frames[1:]:
                        new_cols = [c for c in frame.columns if c not in {"player_id", "season"} and c not in merged.columns]
                        if new_cols:
                            merged = merged.join(
                                frame.select(["player_id", "season"] + new_cols),
                                on=["player_id", "season"],
                                how="full",
                                coalesce=True,
                            )
                        else:
                            # Same columns - need to aggregate
                            common_cols = [c for c in frame.columns if c not in {"player_id", "season"}]
                            if common_cols:
                                # Just concat and group
                                merged = pl.concat([merged, frame], how="diagonal_relaxed")
                                agg_exprs = [pl.col(c).sum().alias(c) for c in merged.columns if c not in {"player_id", "season"}]
                                merged = merged.group_by(["player_id", "season"]).agg(agg_exprs)
                    
                    merged = merged.filter(pl.col("player_id").is_not_null() & (pl.col("player_id") != ""))
                    if merged.height > 0:
                        aggregated_frames.append(merged)
                        console.print(f"[dim green]  ✓ {season}: {merged.height} impact records[/]")
                
            except Exception as exc:
                console.print(f"[yellow]Warning: Failed to process {season}: {exc}[/]")
            
            progress.advance(task)
        
        if not aggregated_frames:
            return 0
        
        combined = pl.concat(aggregated_frames, how="diagonal_relaxed")
        combined = combined.with_columns(pl.lit(datetime.now()).alias("_last_updated"))
        
        for col, dtype in PLAYER_IMPACTS_SCHEMA.items():
            if col not in combined.columns:
                combined = combined.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        return self.store.upsert_player_impacts(combined)
    
    def _update_bio_data(self, progress: Progress, batch_size: int = 100) -> int:
        """Update bio data for players."""
        import polars as pl
        
        players = self.store.load_players()
        
        missing = players.filter(
            (pl.col("_bio_fetched").is_null() | (pl.col("_bio_fetched") == False)) &
            pl.col("pfr_id").is_not_null() &
            (pl.col("pfr_id") != "")
        )
        
        if missing.height == 0:
            console.print("[dim]All players already have bio data fetched.[/]")
            return 0
        
        console.print(f"[dim]{missing.height} players missing bio data, fetching up to {batch_size}...[/]")
        
        # If batch_size is 0, fetch all
        to_fetch = missing if batch_size == 0 else missing.head(batch_size)
        updated_count = 0
        rate_limited = False
        
        try:
            from down_data.data.pfr.client import PFRClient
            from down_data.data.pfr.players import fetch_player_bio_fields
            from requests import HTTPError
            
            with PFRClient(enable_cache=True, min_delay=1.0) as client:
                for i, row in enumerate(to_fetch.iter_rows(named=True)):
                    pfr_id = row.get("pfr_id")
                    player_id = row.get("player_id")
                    
                    if not pfr_id or not player_id:
                        continue
                    
                    try:
                        bio = fetch_player_bio_fields(client, pfr_id)
                        if bio:
                            self.store.update_player_bio(player_id, {
                                "handedness": bio.get("handedness", "N/A"),
                                "birth_city": bio.get("birth_city", "N/A"),
                                "birth_state": bio.get("birth_state", "N/A"),
                                "birth_country": bio.get("birth_country", "N/A"),
                            })
                            updated_count += 1
                            
                            # Progress indicator every 10 players
                            if updated_count % 10 == 0:
                                console.print(f"[dim]  Bio: {updated_count} fetched...[/]")
                                
                    except HTTPError as exc:
                        if getattr(exc.response, "status_code", None) == 429:
                            console.print(f"[yellow]Rate limited after {updated_count} bio fetches. Run again later to continue.[/]")
                            rate_limited = True
                            break
                    except Exception as exc:
                        pass  # Skip failed players
                        
        except ImportError:
            console.print("[yellow]PFR client not available for bio fetching.[/]")
        
        if not rate_limited and updated_count > 0:
            console.print(f"[dim green]  ✓ Fetched bio for {updated_count} players[/]")
        
        return updated_count


def show_status(store: Any) -> None:
    """Display current data store status with Rich."""
    console.print()
    console.print(create_header())
    console.print()
    
    if not store.players_path.exists():
        console.print(Panel(
            "[yellow]Data store not initialized.[/]\n\n"
            "Run without [cyan]--status[/] to build the data store.",
            title="[bold yellow]⚠ Not Initialized[/]",
            border_style="yellow",
        ))
        return
    
    console.print(create_status_panel(store))
    
    # Show last updated times
    status = store.get_status()
    table = Table(
        title="Last Updated",
        box=box.ROUNDED,
        border_style="dim",
    )
    table.add_column("Table", style="cyan")
    table.add_column("Timestamp", style="white")
    
    for table_name, timestamp in status['last_updated'].items():
        table.add_row(table_name.replace("_", " ").title(), timestamp or "Never")
    
    console.print(table)
    console.print()


def main() -> int:
    args = parse_args()
    
    from down_data.data.nfl_datastore import get_default_store
    
    store = get_default_store()
    
    # Status-only mode
    if args.status:
        show_status(store)
        return 0
    
    # Build mode
    console.print()
    console.print(create_header())
    console.print()
    
    # Show configuration
    console.print(create_config_table(
        args.seasons,
        args.force,
        args.skip_bio,
        args.skip_impacts,
        args.skip_snaps,
    ))
    console.print()
    
    # Show current status if exists
    if store.players_path.exists() and not args.force:
        console.print(create_status_panel(store))
        console.print()
    
    # Confirm
    console.print("[dim]Starting build process...[/]")
    console.print()
    
    start_time = time.time()
    
    try:
        builder = RichDataBuilder(
            store,
            args.seasons,
            args.force,
            args.skip_bio,
            args.skip_impacts,
            args.skip_snaps,
            args.bio_batch,
        )
        stats = builder.run()
        
        elapsed = time.time() - start_time
        
        console.print()
        console.print(create_results_panel(stats))
        console.print()
        
        # Show final status
        console.print(create_status_panel(store))
        
        # Show timing
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        console.print(f"[dim]Total time: {minutes}m {seconds}s[/]")
        console.print()
        
        return 0 if not stats['errors'] else 1
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Build interrupted by user.[/]")
        return 1
    except Exception as exc:
        console.print(f"\n[red bold]❌ Build failed:[/] {exc}")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/]")
        return 1


if __name__ == "__main__":
    sys.exit(main())
