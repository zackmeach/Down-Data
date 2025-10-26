from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Sequence

import polars as pl

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from player import (
    Player,
    PlayerNotFoundError,
    SeasonNotAvailableError,
    EARLIEST_SEASON_AVAILABLE,
    LATEST_SEASON_AVAILABLE,
    EARLIEST_NEXTGEN_SEASON,
)

logging.basicConfig(level=logging.INFO)
console = Console()


def ask_optional_int(message: str) -> Optional[int]:
    while True:
        raw = Prompt.ask(message, default="").strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            console.print("[red]Please enter a valid integer or leave blank.[/red]")


def ask_optional_str(message: str) -> Optional[str]:
    raw = Prompt.ask(message, default="").strip()
    return raw or None


def parse_seasons(raw: str) -> List[int]:
    """Parse a comma-separated string of seasons into a de-duplicated list."""

    seasons: List[int] = []
    seen: set[int] = set()
    for chunk in (part.strip() for part in raw.split(",")):
        if not chunk:
            continue
        try:
            season = int(chunk)
        except ValueError:
            console.print(f"[yellow]Ignoring invalid season entry: {chunk}[/yellow]")
            continue
        if season not in seen:
            seasons.append(season)
            seen.add(season)
    return seasons


def _render_preview_table(
    *,
    title: str,
    preview_rows: pl.DataFrame,
    candidate_columns: Sequence[str],
    empty_message: str,
    missing_columns_message: str,
) -> Table:
    """Create a Rich table preview for a stats dataset."""

    table = Table(title=title)

    available_columns = [column for column in candidate_columns if column in preview_rows.columns]

    if not available_columns:
        table.add_column("message")
        table.add_row(missing_columns_message)
        return table

    for column in available_columns:
        table.add_column(column, overflow="fold")

    if preview_rows.height == 0:
        table.add_row(empty_message, *([""] * (len(available_columns) - 1)))
        return table

    for row in preview_rows.iter_rows(named=True):
        table.add_row(*(str(row.get(column, "")) for column in available_columns))

    return table


def build_stats_table(
    player: Player, seasons: Iterable[int], stats_columns: Optional[List[str]] = None
) -> Table:
    seasons_list = list(seasons)
    season_label = ", ".join(str(season) for season in seasons_list) or "all requested seasons"
    stats = player.fetch_stats(seasons=seasons_list)
    console.print(f"Loaded {stats.height} rows of stats for {player.profile.full_name}.")
    preview = stats.head(5)

    columns = stats_columns or player.get_relevant_stat_columns()

    return _render_preview_table(
        title=f"Stat preview for seasons {season_label}",
        preview_rows=preview,
        candidate_columns=columns,
        empty_message="No stats returned for the selected seasons.",
        missing_columns_message="No preview columns are available for the returned dataset.",
    )


def build_nextgen_stats_table(
    player: Player, seasons: Iterable[int], stat_type: Optional[str] = None
) -> Table:
    """Build a table showing NextGen advanced stats."""
    seasons_list = list(seasons)
    season_label = ", ".join(str(season) for season in seasons_list) or "all requested seasons"
    stat_type = stat_type or player.get_nextgen_stat_type()

    console.print(f"Loading NextGen {stat_type} stats...")
    nextgen_stats = player.fetch_nextgen_stats(seasons=seasons_list, stat_type=stat_type)
    console.print(f"Loaded {nextgen_stats.height} rows of NextGen stats.")

    stat_type_names = {
        "passing": "Passing",
        "rushing": "Rushing",
        "receiving": "Receiving",
    }
    title = f"NextGen {stat_type_names.get(stat_type, stat_type)} Stats - {season_label}"

    return _render_preview_table(
        title=title,
        preview_rows=nextgen_stats.head(5),
        candidate_columns=player.get_relevant_nextgen_columns(stat_type),
        empty_message="No stats returned.",
        missing_columns_message="No preview columns available.",
    )


def main() -> None:
    console.print(
        Panel(
            "Interactively explore nflverse player data using nflreadpy.",
            title="Down-Data Player Explorer",
            subtitle="Enter a player name to begin.",
            border_style="blue",
        )
    )

    while True:
        name = Prompt.ask("Player name (or type 'exit' to quit)").strip()
        if not name:
            continue
        if name.lower() in {"exit", "quit", "q"}:
            console.print("Goodbye!")
            break

        team = ask_optional_str("Team filter (optional)")
        draft_year = ask_optional_int("Draft year filter (optional)")
        draft_team = ask_optional_str("Draft team filter (optional)")
        position = ask_optional_str("Position filter (optional)")

        try:
            player = Player(name=name, team=team, draft_year=draft_year, draft_team=draft_team, position=position)
        except PlayerNotFoundError as exc:
            console.print(f"[red]{exc}[/red]")
            continue
        except Exception as exc:  # pragma: no cover - defensive error handling
            console.print(f"[red]Unexpected error: {exc}[/red]")
            logging.exception("Unexpected error while creating player instance")
            continue

        console.print(player.to_rich_table())

        if Confirm.ask("Fetch stats for specific seasons now?", default=False):
            console.print(f"[dim]Note: Stats data is only available from {EARLIEST_SEASON_AVAILABLE} to {LATEST_SEASON_AVAILABLE}[/dim]")
            raw_seasons = Prompt.ask("Enter comma separated seasons", default="").strip()
            seasons = parse_seasons(raw_seasons)
            if not seasons:
                console.print("[yellow]No valid seasons provided; skipping stat retrieval.[/yellow]")
            else:
                try:
                    stats_table = build_stats_table(player, seasons)
                    console.print(stats_table)
                except SeasonNotAvailableError as exc:
                    console.print(f"[red]{exc}[/red]")
                    continue
                
                # Offer to show career totals
                if Confirm.ask("Show career totals for these seasons?", default=True):
                    try:
                        career_stats = player.get_career_stats(seasons=seasons)
                        if career_stats:
                            career_table = Table(title=f"Career Totals for {player.profile.full_name}")
                            career_table.add_column("Stat", style="cyan")
                            career_table.add_column("Total", style="green")
                            for stat_name, value in career_stats.items():
                                career_table.add_row(stat_name.replace("_", " ").title(), str(value))
                            console.print(career_table)
                        else:
                            console.print("[yellow]No career stats available.[/yellow]")
                    except SeasonNotAvailableError as exc:
                        console.print(f"[red]{exc}[/red]")
                
                # Offer NextGen advanced stats if available
                if Confirm.ask("Fetch NextGen advanced stats? (2016+ only)", default=False):
                    console.print(f"[dim]Note: NextGen Stats only available from {EARLIEST_NEXTGEN_SEASON} onwards[/dim]")
                    nextgen_seasons_raw = Prompt.ask(
                        "Enter comma separated seasons for NextGen stats",
                        default=",".join(str(s) for s in seasons if s >= EARLIEST_NEXTGEN_SEASON)
                    ).strip()
                    
                    nextgen_seasons = parse_seasons(nextgen_seasons_raw)
                    if not nextgen_seasons:
                        console.print("[yellow]No valid seasons provided; skipping NextGen stats.[/yellow]")
                    else:
                        # Auto-detect stat type or allow override
                        auto_stat_type = player.get_nextgen_stat_type()
                        console.print(f"[dim]Auto-detected stat type: {auto_stat_type}[/dim]")
                        
                        if Confirm.ask("Use auto-detected stat type?", default=True):
                            stat_type = auto_stat_type
                        else:
                            stat_type = Prompt.ask(
                                "Enter stat type",
                                choices=["passing", "rushing", "receiving"],
                                default=auto_stat_type
                            )
                        
                        try:
                            nextgen_table = build_nextgen_stats_table(player, nextgen_seasons, stat_type)
                            console.print(nextgen_table)
                        except SeasonNotAvailableError as exc:
                            console.print(f"[red]{exc}[/red]")
                        except ValueError as exc:
                            console.print(f"[yellow]{exc}[/yellow]")
                        except Exception as exc:
                            console.print(f"[red]Error fetching NextGen stats: {exc}[/red]")
                            logging.exception("Error fetching NextGen advanced stats")
        
        # Offer to generate master stats table (outside of stats fetching block so always available)
        if Confirm.ask("Generate master stats table and save to CSV?", default=False):
            console.print(f"[dim]This will create a comprehensive table with all available stats and save to 'data/exports/player_master_stats.csv'[/dim]")
            
            # Ask which seasons to include
            master_seasons_raw = Prompt.ask(
                "Enter comma separated seasons for master table (or leave blank for all available)",
                default=""
            ).strip()
            
            try:
                if master_seasons_raw:
                    master_seasons = parse_seasons(master_seasons_raw)
                    if not master_seasons:
                        console.print("[yellow]No valid seasons provided; fetching all available seasons.[/yellow]")
                        master_seasons = None
                else:
                    master_seasons = None
                
                # Ask about NextGen inclusion
                include_nextgen = Confirm.ask("Include NextGen advanced stats where available? (2016+)", default=True)
                
                # Ask about playoff inclusion
                include_playoffs = Confirm.ask("Include playoff stats?", default=True)
                
                console.print("\n[cyan]Generating master stats table...[/cyan]")
                
                master_table = player.get_master_stats_table(
                    seasons=master_seasons,
                    include_nextgen=include_nextgen,
                    include_playoffs=include_playoffs
                )
                
                if master_table.empty:
                    console.print("[yellow]No stats available to generate master table.[/yellow]")
                else:
                    # Save to CSV in exports directory
                    output_file = "data/exports/player_master_stats.csv"
                    master_table.to_csv(output_file, index=False)
                    
                    console.print(f"\n[green]✓ Master stats table saved to '{output_file}'[/green]")
                    console.print(f"[dim]Shape: {master_table.shape[0]} seasons × {master_table.shape[1]} stat categories[/dim]")
                    console.print(f"[dim]Seasons: {master_table['season'].min()} - {master_table['season'].max()}[/dim]")
                    console.print(f"[dim]Total columns: {len(master_table.columns)}[/dim]")
                    
                    # Show what was included
                    stats_type = "Regular season"
                    if include_playoffs:
                        stats_type = "Regular season + playoffs"
                    console.print(f"[dim]Stats type: {stats_type}[/dim]")
                    
                    # Show sample of what was saved
                    console.print("\n[cyan]Sample of saved data (first 5 columns):[/cyan]")
                    sample_cols = list(master_table.columns[:5])
                    sample_table = Table(title=f"{player.profile.full_name} - Master Stats Preview")
                    for col in sample_cols:
                        sample_table.add_column(col, overflow="fold")
                    for idx, row in master_table.iterrows():
                        sample_table.add_row(*[str(row[col]) for col in sample_cols])
                    console.print(sample_table)
            
            except SeasonNotAvailableError as exc:
                console.print(f"[red]{exc}[/red]")
            except ImportError as exc:
                console.print(f"[red]{exc}[/red]")
            except Exception as exc:
                console.print(f"[red]Error generating master table: {exc}[/red]")
                logging.exception("Error generating master stats table")

        if not Confirm.ask("Look up another player?", default=True):
            console.print("Enjoy exploring NFL data!")
            break


if __name__ == "__main__":
    main()
