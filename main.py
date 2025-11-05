"""
Down-Data Player Explorer

Interactive terminal app for exploring NFL player profiles and stats using
the Player object as the single source of truth.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

import polars as pl
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.table import Table

from player import Player, SeasonNotAvailableError


logging.basicConfig(level=logging.WARNING)
console = Console()


def _parse_seasons(value: str) -> Optional[List[int] | bool]:
    """Parse user input like '2019,2021-2023' or 'all' into a list of ints or True.

    Returns None for empty input.
    """
    value = value.strip()
    if not value:
        return None
    lowered = value.lower()
    if lowered in {"all", "*", "true"}:
        return True
    seasons: List[int] = []
    for token in value.replace(" ", "").split(","):
        if not token:
            continue
        if "-" in token:
            start_s, end_s = token.split("-", 1)
            try:
                start = int(start_s)
                end = int(end_s)
                if start > end:
                    start, end = end, start
                seasons.extend(list(range(start, end + 1)))
            except ValueError:
                continue
        else:
            try:
                seasons.append(int(token))
            except ValueError:
                continue
    # Deduplicate and sort
    return sorted(set(seasons)) if seasons else None


def _render_profile_table(player: Player) -> Table:
    table = player.to_rich_table()
    table.title = f"{player.profile.full_name}"
    return table


def _preview_df(df: pl.DataFrame, title: str, limit: int = 10) -> None:
    if df.height == 0:
        console.print("\n[yellow]No data available[/yellow]")
        return
    head = df.head(limit)
    # Render a simple Rich table for visibility
    table = Table(title=title, show_lines=False)
    for col in head.columns:
        table.add_column(col)
    for row in head.iter_rows():
        table.add_row(*["" if v is None else str(v) for v in row])
    console.print(table)


def main() -> None:
    console.print("\n[bold cyan]Down-Data Player Explorer[/bold cyan]")
    console.print("Explore NFL players by name, then view profile and stats.\n")

    while True:
        name = Prompt.ask("Player name", default="Patrick Mahomes").strip()
        if not name:
            console.print("[yellow]Please enter a player name.[/yellow]")
            continue

        team = Prompt.ask("Team (optional)", default="").strip() or None
        position = Prompt.ask("Position (optional)", default="").strip() or None
        draft_year_raw = Prompt.ask("Draft year (optional)", default="").strip()
        draft_year = int(draft_year_raw) if draft_year_raw.isdigit() else None
        draft_team = Prompt.ask("Draft team (optional)", default="").strip() or None

        try:
            player = Player(
                name=name,
                team=team,
                draft_year=draft_year,
                draft_team=draft_team,
                position=position,
            )
        except Exception as exc:
            console.print(f"\n[red]Could not resolve player:[/red] {exc}\n")
            if not Confirm.ask("Try again?", default=True):
                break
            continue

        console.print("\n[bold]Profile[/bold]")
        console.print(_render_profile_table(player))

        # Actions loop for this player
        while True:
            console.print("\n[bold]Choose an action:[/bold]")
            console.print("1. Preview weekly/season stats")
            console.print("2. View career totals")
            console.print("3. Preview NextGen stats (2016+)")
            console.print("4. Export master stats table to CSV")
            console.print("0. New player / Exit")

            choice = Prompt.ask("Action", choices=["1", "2", "3", "4", "0"], default="1")

            if choice == "0":
                break

            if choice == "1":
                season_str = Prompt.ask("Seasons (e.g. 2020,2022-2023 or 'all')", default="").strip()
                seasons = _parse_seasons(season_str)
                try:
                    df = player.fetch_stats(seasons=seasons)
                    _preview_df(df, title="Player Stats (preview)")
                except SeasonNotAvailableError as e:
                    console.print(f"[yellow]{e}[/yellow]")
            elif choice == "2":
                try:
                    totals = player.get_career_stats(seasons=True)
                except SeasonNotAvailableError as e:
                    console.print(f"[yellow]{e}[/yellow]")
                    continue
                if not totals:
                    console.print("[yellow]No totals available[/yellow]")
                else:
                    table = Table(title="Career totals")
                    table.add_column("Metric")
                    table.add_column("Total", justify="right")
                    for key, value in totals.items():
                        table.add_row(key, str(value))
                    console.print(table)
            elif choice == "3":
                default_type = player.get_nextgen_stat_type()
                stat_type = Prompt.ask(
                    f"NextGen stat type [passing/rushing/receiving]",
                    choices=["passing", "rushing", "receiving"],
                    default=default_type,
                )
                season_str = Prompt.ask("Seasons (2016+ or 'all')", default="").strip()
                seasons = _parse_seasons(season_str)
                try:
                    ng = player.fetch_nextgen_stats(seasons=seasons, stat_type=stat_type)
                    _preview_df(ng, title=f"NextGen ({stat_type}) (preview)")
                except SeasonNotAvailableError as e:
                    console.print(f"[yellow]{e}[/yellow]")
            elif choice == "4":
                try:
                    master = player.get_master_stats_table()
                except Exception as e:
                    console.print(f"[red]Failed to build master stats table:[/red] {e}")
                    continue
                if getattr(master, "empty", False):
                    console.print("[yellow]No data available to export[/yellow]")
                else:
                    filename = f"data/exports/{player.profile.full_name.replace(' ', '_').lower()}_master_stats.csv"
                    try:
                        master.to_csv(filename, index=False)
                        console.print(f"[green]Saved to {filename}[/green]")
                    except Exception as e:
                        console.print(f"[red]Failed to save CSV:[/red] {e}")

        if not Confirm.ask("\nSearch for another player?", default=True):
            console.print("\n[cyan]Goodbye![/cyan]")
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Interrupted[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        logging.exception("Error in main")
