from __future__ import annotations

import logging
from typing import Iterable, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from player import Player, PlayerNotFoundError

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
    seasons: List[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            seasons.append(int(chunk))
        except ValueError:
            console.print(f"[yellow]Ignoring invalid season entry: {chunk}[/yellow]")
    return seasons


def build_stats_table(player: Player, seasons: Iterable[int], stats_columns: Optional[List[str]] = None) -> Table:
    stats = player.fetch_stats(seasons=seasons)
    console.print(f"Loaded {stats.height} rows of stats for {player.profile.full_name}.")
    preview = stats.head(5)
    columns = stats_columns or [
        "season",
        "week",
        "team",
        "opponent_team",
        "passing_yards",
        "rushing_yards",
        "receiving_yards",
        "fantasy_points",
    ]
    available_columns = [col for col in columns if col in preview.columns]

    table = Table(title=f"Stat preview for seasons {', '.join(str(s) for s in seasons)}")
    if not available_columns:
        table.add_column("message")
        table.add_row("No preview columns are available for the returned dataset.")
        return table

    for column in available_columns:
        table.add_column(column)

    for row in preview.iter_rows(named=True):
        table.add_row(*[str(row.get(column, "")) for column in available_columns])

    if preview.height == 0:
        table.add_row(*(["No stats returned for the selected seasons."] + [""] * (len(available_columns) - 1)))
    return table


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
            raw_seasons = Prompt.ask("Enter comma separated seasons", default="").strip()
            seasons = parse_seasons(raw_seasons)
            if not seasons:
                console.print("[yellow]No valid seasons provided; skipping stat retrieval.[/yellow]")
            else:
                stats_table = build_stats_table(player, seasons)
                console.print(stats_table)

        if not Confirm.ask("Look up another player?", default=True):
            console.print("Enjoy exploring NFL data!")
            break


if __name__ == "__main__":
    main()
