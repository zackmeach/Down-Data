"""Interactive helper to inspect every stat category exposed by API sources.

Run from the repository root:

    python data.py

You will be prompted for a position (e.g. QB, WR, EDGE). The script looks at
all nflverse-backed APIs used in the project—player directory, weekly stats,
NextGen tracking, and play-by-play—and shows every column that contains data
for that position, along with the originating source.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Mapping, Sequence

import polars as pl

try:  # pragma: no cover - runtime dependency
    from nflreadpy import load_player_stats, load_players, load_nextgen_stats, load_pbp
except ImportError:  # pragma: no cover - defensive import
    load_player_stats = None  # type: ignore[assignment]
    load_players = None  # type: ignore[assignment]
    load_nextgen_stats = None  # type: ignore[assignment]
    load_pbp = None  # type: ignore[assignment]


# --------------------------------------------------------------------------- #
# Position helpers

BASE_POSITION_ALIASES: dict[str, Sequence[str]] = {
    "QB": ("QB", "QUARTERBACK"),
    "RB": ("RB", "RUNNING BACK", "TAILBACK", "HALFBACK"),
    "FB": ("FB", "FULLBACK"),
    "WR": ("WR", "WIDE RECEIVER"),
    "TE": ("TE", "TIGHT END"),
    "OL": ("OL", "OFFENSIVE LINE", "LINEMAN", "OLINE"),
    "OT": ("OT", "OFFENSIVE TACKLE", "TACKLE", "T"),
    "OG": ("OG", "OFFENSIVE GUARD", "GUARD", "G"),
    "OC": ("OC", "CENTER", "C"),
    "DL": ("DL", "DEFENSIVE LINE", "DEFENSIVE LINEMAN", "DLINE"),
    "DE": ("DE", "DEFENSIVE END"),
    "DT": ("DT", "DEFENSIVE TACKLE"),
    "NT": ("NT", "NOSE TACKLE"),
    "EDGE": ("EDGE", "PASS RUSHER"),
    "LB": ("LB", "LINEBACKER"),
    "ILB": ("ILB", "INSIDE LINEBACKER", "MIDDLE LINEBACKER", "MLB"),
    "OLB": ("OLB", "OUTSIDE LINEBACKER", "WLB", "SLB"),
    "DB": ("DB", "DEFENSIVE BACK"),
    "CB": ("CB", "CORNERBACK"),
    "S": ("S", "SAFETY"),
    "FS": ("FS", "FREE SAFETY"),
    "SS": ("SS", "STRONG SAFETY"),
    "K": ("K", "KICKER"),
    "P": ("P", "PUNTER"),
    "PR": ("PR", "PUNT RETURNER"),
    "KR": ("KR", "KICK RETURNER"),
    "LS": ("LS", "LONG SNAPPER"),
}

POSITION_NORMALISATION_MAP: Mapping[str, str] = {
    "SAF": "S",
    "HB": "RB",
    "HBK": "RB",
    "FBK": "FB",
    "T": "OT",
    "G": "OG",
    "C": "OC",
}

ALL_CANONICAL_POSITIONS = set(BASE_POSITION_ALIASES.keys())
OFFENSE_SKILL_POSITIONS = {"QB", "RB", "WR", "TE", "FB"}
OFFENSE_LINE_POSITIONS = {"OL", "OT", "OG", "OC"}
DEFENSE_FRONT_POSITIONS = {"DL", "DE", "DT", "NT", "EDGE", "LB", "ILB", "OLB"}
DEFENSE_COVERAGE_POSITIONS = {"DB", "CB", "S", "FS", "SS"}
SPECIAL_TEAMS_POSITIONS = {"K", "P", "PR", "KR", "LS"}


def normalize_key(value: str) -> str:
    """Normalise free-text input for lookup comparisons."""

    return "".join(char for char in value.upper() if char.isalnum())


def canonicalize_position(value: str) -> str | None:
    """Map raw position labels to canonical keys."""

    if not value:
        return None
    upper = value.strip().upper()
    if not upper:
        return None
    upper = POSITION_NORMALISATION_MAP.get(upper, upper)
    return upper


def build_alias_lookup(positions: Iterable[str]) -> dict[str, str]:
    """Build a lookup dictionary mapping normalised input to canonical positions."""

    lookup: dict[str, str] = {}
    for position in positions:
        lookup[normalize_key(position)] = position

    for canonical, aliases in BASE_POSITION_ALIASES.items():
        if canonical not in positions:
            continue
        for alias in aliases:
            lookup[normalize_key(alias)] = canonical

    return lookup


# --------------------------------------------------------------------------- #
# Generic dataframe helpers

def ensure_dataframe(frame: object, *, name: str) -> pl.DataFrame:
    """Coerce third-party dataframes into Polars instances."""

    if isinstance(frame, pl.DataFrame):
        return frame
    try:
        return pl.DataFrame(frame)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise TypeError(f"Unsupported frame type returned by {name}: {type(frame)!r}") from exc


def classify_player_stats_source(column: str) -> str:
    """Return a descriptive label for a player stats column."""

    column_lower = column.lower()
    if column_lower in {"completions", "attempts"}:
        return "nflverse.player_stats (passing)"
    if column_lower in {"fantasy_points", "fantasy_points_ppr"}:
        return "nflverse.player_stats (fantasy)"
    if column_lower.startswith("passing_") or column_lower in {"pass_yards", "pass_tds"}:
        return "nflverse.player_stats (passing)"
    if column_lower.startswith("rushing_") or column_lower in {"carries", "rush_yards"}:
        return "nflverse.player_stats (rushing)"
    if column_lower.startswith("receiving_") or column_lower in {"targets", "receptions"}:
        return "nflverse.player_stats (receiving)"
    if column_lower.startswith("def_") or column_lower in {"solo_tackles", "tackles"}:
        return "nflverse.player_stats (defense)"
    if column_lower.startswith("fg_") or column_lower.startswith("pat_") or column_lower.startswith("gwfg_"):
        return "nflverse.player_stats (kicking)"
    if column_lower.startswith("punt_return") or column_lower.startswith("kickoff_return") or column_lower.startswith("kick_return"):
        return "nflverse.player_stats (returning)"
    if column_lower.endswith("_share") or column_lower.endswith("_epa") or column_lower.endswith("_conversions"):
        return "nflverse.player_stats (derived)"
    return "nflverse.player_stats"


def format_label(column: str) -> str:
    """Return a human-friendly label for a stat column name."""

    words = column.replace("_", " ").split()
    formatted = " ".join(word.upper() if len(word) <= 3 else word.capitalize() for word in words)
    return formatted


# --------------------------------------------------------------------------- #
# Data preparation (player directory, weekly stats, NextGen, play-by-play)

PLAYER_DIRECTORY_EXCLUDED = {
    "_position",
    "position",
    "position_group",
    "ngs_position",
    "ngs_position_group",
    "display_name",
    "common_first_name",
    "first_name",
    "last_name",
    "short_name",
    "football_name",
    "suffix",
    "headshot",
    "latest_team",
    "status",
    "ngs_status",
    "ngs_status_short_description",
    "gsis_id",
    "esb_id",
    "nfl_id",
    "pfr_id",
    "pff_id",
    "otc_id",
    "espn_id",
    "smart_id",
}

PLAYER_STATS_EXCLUDED = {
    "_position",
    "player_id",
    "player_name",
    "player_display_name",
    "position",
    "position_group",
    "recent_team",
    "team",
    "opponent_team",
    "headshot_url",
    "game_id",
    "season",
    "week",
    "season_type",
}

NEXTGEN_EXCLUDED = {
    "_position",
    "player_display_name",
    "player_position",
    "team_abbr",
    "season",
    "season_type",
    "week",
}


def prepare_player_directory() -> pl.DataFrame:
    """Return nflverse player directory with canonical position column."""

    if load_players is None:
        return pl.DataFrame()
    players = ensure_dataframe(load_players(), name="load_players")
    position_candidates = [col for col in ("position", "position_group", "ngs_position", "ngs_position_group") if col in players.columns]
    if not position_candidates:
        return pl.DataFrame()

    players = players.with_columns(
        pl.coalesce([pl.col(col) for col in position_candidates])
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
        .map_elements(lambda val: canonicalize_position(val) or "", return_dtype=pl.Utf8)
        .alias("_position")
    )
    players = players.filter(pl.col("_position").str.len_chars() > 0)

    usable_columns = ["_position"] + [
        column for column in players.columns if column not in PLAYER_DIRECTORY_EXCLUDED
    ]
    return players.select(usable_columns)


def call_load_player_stats(seasons: Iterable[int] | bool | None = None) -> pl.DataFrame:
    """Call nflreadpy.load_player_stats with graceful fallbacks."""

    if load_player_stats is None:
        raise RuntimeError(
            "nflreadpy is not installed. Install project dependencies first "
            "(`pip install -r requirements.txt`)."
        )

    kwargs: dict[str, object] = {}
    if seasons is not None:
        kwargs["seasons"] = seasons

    try:
        frame = load_player_stats(**kwargs)
    except TypeError:
        frame = load_player_stats(seasons)  # type: ignore[misc]

    return ensure_dataframe(frame, name="load_player_stats")


def prepare_player_stats_frame() -> pl.DataFrame:
    """Fetch player stats (rolling four seasons) with canonical positions."""

    current_year = datetime.now().year
    seasons = list(range(current_year - 3, current_year + 1))
    print(f"Fetching nflverse player stats for seasons {seasons[0]}-{seasons[-1]}...")
    stats = call_load_player_stats(seasons=seasons)
    if stats.is_empty():
        raise RuntimeError("No stats returned from nflverse; cannot build stat catalog.")

    position_columns = [col for col in ("player_position", "position", "position_group") if col in stats.columns]
    stats = stats.with_columns(
        pl.coalesce([pl.col(col) for col in position_columns])
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
        .map_elements(lambda val: canonicalize_position(val) or "", return_dtype=pl.Utf8)
        .alias("_position")
    )
    stats = stats.filter(pl.col("_position").str.len_chars() > 0)

    subset_columns = ["_position"] + [
        col for col in stats.columns if col not in {"player_id", "week", "season", "_position"}
    ]
    return stats.select(subset_columns)


def prepare_nextgen_frame(stat_type: str) -> pl.DataFrame:
    """Fetch NextGen stats for the provided stat type."""

    if load_nextgen_stats is None:
        return pl.DataFrame()
    current_year = datetime.now().year
    start_year = max(2016, current_year - 3)
    seasons = list(range(start_year, current_year + 1))
    print(f"Fetching nflverse nextgen stats ({stat_type}) for seasons {seasons[0]}-{seasons[-1]}...")
    frame = ensure_dataframe(load_nextgen_stats(stat_type=stat_type, seasons=seasons), name=f"load_nextgen_stats[{stat_type}]")
    if frame.is_empty():
        return frame
    frame = frame.with_columns(
        pl.col("player_position")
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
        .map_elements(lambda val: canonicalize_position(val) or "", return_dtype=pl.Utf8)
        .alias("_position")
    )
    frame = frame.filter(pl.col("_position").str.len_chars() > 0)
    usable_columns = ["_position"] + [
        column for column in frame.columns if column not in NEXTGEN_EXCLUDED
    ]
    return frame.select(usable_columns)


def prepare_pbp_frame() -> pl.DataFrame:
    """Fetch a recent season of play-by-play data."""

    if load_pbp is None:
        return pl.DataFrame()
    current_year = datetime.now().year
    seasons = [current_year - 1]
    print(f"Fetching nflverse play-by-play data for season {seasons[0]}...")
    frame = ensure_dataframe(load_pbp(seasons=seasons), name="load_pbp")
    return frame


# --------------------------------------------------------------------------- #
# Catalog construction helpers

CatalogType = dict[str, dict[str, set[str]]]


def update_catalog_from_frame(
    catalog: CatalogType,
    frame: pl.DataFrame,
    *,
    position_column: str,
    excluded_columns: set[str],
    source_resolver,
) -> None:
    """Add columns from a frame into the position catalog."""

    if frame.is_empty():
        return
    usable_columns = [column for column in frame.columns if column not in excluded_columns]
    if not usable_columns:
        return

    grouped = frame.group_by(position_column).agg(
        [
            pl.col(column).is_not_null().any().alias(column)
            for column in usable_columns
        ]
    )

    for row in grouped.iter_rows(named=True):
        raw_position = row[position_column]
        if not isinstance(raw_position, str):
            continue
        position = canonicalize_position(raw_position) or raw_position
        if not position:
            continue
        for column, has_data in row.items():
            if column == position_column or not has_data:
                continue
            source_label = source_resolver(column)
            catalog[position].setdefault(column, set()).add(source_label)


PBP_COLUMN_EXCLUDED = {
    "game_id",
    "old_game_id",
    "home_team",
    "away_team",
    "posteam",
    "defteam",
    "side_of_field",
    "yardline_100",
    "game_date",
    "drive",
    "play_id",
    "desc",
}

PBP_COLUMN_RULES: list[tuple[str, set[str]]] = [
    ("pass_", {"QB"}),
    ("qb_", {"QB"}),
    ("pocket_", {"QB"}),
    ("air_yards", {"QB", "WR", "TE"}),
    ("rusher_", OFFENSE_SKILL_POSITIONS),
    ("rush_", OFFENSE_SKILL_POSITIONS),
    ("run_", OFFENSE_SKILL_POSITIONS),
    ("yards_after_contact", OFFENSE_SKILL_POSITIONS),
    ("receiving_", {"WR", "TE", "RB"}),
    ("receiver_", {"WR", "TE", "RB"}),
    ("rec_", {"WR", "TE", "RB"}),
    ("target_", {"WR", "TE", "RB"}),
    ("carries", OFFENSE_SKILL_POSITIONS),
    ("complete_pass", OFFENSE_SKILL_POSITIONS),
    ("incomplete_pass", {"QB", "WR", "TE", "RB"}),
    ("solo_tackle", DEFENSE_FRONT_POSITIONS | DEFENSE_COVERAGE_POSITIONS),
    ("assist_tackle", DEFENSE_FRONT_POSITIONS | DEFENSE_COVERAGE_POSITIONS),
    ("tackle", DEFENSE_FRONT_POSITIONS | DEFENSE_COVERAGE_POSITIONS),
    ("qb_hit", DEFENSE_FRONT_POSITIONS),
    ("pressure", DEFENSE_FRONT_POSITIONS | OFFENSE_LINE_POSITIONS),
    ("sack", DEFENSE_FRONT_POSITIONS | OFFENSE_LINE_POSITIONS),
    ("interception", DEFENSE_COVERAGE_POSITIONS),
    ("pass_defense", DEFENSE_COVERAGE_POSITIONS),
    ("fumble", OFFENSE_SKILL_POSITIONS | DEFENSE_FRONT_POSITIONS | DEFENSE_COVERAGE_POSITIONS),
    ("punt_", {"P", "PR", "WR"}),
    ("punter_", {"P"}),
    ("kickoff_", {"K", "KR", "WR", "RB"}),
    ("kick_", {"K", "KR"}),
    ("fg_", {"K"}),
    ("pat_", {"K"}),
    ("field_goal", {"K"}),
    ("extra_point", {"K"}),
    ("return_", {"PR", "KR", "WR", "RB", "CB"}),
    ("blocked", DEFENSE_FRONT_POSITIONS | DEFENSE_COVERAGE_POSITIONS),
    ("penalty", ALL_CANONICAL_POSITIONS),
]


def infer_pbp_positions(column: str) -> set[str]:
    """Return positions associated with a play-by-play metric via heuristics."""

    column_lower = column.lower()
    if column_lower in PBP_COLUMN_EXCLUDED or column_lower.endswith("_player_id"):
        return set()
    for prefix, positions in PBP_COLUMN_RULES:
        if column_lower.startswith(prefix):
            return set(positions)
        if prefix in {"air_yards", "pressure", "field_goal"} and prefix in column_lower:
            return set(positions)
    if any(token in column_lower for token in ("epa", "wpa", "cpoe", "success", "yards_gained", "td", "touchdown")):
        return set(ALL_CANONICAL_POSITIONS)
    return set()


def update_catalog_from_pbp(catalog: CatalogType, frame: pl.DataFrame, *, source_label: str) -> None:
    """Augment catalog with play-by-play columns using heuristic mappings."""

    if frame.is_empty():
        return
    for column in frame.columns:
        positions = infer_pbp_positions(column)
        if not positions:
            continue
        if "ALL" in positions:
            positions = set(ALL_CANONICAL_POSITIONS)
        for position in positions:
            catalog[position].setdefault(column, set()).add(source_label)


def build_catalog() -> CatalogType:
    """Collect stat categories from every API source used by the project."""

    catalog: CatalogType = defaultdict(lambda: defaultdict(set))

    players = prepare_player_directory()
    update_catalog_from_frame(
        catalog,
        players,
        position_column="_position",
        excluded_columns=PLAYER_DIRECTORY_EXCLUDED,
        source_resolver=lambda _col: "nflverse.players",
    )

    player_stats = prepare_player_stats_frame()
    update_catalog_from_frame(
        catalog,
        player_stats,
        position_column="_position",
        excluded_columns=PLAYER_STATS_EXCLUDED,
        source_resolver=classify_player_stats_source,
    )

    for stat_type in ("passing", "rushing", "receiving"):
        nextgen = prepare_nextgen_frame(stat_type)
        update_catalog_from_frame(
            catalog,
            nextgen,
            position_column="_position",
            excluded_columns=NEXTGEN_EXCLUDED,
            source_resolver=lambda _col, st=stat_type: f"nflverse.nextgen_stats ({st})",
        )

    pbp = prepare_pbp_frame()
    update_catalog_from_pbp(catalog, pbp, source_label="nflverse.play_by_play")

    return catalog


# --------------------------------------------------------------------------- #
# Presentation helpers

def sort_catalog_for_display(catalog: CatalogType) -> dict[str, dict[str, list[str]]]:
    """Convert catalog into display-friendly dictionaries."""

    sorted_catalog: dict[str, dict[str, list[str]]] = {}
    for position, columns in catalog.items():
        sorted_columns = {
            column: sorted(sources)
            for column, sources in sorted(columns.items(), key=lambda item: item[0])
        }
        sorted_catalog[position] = sorted_columns
    return sorted_catalog


def prompt_for_position(catalog: dict[str, dict[str, list[str]]]) -> None:
    """Interactive prompt that lets the user select positions to inspect."""

    if not catalog:
        print("No statistical categories were discovered. Nothing to display.")
        return

    positions = sorted(catalog.keys())
    alias_lookup = build_alias_lookup(positions)

    print("\nAvailable positions:")
    print(", ".join(positions))
    print("Type a position code (or common name), 'list' to reprint the options, or 'quit' to exit.\n")

    while True:
        try:
            raw_choice = input("Position> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            return

        if not raw_choice:
            continue
        choice_lower = raw_choice.lower()
        if choice_lower in {"quit", "exit"}:
            print("Goodbye!")
            return
        if choice_lower == "list":
            print(", ".join(positions))
            continue

        normalised = normalize_key(raw_choice)
        resolved = alias_lookup.get(normalised)
        if not resolved:
            print("Unrecognised position. Type 'list' to see valid options.")
            continue

        entries = catalog.get(resolved)
        if not entries:
            print(f"No statistical categories found for {resolved}.")
            continue

        print(f"\nStatistical categories for {resolved} ({len(entries)} total):")
        for column, sources in entries.items():
            source_label = ", ".join(sources)
            print(f"- {column} [{source_label}] ({format_label(column)})")
        print()
        print("Enter another position, or type 'quit' to exit.\n")


# --------------------------------------------------------------------------- #
# Entry point

def main() -> int:
    """Script entry point."""

    try:
        catalog = build_catalog()
        display_catalog = sort_catalog_for_display(catalog)
    except Exception as exc:  # pragma: no cover - runtime diagnostic
        print(f"[Error] {exc}")
        return 1

    prompt_for_position(display_catalog)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())

