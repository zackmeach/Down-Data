# Down-Data Player Explorer (OOTP-style)

## Overview

This project is now centered on a single, powerful `Player` object and a minimal, Rich-powered CLI in `main.py`. It’s designed as a foundation for an OOTP-style NFL data explorer, keeping only what’s necessary:

- `player.py`: High-level API for player profile and stats (1999+), NextGen stats (2016+), and helpers.
- `main.py`: Interactive terminal explorer for searching players, previewing stats, and exporting a master CSV.
- `data/`: Your data workspace (kept intact, including exports and raw data areas).
- `agentic-feedback/` and `ootp-images/`: Preserved for notes and UI inspiration.

Removed legacy components include the `stats/` folder and `stats_engine.py`.

## Install

- Install Python 3.10+.
- Install dependencies:

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

You’ll be prompted for a player name and optional filters (team, position, draft year/team). Then choose actions to preview weekly/season stats, view career totals, fetch NextGen stats, or export a master stats table to `data/exports/`.

## Capabilities (via `player.py`)

- Flexible player resolution with optional disambiguation filters.
- Profile snapshot with identifiers (GSIS, PFR, PFF, ESPN, etc.) and biographical fields.
- Weekly/season stats: 1999–present.
- NextGen advanced stats: 2016–present (passing/rushing/receiving).
- Career totals and a full master stats table (one row per season) with optional playoffs.
- CSV export from the CLI.

## Data availability and limitations

- Profile data exists across eras, but weekly/season stats are available from 1999 onward.
- NextGen tracking metrics exist from 2016 onward.
- Defensive player attribution in play-by-play is limited; coverage-derived stats are partial by nature.

## Minimal example

```python
from player import Player

player = Player(name="Patrick Mahomes")
print(player.info())  # Profile dict

# Weekly/season stats (1999+)
stats = player.fetch_stats(seasons=[2018, 2019])

# NextGen stats (2016+)
ng = player.fetch_nextgen_stats(seasons=[2023], stat_type=player.get_nextgen_stat_type())

# Career totals and master table
totals = player.get_career_stats(seasons=True)
master = player.get_master_stats_table()
```

## Repository layout (post-cleanup)

- `player.py`
- `main.py`
- `data/`
- `agentic-feedback/`
- `ootp-images/`
- `requirements.txt`
- `README.md`

## Credits

Built on top of the excellent nflverse ecosystem and `nflreadpy` for data access.
