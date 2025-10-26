# Down-Data Player Explorer

## High-level summary

This project provides an extendable `Player` object powered by [`nflreadpy`](https://github.com/nflverse/nflreadr) and a Rich-based
terminal application (`main.py`) that lets you explore player metadata interactively. The constructor supports disambiguating players
with the same name by optionally supplying team, draft year, draft team, or position filters and chooses the most notable match when
multiple candidates remain. Once instantiated, the `Player` keeps key identifiers and biographical details in memory and exposes an
entry point for loading nflverse stats on demand. The CLI now deduplicates season inputs, reuses a shared preview table renderer, and
surfaces clearer guidance when no preview columns or rows are available.

## Dependencies

The core `Player` class relies on the following Python packages:

- [`polars`](https://pola.rs/) for fast in-memory data manipulation.
- [`nflreadpy`](https://github.com/nflverse/nflreadr) for downloading nflverse datasets.
- [`pandas`](https://pandas.pydata.org/) for the optional `get_master_stats_table()` export helper.
- [`rich`](https://rich.readthedocs.io/) for the interactive terminal explorer in `main.py`.

Install them with `pip install -r requirements.txt` before running the examples below.

## Data Availability & Limitations

**Player Profile Data**: Available for all NFL players across all eras, including historical players. This includes biographical
information (name, birth date, college), physical attributes (height, weight), draft information, and cross-platform IDs (PFR, PFF,
ESPN, etc.).

**Weekly/Seasonal Stats Data**: **Only available from 1999 to present (2025)**. The nflverse dataset does not include weekly player
statistics from seasons prior to 1999. This means:

- ✅ You can look up **any player** from any era (e.g., Dan Marino, Jerry Rice, Walter Payton)
- ✅ You can view their profile information, draft details, and career metadata
- ❌ You cannot retrieve weekly or seasonal stats for seasons before 1999
- ✅ For players who played in 1999 or later, you can access comprehensive stats including:
  - Offensive stats: passing/rushing/receiving yards, touchdowns, targets, receptions, EPA, fantasy points
  - Defensive stats: tackles, sacks, interceptions, passes defended, forced fumbles
  - Special teams stats and more
- ✅ For players active in 2016 or later, you can also access **NFL NextGen Stats**:
  - Advanced tracking metrics: time to throw, separation, cushion, yards over expected, efficiency, aggressiveness, etc.
  - Powered by player tracking data for deeper performance insights

**Example**:
```python
# This works - Dan Marino's profile is available
player = Player(name="Dan Marino")
print(player.info())  # Shows all biographical data

# This fails - 1990 stats are not available
stats = player.fetch_stats(seasons=[1990])  # Raises SeasonNotAvailableError

# This works - 1999+ stats are available for any player active in those years
stats = player.fetch_stats(seasons=[1999, 2000])  # Works if player was active
```

## Implementation details

* **Caching nflverse datasets** – `player.py` lazily loads and caches the nflverse player master table and ID crosswalk so repeated
  lookups avoid redundant network calls. The joined table includes both core metadata and cross-platform identifiers for later use.
* **Flexible name resolution** – The `PlayerFinder` first attempts exact name matches across `display_name`, `full_name`, `football_name`,
  and related fields. If those filters fail to capture renamed players (e.g. “Josh Hines-Allen”), a token-based fallback extends the
  search so that legacy inputs like “Josh Allen” still resolve correctly. Deduplication happens by `gsis_id` so merged result sets stay
  clean.
* **Filter-aware selection** – Optional filters (team, draft year, draft team, position) are normalised with the nflverse team catalog
  and applied before scoring candidates. The most notable player is picked using active status, experience, latest season, and draft
  capital heuristics, producing consistent selections when several players satisfy the same query.
* **Profile snapshot** – `PlayerProfile` captures the required fields (full name, birth date, college, nflverse/GSIS IDs, PFR/PFF/ESPN
  IDs, Sportradar/ESB/OTC IDs, physical attributes, draft metadata, and positional info) in an immutable dataclass with convenient
  `to_dict()` conversion for display and serialisation. The nflverse player ID currently mirrors the GSIS ID because nflverse uses the
  GSIS identifier as its canonical key; other IDs are populated whenever nflverse supplies them.
* **Stats infrastructure** – The `Player` class provides multiple methods for accessing player statistics:
  - `fetch_stats()` wraps `nflreadpy.load_player_stats` and caches the filtered Polars DataFrame for week-by-week data
  - `get_career_stats()` calculates career totals with position-specific stat aggregations (sacks, tackles for defensive; yards, TDs for offensive)
  - `get_master_stats_table()` generates a comprehensive Pandas DataFrame with one row per season and all available stat categories as columns - perfect for exporting to CSV/Excel or further analysis
  - `fetch_nextgen_stats()` accesses NFL's official advanced tracking metrics (2016+) including separation, time to throw, yards over expected, etc.
  - `fetch_coverage_stats()` extracts partial coverage data from play-by-play for defensive players (limited - see COVERAGE_STATS_RESEARCH.md)
  - Position-aware helpers: `is_defensive()`, `get_relevant_stat_columns()`, `get_nextgen_stat_type()` automatically detect appropriate stat types
* **Rich-powered CLI** – `main.py` builds an interactive prompt with Rich panels, prompts, and tables. It guides you through player
  lookup, displays the stored profile, and optionally previews per-season stats (first five rows) for the seasons you enter. The
  preview builder now guarantees consistent table formatting across basic and NextGen stats (including graceful handling of empty
  datasets) while keeping the CLI intentionally thin so future commands or visualisations can plug in easily.

## Running the demo

1. Install dependencies: `pip install -r requirements.txt`.
2. Launch the explorer: `python main.py`.
3. Follow the prompts to search for players. Supply optional filters to narrow down common names (e.g. `Player(name="Josh Allen",
   draft_team="Bills")` versus `Player(name="Josh Allen", draft_team="Jaguars")`). 
4. Once a player is found, you can:
   - View weekly stats for specific seasons
   - See career totals
   - Fetch NextGen advanced stats (2016+)
   - **Generate and save a master stats table to CSV** - creates `player_master_stats.csv` with all stats for all seasons in one file

## Quick Usage Examples

### Generate a Master Stats Table

Create a comprehensive DataFrame with all stats for a player's career:

```python
from player import Player

# Create player and fetch all available stats
player = Player(name="Patrick Mahomes")
master_table = player.get_master_stats_table(
    seasons=range(2018, 2025),
    include_nextgen=True,
    include_playoffs=True  # Set to False for regular season only
)

# Result: Pandas DataFrame with one row per season, 100+ stat categories
print(f"Shape: {master_table.shape}")  # (7 seasons, 104 columns)

# Export to CSV or Excel
master_table.to_csv('mahomes_career_stats.csv', index=False)
master_table.to_excel('mahomes_career_stats.xlsx', index=False)

# Analyze the data
career_passing_yards = master_table['passing_yards'].sum()
best_season = master_table.loc[master_table['passing_tds'].idxmax()]

# Get regular season only (excludes playoff stats)
regular_season = player.get_master_stats_table(include_playoffs=False)
```

### Access Advanced NextGen Stats

```python
# Get NFL's official tracking metrics (2016+)
player = Player(name="Tyreek Hill")
nextgen = player.fetch_nextgen_stats(seasons=[2023], stat_type="receiving")

# Advanced metrics like separation, cushion, YAC above expectation
avg_separation = nextgen['avg_separation'].mean()
avg_cushion = nextgen['avg_cushion'].mean()
```

## Known limitations

- **Seasonal aggregation for rate stats** – `get_master_stats_table()` currently sums every numeric column when collapsing
  weekly stats to the season level. Rate metrics such as completion percentage or EPA per play will therefore be inflated and
  should be recomputed downstream if you need precise season averages.
- **NextGen player matching** – NextGen datasets lack a stable GSIS identifier, so `fetch_nextgen_stats()` matches on
  `player_display_name`. Players who share the same display name may require additional filtering before analysis.

## Extending the system

* Add new profile fields by enriching the cached DataFrame or by joining additional nflverse tables in `PlayerDataSource`. Store the
  computed values in `PlayerProfile` so downstream consumers remain type-safe.
* Implement derived analytics (career summaries, advanced metrics) by building on `Player.fetch_stats()` and caching results in the
  `_cache` dictionary. Keep functions pure where possible to simplify future testing.
* Extend the Rich CLI with extra commands (e.g. comparing two players, exporting JSON) by leveraging the existing prompt helpers.
  Maintain the pattern of validating user input and surfacing informative warnings for ambiguous filters.
* When contributing new code, favour Polars expressions over pandas conversions for performance, respect the lazy-loading caches to
  minimise HTTP calls, and rely on the team normalisation utilities to keep abbreviations consistent.

