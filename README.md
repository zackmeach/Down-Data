# Down-Data Player Explorer

## High-level summary

This project provides an extendable `Player` object powered by [`nflreadpy`](https://github.com/nflverse/nflreadr) and a Rich-based
terminal application (`main.py`) that lets you explore player metadata interactively. The constructor supports disambiguating players
with the same name by optionally supplying team, draft year, draft team, or position filters and chooses the most notable match when
multiple candidates remain. Once instantiated, the `Player` keeps key identifiers and biographical details in memory and exposes an
entry point for loading nflverse stats on demand.

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
* **Stats infrastructure** – The `Player.fetch_stats()` method wraps `nflreadpy.load_player_stats` and caches the filtered Polars
  DataFrame. This establishes the hook for future stat computations or aggregations while keeping the initial object lightweight.
* **Rich-powered CLI** – `main.py` builds an interactive prompt with Rich panels, prompts, and tables. It guides you through player
  lookup, displays the stored profile, and optionally previews per-season stats (first five rows) for the seasons you enter. The CLI
  showcases the `Player` API but is intentionally thin so future commands or visualisations can plug in easily.

## Running the demo

1. Install dependencies: `pip install -r requirements.txt`.
2. Launch the explorer: `python main.py`.
3. Follow the prompts to search for players. Supply optional filters to narrow down common names (e.g. `Player(name="Josh Allen",
   draft_team="Bills")` versus `Player(name="Josh Allen", draft_team="Jaguars")`). Opt in to stat previews to exercise the
   stats-loading hook.

## Extending the system

* Add new profile fields by enriching the cached DataFrame or by joining additional nflverse tables in `PlayerDataSource`. Store the
  computed values in `PlayerProfile` so downstream consumers remain type-safe.
* Implement derived analytics (career summaries, advanced metrics) by building on `Player.fetch_stats()` and caching results in the
  `_cache` dictionary. Keep functions pure where possible to simplify future testing.
* Extend the Rich CLI with extra commands (e.g. comparing two players, exporting JSON) by leveraging the existing prompt helpers.
  Maintain the pattern of validating user input and surfacing informative warnings for ambiguous filters.
* When contributing new code, favour Polars expressions over pandas conversions for performance, respect the lazy-loading caches to
  minimise HTTP calls, and rely on the team normalisation utilities to keep abbreviations consistent.

