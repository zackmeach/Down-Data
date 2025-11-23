# Down-Data Player Explorer

Desktop clone of the OOTP26 player experience for real NFL data. The UI is built with PySide6, backed by our existing `Player` domain layer, and organised around a two-level navigation shell plus a 12×24 grid layout. This README targets developers working inside the repo.

## Requirements

- Python 3.10 or newer
- Windows/macOS/Linux with Qt 6 runtime (installed automatically via PySide6)
- Internet access the first time nflverse assets are fetched (nflreadpy caches on demand)
- Optional: network access to Pro-Football-Reference when using the scraper helpers

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate      # or source .venv/bin/activate
pip install -r requirements.txt
```

## Running

### Desktop UI

```bash
python main.py
```

This resolves to `down_data.app.run_app()`, which creates the shared `QApplication`, applies the dark palette, and instantiates `MainWindow`. The shell starts maximised with a 16:9 aspect ratio lock to preserve grid proportions.

### CLI (legacy utilities)

```bash
python -m down_data.cli
```

The CLI still exercises the same domain layer (`Player`, nflreadpy wrappers) and is useful for quick smoke-tests when you do not need the Qt surface.

## Repository layout (developer view)

```text
.
├── main.py                  # Minimal entry point – forwards to down_data.app.run_app
├── down_data/
│   ├── app.py               # QApplication bootstrap + palette hook
│   ├── backend/             # Bridges between UI widgets and domain objects
│   │   └── player_service.py
│   ├── core/                # Domain models (Player, PlayerProfile, ratings, queries)
│   ├── ui/                  # PySide6 widgets/pages and styling assets
│   │   ├── main_window.py   # QMainWindow wrapper with history-aware navigation
│   │   ├── pages/
│   │   │   ├── content_page.py       # Navigation controller + stacked content
│   │   │   ├── player_search_page.py # OOTP-style finder with filter tabs
│   │   │   ├── player_detail_page.py # Tab scaffold + stats/ratings presentation
│   │   │   └── placeholder_page.py   # Temporary stubs for unfinished sections
│   │   ├── widgets/         # Reusable controls (nav bar, context bar, grid helper, panels)
│   │   └── styles.py        # Centralised palette + fonts
│   └── cli.py               # Rich-powered command line entry
├── docs/ui/                 # Grid, panel, and visual language references
├── agentic-feedback/        # Consolidated multi-agent planning notes
├── ootp-images/             # Screenshot references from the original OOTP UI
└── requirements.txt
```

## UI architecture

- **Navigation shell** (`ContentPage`): Maintains two navigation contexts (`default`, `player_detail`) so back/forward/home behave like OOTP. Pages are mapped via `NAVIGATION_MAP`, then pushed into a `QStackedWidget`.
- **Grid system** (`GridLayoutManager`): All pages live on a 12×24 coordinate system. Each widget registers a `GridCell` (column, row, spans) so resizing the window simply re-computes cell bounds.
- **Panels + tables** (`widgets/panel.py`, `widgets/table_panel.py`): Provide consistent frame/border styling and QTableWidget wrappers with built-in zebra rows, placeholder text, and header formatting.
- **Styling** (`styles.py`): Applies the charcoal palette, accent colours, typography, and shared object names (`PrimaryButton`, `FilterComboBox`, etc.) used across the UI.

## Player search flow

- Implemented in `player_search_page.py`.
- Filter panel: offence/defence tabs with age/service sliders, team/year selectors, draft and contract filters. Controls are backed by helper widgets (`RangeSelector`, standardised combobox styles).
- Data handling: pulls the cached nflverse directory via `PlayerService.get_all_players()`, normalises it (`_prepare_player_directory_frame`), and filters in-memory using Polars.
- Results table: `TablePanel` lists players with pagination, surfacing display name, position, team, and bio data. Selecting a row emits `playerSelected` with IDs plus the original row payload.

## Player detail flow

- `player_detail_page.py` consumes the payload from the search page and prepares a multi-tab scaffold (`Profile` → `Summary|Contract|Injury History`, `Stats`, `History`).
- Stats: uses `PlayerService.get_player_stats()` which caches nflreadpy responses keyed by player/seasons. Data is aggregated by season to produce the OOTP-style grid plus derived rating summaries (20–80 scale) via `PlayerService.get_basic_ratings()`.
- Personal details: reconciles duplicate identifier fields from nflverse (team, handedness, college, contract placeholders) and formats them for the left-hand panels.
- Placeholder panels are already wired so we can drop in contract/injury/history widgets without restructuring the page.

## Data/service layer

- `PlayerDirectory` caches `nflreadpy.load_players()` on first use, coercing Pandas ↔︎ Polars as needed.
- `PlayerService` exposes the app-facing API: search summaries, instantiate `Player` objects, cached stats fetches, rating baselines (computed from multi-season aggregates), and light utility helpers.
- Heavy nflverse payloads are cached in memory per session; we avoid writing to disk beyond nflreadpy’s internal cache.
- Contract/injury data is currently sparse – UI shows placeholders until we wire an external provider.

## Pro-Football-Reference scraping helpers

PFR data is collected via the modules under `down_data/data/pfr/`:

- `client.PFRClient` – polite `requests` session with user-agent, caching, and delay controls.
- `html.py` – resilient table extraction (handles comment-wrapped tables, multi-index headers, duplicate column names).
- `league.py`, `teams.py`, `players.py` – convenience wrappers that convert tables into Polars frames.

Coach-specific utilities live in `down_data/data/pfr/players.py` (for player/coach tables) and share the same HTML helpers. All scraper surfaces are covered by unit tests in `tests/test_pfr_html_utils.py`, `tests/test_pfr_players.py`, `tests/test_pfr_teams.py`, and `tests/test_pfr_league.py`:

```bash
python -m unittest tests.test_pfr_html_utils tests.test_pfr_players tests.test_pfr_teams tests.test_pfr_league
```

Refer to those tests for example usage (e.g., fetching coach tables by ID, pulling team schedules, or loading league-wide advanced stats).

## Styling, architecture, and design resources

- `docs/ui/GRID_SYSTEM.md` – coordinate system and usage.
- `docs/ui/UI_DESIGN_GUIDE.md` – colour tokens, typography, spacing.
- `docs/ui/PANEL_ARCHITECTURE.md` & `docs/ui/COMPONENT_REUSABILITY_GUIDE.md` – compose new widgets consistently.
- `docs/ARCHITECTURE.md` – layer diagram, data flow, caching strategy, and extension playbook.

Apply new palettes or typography by editing `down_data/ui/styles.py` (the palette is applied globally via `apply_app_palette` in `app.py`).

## Development workflow

- Desktop hot reload is not available; iterate by restarting `python main.py`.
- Keep UI work inside `down_data/ui/**`; backend/data logic belongs in `down_data/backend/**` or `down_data/core/**`.
- When adding a page:
  1. Create a `SectionPage` subclass under `down_data/ui/pages/`.
  2. Register it in `ContentPage.NAVIGATION_MAP` and instantiate it in `_build_content_area()`.
  3. Provide a `service: PlayerService` dependency if the page needs nflverse data.
- For experiments that require the domain layer without UI, use `python -m down_data.cli` or import `down_data.core.Player` directly in a REPL.

## Known gaps / next steps

- Contract and injury sections currently surface placeholder copy; integrate Spotrac/OTC (or local fixtures) and backfill the associated filters.
- Search runs synchronously on the UI thread; move the heavier filtering work into a background worker (`QThreadPool + QRunnable`) and debounce the search button.
- Player detail stats support seasonal aggregates; weekly game logs, streaks, and accomplishments should reuse the same service/cache once implemented.
- Teams/Coaches menus route to placeholder pages. When implementing, replicate the same navigation/context pattern to keep history coherent.

## Credits

Built on top of the nflverse ecosystem (`nflreadpy`, `polars`) and influenced by the original OOTP26 desktop UI.
