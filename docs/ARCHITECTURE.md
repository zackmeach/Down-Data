# Architecture Overview

This document codifies how the Down-Data Player Explorer is structured today and
sets the guardrails for future work. Treat it as the canonical description of
layer boundaries, data flow, concurrency considerations, and extension points.

---

## Guiding Principles

1. **Layered responsibilities** – Presentation code (PySide6 widgets/pages)
   never touches nflverse/persistence directly. All external access funnels
   through backend services which wrap the domain layer (`down_data.core`).
2. **Deterministic data flow** – UI → `PlayerService` → repositories/caches →
   third-party sources. Keep plumbing uni-directional to simplify caching and
   testing.
3. **Thread-safe UI** – Any expensive I/O (nflreadpy fetches, Polars
   aggregations) must execute on a worker thread; UI slots simply react to
   signals and render already-prepared data.
4. **Cache-first mindset** – Disk-backed Parquet caches (`basic_cache`,
   `basic_offense`) should satisfy most requests before we fall back to remote
   calls.
5. **Composable widgets** – Pages assemble reusable panels, tables, and grid
   helpers. New UIs are built by composing components, not by introducing bespoke
   layouts.

---

## Layer Diagram

```text
PySide6 UI (down_data/ui)          Presentation, navigation, widgets
        │
PlayerService facade (backend)     API surface for UI; orchestrates repositories
        │
Repositories (backend/*_repository.py)  Cached parquet / nflverse adapters
        │
Domain objects (down_data/core)    Player, PlayerProfile, ratings, nflreadpy glue
        │
Data providers (down_data/data/**, nflreadpy, PFR scrapers)
```

Dependencies only flow downward; higher layers never import inward.

---

## Presentation Layer

### Navigation shell (`down_data/ui/pages/content_page.py`)

* Maintains navigation contexts (`default`, `player_detail`) and history.
* Registers every page inside a `QStackedWidget`.
* Emits `playerSelected` payloads directly into the detail context.

### Pages (`down_data/ui/pages/`)

* `player_search_page.py` – filter grids, table pagination, emits selections.
* `player_detail_page.py` – sectioned scaffold that now streams data in two
  phases:
  1. Immediate base stats from caches (season aggregates, no EPA/WPA).
  2. Deferred advanced metrics via a `QThreadPool` worker.
* Upcoming sections (Contract, Injury, History) plug into the same scaffold.

### Widgets (`down_data/ui/widgets/`)

* Grid/panel primitives, navbar/context bar, table wrappers.
* Keep these stateless and reusable; pass data in via setters.

#### Threading contract

* All blocking work is wrapped in `QRunnable` workers (see player detail).
* Signals carry immutable dataclasses (`PlayerDetailComputationResult`) back to
  the GUI thread.
* Future async features (search filtering, team dashboards) should copy the same
  pattern: gather payload → launch worker → update UI in the slot.

---

## Backend Service Layer

### `PlayerService`

* Entry point for UI logic:
  * Player search directory (`PlayerDirectory` cached Polars frame).
  * Creating `Player` domain objects (with nflreadpy inside).
  * Stats caches (`get_player_stats`, `get_basic_offense_stats`,
    `get_basic_player_stats`).
  * Impact metrics (EPA/WPA by role), ratings baseline computation, schedule
    helpers.
* All nflreadpy interactions are cached per app session. Methods return cloned
  Polars frames so callers cannot mutate internals.

### Repositories

**Primary (Preferred):**

* `NFLDataRepository` – Unified access to the NFL Data Store. Provides:
  * `get_player()` / `get_players()` – Static player info
  * `get_player_seasons()` – Season-level statistics
  * `get_player_impacts()` – EPA/WPA metrics
  * `get_player_summary()` – Combined data for UI
  * `get_player_bio()` – Birthplace, handedness (static data)

**Legacy (Fallback):**

* `BasicOffenseStatsRepository` – qb-focused parquet cache (historical seasons).
* `BasicPlayerStatsRepository` – general-purpose cache wrapper
  (`data/basic_cache.py`).
* `PlayerSummaryRepository` – merged cache with stats + impacts.
* `PlayerImpactRepository` – EPA/WPA cache from play-by-play.

All repositories follow the template:
  * `ensure_cache()` – build/refresh if missing.
  * `scan()`/`load()` – lazy vs eager access.
  * `query()` – typed filters (player IDs, seasons, team, position).

---

## Domain Layer (`down_data/core`)

* `Player` – wraps nflreadpy + cached results + PFR helpers. All validation,
  next-gen utilities, ratings logic live here.
* `PlayerProfile`, `PlayerQuery`, `RatingBreakdown` – immutable dataclasses.
* Team normalization (`TeamDirectory`) is shared by both UI layers and CLI.

Guideline: keep pure domain logic (formatting, derived stats, scraping) here so
it can be reused by CLI/tests without Qt.

---

## Data Providers

### NFL Data Store (`down_data/data/nfl_datastore.py`) – **Primary Data System**

The NFL Data Store is the preferred, structured database system for NFL player
data. Unlike legacy caches, it is designed to be:

* **Persistent** – Data is intentionally stored and managed, not temporary.
* **Structured** – Well-defined schemas with known features and date ranges.
* **Updateable** – Can expand date ranges (e.g., 1999-2024 → 1999-2025).
* **Refreshable** – Any subset of data can be updated independently.
* **Efficient** – Avoids redundant fetching (birthplace doesn't change yearly).
* **Error-aware** – Graceful error handling with flagging for review.

**Data Tables:**

| Table | Path | Description |
| --- | --- | --- |
| `players` | `data/nflverse/players.parquet` | Static player info (bio, birthplace, college, draft) |
| `player_seasons` | `data/nflverse/player_seasons.parquet` | Season-level statistics (games, snaps, stats) |
| `player_impacts` | `data/nflverse/player_impacts.parquet` | EPA/WPA metrics by player-season |
| `metadata` | `data/nflverse/metadata.json` | Schema version, date ranges, error log |

**Key Classes:**

* `NFLDataStore` – Core data access and storage manager.
* `NFLDataBuilder` – Orchestrates data refresh from nflverse sources.
* `NFLDataRepository` (`backend/nfl_data_repository.py`) – Clean query interface.

**Build Command:**

```bash
python scripts/build_nfl_datastore.py                    # Full build
python scripts/build_nfl_datastore.py --seasons 2024    # Update single season
python scripts/build_nfl_datastore.py --status          # Check status
```

### Legacy Cache System (deprecated, still functional)

* `down_data/data/basic_cache.py` – build/scan the all-player season cache.
* `down_data/data/basic_offense.py` – qb/offense-specific aggregation.
* `down_data/data/pfr/*` – HTML scrapers and tests.
* `scripts/build_basic_cache.py` – CLI hook to prebuild caches.

**Migration Note:** The system automatically prefers the new NFL Data Store when
available, falling back to legacy caches. Build the new store with
`scripts/build_nfl_datastore.py` to take advantage of the improved architecture.

When introducing a new long-lived dataset, add a module under `down_data/data/`
plus a repository facade under `down_data/backend/`.

---

## Cross-Cutting Concerns

### Data Storage Strategy

| Layer | Storage | Notes |
| --- | --- | --- |
| UI session | `PlayerService._stats_cache` | in-memory Polars keyed by player+filters |
| **NFL Data Store** | `data/nflverse/*.parquet` | **Preferred** – structured player database |
| Legacy disk | `data/cache/*.parquet` | Deprecated – basic_offense / basic_cache |
| nflreadpy | built-in | first network fetch seeds `%APPDATA%`/`~/.cache` |

**Data Access Priority:**

1. NFL Data Store (if initialized) – Unified, structured data
2. Legacy repositories (fallback) – For backward compatibility
3. Service session cache – In-memory for repeated queries
4. Remote nflverse – Only when local data unavailable

**Efficiency Features:**

The NFL Data Store separates static and dynamic data:
- **Static** (players table): Bio, birthplace, college – fetched once
- **Dynamic** (player_seasons table): Stats that change yearly
- **Computed** (player_impacts table): EPA/WPA metrics from play-by-play

This prevents redundant fetching (e.g., birthplace doesn't change year-to-year).

### Concurrency

* Search page: TODO – move `_filter_players_by_criteria` into a `QRunnable`.
* Detail page: implemented – base stats worker + optional advanced worker.
* Additional long-running jobs (e.g., team comparisons) must adhere to the
  pattern documented above.

### Testing

* Backend/data code has unit coverage under `tests/`.
* UI currently lacks automated tests; rely on manual QA plus targeted unit tests
  for data plumbing.
* When adding repositories/services, include focused unit tests per module.

---

## Extension Playbook

1. **Need a new dataset?** Implement under `down_data/data/…` and expose via a
   backend repository. UI should only talk to repositories through
   `PlayerService` (or a future sibling service).
2. **New UI page?** Subclass `SectionPage`, register it in
   `ContentPage.NAVIGATION_MAP`, and inject `PlayerService`.
3. **Heavy computation?** Define a dataclass for results, extend the worker
   pattern (signals + background aggregation), update UI in the slot.
4. **Feature flagging?** Prefer payload-driven toggles (e.g., pass
   `fetch_impacts=False` for base load) rather than global state.

---

## Known Improvements (roadmap)

* Move player search filtering onto a worker thread (mirror the detail page).
* Encapsulate schedule/team record helpers into a `ScheduleRepository`.
* Introduce a lightweight IOC container so services/widgets request
  dependencies explicitly (helps future tests/headless modes).
* Expand architectural docs with sequence diagrams once contract/injury pages
  are implemented.

---

Architecture stewardship is an ongoing process. If you touch a new layer or
introduce a dependency that crosses boundaries, update this document so the next
developer can reason about the system quickly.
