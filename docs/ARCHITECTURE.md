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

* `BasicOffenseStatsRepository` – qb-focused parquet cache (historical seasons).
* `BasicPlayerStatsRepository` – new general-purpose cache wrapper
  (`data/basic_cache.py`) so every position can hydrate season rows without
  touching the network.
* Additional repositories should follow the same template:
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

* `down_data/data/basic_cache.py` – build/scan the all-player season cache.
* `down_data/data/basic_offense.py` – qb/offense-specific aggregation.
* `down_data/data/pfr/*` – HTML scrapers and tests.
* `scripts/build_basic_cache.py` – CLI hook to prebuild caches.

When introducing a new long-lived dataset, add a module under `down_data/data/`
plus a repository facade under `down_data/backend/`.

---

## Cross-Cutting Concerns

### Caching Strategy

| Layer | Cache | Notes |
| --- | --- | --- |
| UI session | `PlayerService._stats_cache` | in-memory Polars keyed by player+filters |
| Disk | `basic_offense` / `basic_cache` | Parquet aggregates served via repositories |
| nflreadpy | built-in | first network fetch seeds `%APPDATA%`/`~/.cache` |

Always attempt caches in this order: repository → service cache → remote.

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
