# Down-Data Player Explorer (OOTP-style)

## Overview

Down-Data is evolving into a desktop NFL data explorer inspired by the Out of the Park interface flow. The project now ships with a PySide6-powered shell that focuses on the player search experience, plus a clean separation between UI, backend services, and the original `Player` domain object.

- `main.py`: Launches the Qt desktop client (player search is the initial landing view).
- `down_data/core/player.py`: Home of the rich `Player` object, unchanged in capability and ready for reuse in the UI.
- `down_data/backend/`: Facades and services that translate UI requests into `Player` operations.
- `down_data/ui/`: Widgets, pages, and styling for the Qt application with placeholders for each planned screen.
- `down_data/cli.py`: The original Rich-powered CLI is preserved for power users and quick testing.
- `data/`, `agentic-feedback/`, `ootp-images/`: Existing assets remain untouched for reference and future development.

## Install

- Install Python 3.10+.
- Install dependencies:

```bash
pip install -r requirements.txt
```

PySide6 is now required for the desktop shell; the CLI continues to work with the same environment.

## Run the desktop app

```bash
python main.py
```

The main window presents a navigation rail on the left and a player search workspace on the right. The search form wires into `PlayerService` for real data when available; the other sections display styled placeholders that map to the `ootp-images` inspiration screens.

## Run the CLI (optional)

```bash
python -m down_data.cli
```

The terminal interface retains the existing features: resolve a player, preview stats, fetch NextGen data, and export CSVs.

## Repository layout

```
.
├── main.py                   # Desktop entry point
├── down_data/
│   ├── app.py                # Qt bootstrap helpers
│   ├── cli.py                # Terminal explorer (legacy but maintained)
│   ├── backend/              # Services and data facades
│   ├── core/                 # Domain models (Player, profiles, queries)
│   └── ui/                   # Qt widgets, pages, styles
├── data/                     # Local data workspace
├── ootp-images/              # UI inspiration screenshots
├── agentic-feedback/         # Brainstorming notes
└── requirements.txt
```

## Next steps

- Flesh out the player search UX (results styling, keyboard shortcuts, live feedback).
- Replace placeholder screens with purpose-built widgets referencing the `ootp-images` mocks.
- Introduce shared view models/controllers to drive navigation between pages.

## Credits

Built on top of the excellent nflverse ecosystem and `nflreadpy` for data access.
