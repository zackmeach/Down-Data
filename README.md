# Down-Data Player Explorer

## Overview

Down-Data is a desktop NFL data explorer inspired by the Out of the Park Baseball interface. Built with PySide6 and featuring a hierarchical navigation system with a responsive grid layout, it provides a clean, modern interface for exploring NFL player data, teams, and coaching information.

### Key Features

- **Hierarchical Navigation**: Two-tier menu system (Teams/Players/Coaches → context-specific submenus)
- **Responsive Grid Layout**: 12-column × 24-row grid system for predictable, scalable UI layouts
- **OOTP-Inspired Design**: Dark theme with navigation bars, context bar, and panel-based content areas
- **Dynamic Page Switching**: Content updates based on menu selections
- **Clean Architecture**: Separation between UI, backend services, and domain objects
- **CLI Support**: Original Rich-powered CLI preserved for power users

### Project Structure

- `main.py`: Launches the Qt desktop client with hierarchical navigation
- `down_data/core/player.py`: Core `Player` domain object with rich data capabilities
- `down_data/backend/`: Services that translate UI requests into domain operations
- `down_data/ui/`: Widgets, pages, grid layout system, and styling for the Qt application
  - `pages/content_page.py`: Main navigation controller with two-tier menu system
  - `widgets/`: Reusable components (`nav_bar`, `menu_bar`, `context_bar`, `panel`, `table_panel`, grid helpers)
- `docs/ui/`: Design notes covering grid guidelines, panel architecture, and reusable component patterns
- `down_data/cli.py`: Rich-powered CLI for terminal-based exploration
- `data/`, `agentic-feedback/`, `ootp-images/`: Assets, references, and development notes

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

The application launches with a hierarchical navigation system:
- **Top NavBar**: Global navigation (FILE, GAME, etc.)
- **Context Bar**: Logo, title, schedule selector, and CONTINUE button
- **Main Menu**: Teams, Players, Coaches
- **Secondary Menu**: Context-specific options (e.g., Find A Player, NFL Player List, Compare Players)
- **Content Area**: Dynamic pages based on menu selection

Navigate by clicking menu items—the secondary menu and content update automatically.

## Run the CLI (optional)

```bash
python -m down_data.cli
```

The terminal interface retains the existing features: resolve a player, preview stats, fetch NextGen data, and export CSVs.

## Repository Layout

```
.
├── main.py                   # Desktop entry point
├── down_data/
│   ├── app.py                # Qt bootstrap helpers
│   ├── cli.py                # Terminal explorer
│   ├── backend/              # Services and data facades
│   │   └── player_service.py
│   ├── core/                 # Domain models (Player, profiles, queries)
│   │   └── player.py
│   └── ui/                   # Qt UI components
│       ├── main_window.py    # Main application window
│       ├── styles.py         # Application styling (dark theme)
│       ├── pages/            # UI pages/screens
│       │   ├── base_page.py
│       │   ├── content_page.py       # Main navigation controller
│       │   ├── player_search_page.py
│       │   └── placeholder_page.py
│       └── widgets/          # Reusable UI components (nav bar, menu bar, context bar, grid helpers, panels, tables)
├── docs/
│   └── ui/                   # Grid/panel design guides and reusable-component notes
├── data/                     # Local data workspace
├── ootp-images/              # UI inspiration screenshots
├── agentic-feedback/         # Development notes
└── requirements.txt
```

## Grid Layout System

The application uses a **12-column × 24-row responsive grid system** for all UI layouts. This ensures consistent, predictable positioning that scales dynamically with window size.

### Quick Start

```python
from down_data.ui.widgets import GridLayoutManager, GridCell

# Create grid layout
grid = GridLayoutManager(parent=self, columns=12, rows=24)

# Position a widget
cell = GridCell(col=2, row=5, col_span=8, row_span=3)
grid.add_widget(my_widget, cell)
```

### Documentation

- **`docs/ui/GRID_SYSTEM.md`**: Complete grid API reference and best practices
- **`docs/ui/GRID_REFERENCE.md`**: Visual guide to grid coordinates and communication formats
- **`docs/ui/GRID_IMPLEMENTATION_SUMMARY.md`**: Technical implementation details
- **`docs/ui/PANEL_ARCHITECTURE.md`** & **`docs/ui/COMPONENT_REUSABILITY_GUIDE.md`**: Guidelines for building reusable panels and tables
- **`docs/ui/UI_DESIGN_GUIDE.md`**: Brand and styling reference for OOTP-inspired layouts

## Navigation System

The application uses a two-tier hierarchical navigation:

1. **Main Menu** (row 3): High-level sections
   - Teams
   - Players (default)
   - Coaches

2. **Secondary Menu** (row 4): Context-specific options
   - When "Players" is selected: Find A Player, NFL Player List, Compare Players
   - Other sections have placeholder submenus (Temp1-6)

Clicking a main menu item updates the secondary menu options. Clicking a secondary menu item loads the corresponding page in the content area.

## Adding New Pages

To add a new page to the navigation:

1. Create your page class in `down_data/ui/pages/` (inherit from `SectionPage`)
2. Add it to the `NAVIGATION_MAP` in `content_page.py`
3. Instantiate it in `_build_content_area()` and add to `self._pages`
4. The navigation system will automatically wire it up

Example:
```python
# In content_page.py NAVIGATION_MAP
"Players": {
    "Find A Player": "players_find",
    "Your New Page": "players_new",  # Add this
}

# In _build_content_area()
self._pages["players_new"] = YourNewPage(service=self._service, parent=self._content_container)
```

## Next Steps

- Build out player search UX with filters and results table
- Add player profile, stats, and history pages
- Implement Teams and Coaches sections
- Integrate real data from nflreadpy

## Credits

Built on top of the excellent nflverse ecosystem and `nflreadpy` for data access.
