"""Content page with hierarchical navigation (main menu -> submenu -> content)."""

from __future__ import annotations

from typing import Dict, Optional

from PySide6.QtWidgets import QLabel, QStackedWidget, QVBoxLayout, QWidget

from down_data.backend.player_service import PlayerService
from down_data.ui.widgets import ContextBar, GridCell, GridLayoutManager, MenuBar, NavBar

from .base_page import SectionPage
from .placeholder_page import PlaceholderPage
from .player_search_page import PlayerSearchPage


class ContentPage(SectionPage):
    """Main content page with hierarchical navigation system.
    
    Navigation structure:
    - Top: NavBar (global navigation)
    - Row 1-2: ContextBar (logo, title, schedule, continue)
    - Row 3: Main menu (Teams, Players, Coaches)
    - Row 4: Secondary menu (context-specific options)
    - Row 5+: Content area (dynamic pages based on selection)
    """

    # Define navigation structure: main_menu -> {submenu_items: page_key}
    NAVIGATION_MAP = {
        "Teams": {
            "Temp1": "teams_temp1",
            "Temp2": "teams_temp2",
            "Temp3": "teams_temp3",
        },
        "Players": {
            "Find A Player": "players_find",
            "NFL Player List": "players_list",
            "Compare Players": "players_compare",
        },
        "Coaches": {
            "Temp4": "coaches_temp4",
            "Temp5": "coaches_temp5",
            "Temp6": "coaches_temp6",
        },
    }

    def __init__(
        self,
        *,
        service: PlayerService,
        parent: Optional[QWidget] = None,
        show_grid_debug: bool = False,
    ) -> None:
        super().__init__(title="Content", parent=parent)
        self._service = service
        self._show_grid_debug = show_grid_debug

        # Create the grid layout manager (12 columns x 24 rows)
        self._grid_layout = GridLayoutManager(parent=self, columns=12, rows=24)

        # Track current navigation state
        self._current_main_menu = "Players"  # Default
        self._current_submenu = "Find A Player"  # Default

        # Build UI components
        self._build_navigation()
        self._build_content_area()

        # Initialize navigation to default
        self._update_secondary_menu(self._current_main_menu)

    def _build_navigation(self) -> None:
        """Build the navigation bars."""
        # Top NavBar spanning all 12 columns in row 0
        self._nav_bar = NavBar(parent=self)
        self._grid_layout.add_widget(
            self._nav_bar, GridCell(col=0, row=0, col_span=12, row_span=1)
        )

        # ContextBar spanning rows 1-2
        self._context_bar = ContextBar(title="FIND A PLAYER", parent=self)
        self._grid_layout.add_widget(
            self._context_bar, GridCell(col=0, row=1, col_span=12, row_span=2)
        )

        # Main menu bar at row 3
        self._main_menu = MenuBar(
            items=list(self.NAVIGATION_MAP.keys()),
            parent=self,
            default_index=1,  # Default to "Players"
        )
        self._main_menu.selectionChanged.connect(self._on_main_menu_changed)
        self._grid_layout.add_widget(
            self._main_menu, GridCell(col=0, row=3, col_span=12, row_span=1)
        )

        # Secondary menu bar at row 4
        self._secondary_menu = MenuBar(items=[], parent=self)
        self._secondary_menu.selectionChanged.connect(self._on_secondary_menu_changed)
        self._grid_layout.add_widget(
            self._secondary_menu, GridCell(col=0, row=4, col_span=12, row_span=1)
        )

    def _build_content_area(self) -> None:
        """Build the content area with stacked pages."""
        # Create a container widget for the content area (rows 5-23)
        self._content_container = QWidget(self)
        content_layout = QVBoxLayout(self._content_container)
        content_layout.setContentsMargins(0, 0, 0, 0)

        # Stacked widget to switch between pages
        self._content_stack = QStackedWidget()
        content_layout.addWidget(self._content_stack)

        # Create all pages and map them
        self._pages: Dict[str, QWidget] = {}

        # Players pages
        self._pages["players_find"] = PlayerSearchPage(
            service=self._service, parent=self._content_container
        )
        self._pages["players_list"] = PlaceholderPage(
            "NFL Player List", parent=self._content_container
        )
        self._pages["players_compare"] = PlaceholderPage(
            "Compare Players", parent=self._content_container
        )

        # Teams placeholder pages
        self._pages["teams_temp1"] = PlaceholderPage(
            "Teams - Temp1", parent=self._content_container
        )
        self._pages["teams_temp2"] = PlaceholderPage(
            "Teams - Temp2", parent=self._content_container
        )
        self._pages["teams_temp3"] = PlaceholderPage(
            "Teams - Temp3", parent=self._content_container
        )

        # Coaches placeholder pages
        self._pages["coaches_temp4"] = PlaceholderPage(
            "Coaches - Temp4", parent=self._content_container
        )
        self._pages["coaches_temp5"] = PlaceholderPage(
            "Coaches - Temp5", parent=self._content_container
        )
        self._pages["coaches_temp6"] = PlaceholderPage(
            "Coaches - Temp6", parent=self._content_container
        )

        # Add all pages to stack
        for page in self._pages.values():
            self._content_stack.addWidget(page)

        # Add content container to grid (rows 5-23)
        self._grid_layout.add_widget(
            self._content_container, GridCell(col=0, row=5, col_span=12, row_span=19)
        )

    def _on_main_menu_changed(self, index: int, text: str) -> None:
        """Handle main menu selection change."""
        self._current_main_menu = text
        self._update_secondary_menu(text)

    def _update_secondary_menu(self, main_menu_text: str) -> None:
        """Update secondary menu items based on main menu selection."""
        if main_menu_text in self.NAVIGATION_MAP:
            submenu_items = list(self.NAVIGATION_MAP[main_menu_text].keys())
            self._secondary_menu.set_items(submenu_items, default_index=0)
            # The set_items will trigger selectionChanged, which will update content

    def _on_secondary_menu_changed(self, index: int, text: str) -> None:
        """Handle secondary menu selection change."""
        self._current_submenu = text
        self._show_page_for_selection(self._current_main_menu, text)

    def _show_page_for_selection(self, main_menu: str, submenu: str) -> None:
        """Show the appropriate page for the current menu selection."""
        if main_menu in self.NAVIGATION_MAP:
            page_key = self.NAVIGATION_MAP[main_menu].get(submenu)
            if page_key and page_key in self._pages:
                self._content_stack.setCurrentWidget(self._pages[page_key])
                # Update context bar title
                self._context_bar.set_title(submenu.upper())

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        """Handle resize events."""
        super().resizeEvent(event)
        if hasattr(self, "_grid_layout"):
            self._grid_layout.update_layout()

