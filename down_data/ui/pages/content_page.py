"""Content page with hierarchical navigation (main menu -> submenu -> content)."""

from __future__ import annotations

from typing import Any, cast

from PySide6.QtWidgets import QLabel, QStackedWidget, QVBoxLayout, QWidget

from down_data.backend.player_service import PlayerService
from down_data.ui.widgets import ContextBar, GridCell, GridLayoutManager, MenuBar, NavBar

from .base_page import SectionPage
from .player_detail_page import PlayerDetailPage
from .placeholder_page import PlaceholderPage
from .player_search_page import PlayerSearchPage


class ContentPage(SectionPage):
    """Host the hierarchical navigation grid and stacked content pages."""

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

    PLAYER_DETAIL_NAVIGATION = {
        "Profile": {
            "Summary": {
                "page_key": "player_detail",
                "view_state": {"section": "Profile", "subsection": "Summary"},
            },
            "Contract": {
                "page_key": "player_detail",
                "view_state": {"section": "Profile", "subsection": "Contract"},
            },
            "Injury History": {
                "page_key": "player_detail",
                "view_state": {"section": "Profile", "subsection": "Injury History"},
            },
        },
        "Stats": {
            None: {
                "page_key": "player_detail",
                "view_state": {"section": "Stats", "subsection": None},
            }
        },
        "History": {
            None: {
                "page_key": "player_detail",
                "view_state": {"section": "History", "subsection": None},
            }
        },
    }

    def __init__(
        self,
        *,
        service: PlayerService,
        parent: QWidget | None = None,
        show_grid_debug: bool = False,
    ) -> None:
        super().__init__(title="Content", parent=parent)
        self._service = service
        self._show_grid_debug = show_grid_debug

        self._history: list[dict[str, Any]] = []
        self._history_index: int = -1
        self._suppress_menu_signals = False
        self._active_context_id: str = "default"
        self._navigation_contexts: dict[str, dict[str, Any]] = {}
        self._active_navigation_map: dict[str, dict[str | None, dict[str, Any]]] = {}

        self._current_main_menu: str | None = None
        self._current_submenu: str | None = None
        self._player_detail_payload: dict[str, Any] | None = None
        self._current_detail_title: str | None = None

        # Create the grid layout manager (12 columns x 24 rows)
        self._grid_layout = GridLayoutManager(parent=self, columns=12, rows=24)

        # Build UI components
        self._build_navigation()
        self._build_content_area()

        # Register navigation contexts and initialise to default view
        self._register_navigation_context(
            "default",
            self._normalize_navigation_tree(self.NAVIGATION_MAP),
            default_main="Players",
            default_submenu="Find A Player",
        )
        self._register_navigation_context(
            "player_detail",
            self._normalize_navigation_tree(self.PLAYER_DETAIL_NAVIGATION),
            default_main="Profile",
            default_submenu="Summary",
        )
        self._apply_navigation_context("default", trigger=True)

    def _build_navigation(self) -> None:
        """Build the navigation bars."""
        # Top NavBar spanning all 12 columns in row 0
        self._nav_bar = NavBar(parent=self)
        self._nav_bar.backRequested.connect(self._on_nav_back)
        self._nav_bar.homeRequested.connect(self._on_nav_home)
        self._nav_bar.forwardRequested.connect(self._on_nav_forward)
        self._grid_layout.add_widget(
            self._nav_bar, GridCell(col=0, row=0, col_span=12, row_span=1)
        )
        self._update_nav_history_buttons()

        # ContextBar spanning rows 1-2
        self._context_bar = ContextBar(title="FIND A PLAYER", parent=self)
        self._grid_layout.add_widget(
            self._context_bar, GridCell(col=0, row=1, col_span=12, row_span=2)
        )

        # Main menu bar at row 3
        self._main_menu = MenuBar(items=[], parent=self)
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
        self._pages: dict[str, QWidget] = {}

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

        detail_page = PlayerDetailPage(service=self._service, parent=self._content_container)
        detail_page.backRequested.connect(self._on_player_detail_back)
        self._pages["player_detail"] = detail_page

        # Add all pages to stack
        for page in self._pages.values():
            self._content_stack.addWidget(page)

        player_search_page = cast(PlayerSearchPage, self._pages["players_find"])
        player_search_page.playerSelected.connect(self._show_player_detail)

        # Add content container to grid (rows 5-23)
        self._grid_layout.add_widget(
            self._content_container, GridCell(col=0, row=5, col_span=12, row_span=19)
        )

    @staticmethod
    def _normalize_navigation_tree(
        tree: dict[str, dict[str | None, Any] | None]
    ) -> dict[str, dict[str | None, dict[str, Any]]]:
        """Ensure navigation tree entries have consistent structure."""

        normalized: dict[str, dict[str | None, dict[str, Any]]] = {}
        for main, submenu_map in tree.items():
            if submenu_map is None:
                normalized[main] = {}
                continue
            normalized_sub: dict[str | None, dict[str, Any]] = {}
            for submenu, target in submenu_map.items():
                if isinstance(target, str):
                    normalized_sub[submenu] = {"page_key": target}
                elif isinstance(target, dict):
                    normalized_sub[submenu] = dict(target)
                else:
                    raise TypeError(f"Unsupported navigation target for {main} -> {submenu}: {target!r}")
            normalized[main] = normalized_sub
        return normalized

    def _register_navigation_context(
        self,
        context_id: str,
        tree: dict[str, dict[str | None, dict[str, Any]]],
        *,
        default_main: str,
        default_submenu: str | None,
    ) -> None:
        """Register a named navigation context."""

        self._navigation_contexts[context_id] = {
            "tree": tree,
            "default_main": default_main,
            "default_submenu": default_submenu,
        }

    def _apply_navigation_context(
        self,
        context_id: str,
        *,
        main_menu: str | None = None,
        submenu: str | None = None,
        trigger: bool = False,
    ) -> None:
        """Activate a navigation context and optionally trigger a navigation event."""

        if context_id not in self._navigation_contexts:
            raise KeyError(f"Unknown navigation context: {context_id}")

        context = self._navigation_contexts[context_id]
        tree: dict[str, dict[str | None, dict[str, Any]]] = context["tree"]

        items = list(tree.keys())
        if not items:
            self._main_menu.set_items([], emit_signal=False)
            self._secondary_menu.set_items([], emit_signal=False)
            self._secondary_menu.setVisible(False)
            self._active_navigation_map = {}
            self._current_main_menu = None
            self._current_submenu = None
            self._active_context_id = context_id
            if context_id != "player_detail":
                self._player_detail_payload = None
                self._current_detail_title = None
            return

        desired_main = main_menu if main_menu in items else context["default_main"]
        if desired_main not in items:
            desired_main = items[0]

        desired_sub: str | None = submenu
        if desired_sub not in (tree.get(desired_main) or {}):
            if desired_main == context["default_main"]:
                desired_sub = context["default_submenu"]
            else:
                desired_sub = None

        self._active_context_id = context_id
        self._active_navigation_map = tree

        # Update main menu items without emitting signals
        default_index = items.index(desired_main)
        self._suppress_menu_signals = True
        try:
            self._main_menu.set_items(items, default_index=default_index, emit_signal=False)
        finally:
            self._suppress_menu_signals = False

        self._select_navigation(desired_main, desired_sub)

        if context_id != "player_detail":
            self._player_detail_payload = None
            self._current_detail_title = None

        if trigger:
            self._show_page_for_selection(desired_main, desired_sub, record_history=True)

    def _ensure_navigation_context(
        self,
        context_id: str,
        main_menu: str | None,
        submenu: str | None,
    ) -> None:
        """Ensure the desired navigation context is active."""

        if self._active_context_id != context_id:
            self._apply_navigation_context(
                context_id,
                main_menu=main_menu,
                submenu=submenu,
                trigger=False,
            )
        else:
            self._select_navigation(main_menu, submenu)

    def _on_main_menu_changed(self, index: int, text: str) -> None:
        """Handle main menu selection change."""
        if self._suppress_menu_signals:
            return
        self._current_main_menu = text
        self._update_secondary_menu(text, default_submenu=None, emit=True)

    def _update_secondary_menu(
        self,
        main_menu_text: str | None,
        *,
        default_submenu: str | None = None,
        emit: bool = True,
    ) -> None:
        """Update secondary menu items based on the active navigation context."""
        if (
            not main_menu_text
            or main_menu_text not in self._active_navigation_map
        ):
            self._secondary_menu.set_items([], emit_signal=False)
            self._secondary_menu.setVisible(False)
            if emit:
                self._current_submenu = None
            return

        submenu_map = self._active_navigation_map.get(main_menu_text) or {}
        visible_items = [item for item in submenu_map.keys() if item is not None]

        self._secondary_menu.setVisible(bool(visible_items))

        default_index = 0
        if default_submenu is not None and default_submenu in visible_items:
            default_index = visible_items.index(default_submenu)

        self._secondary_menu.set_items(
            visible_items,
            default_index=default_index if visible_items else 0,
            emit_signal=emit and bool(visible_items),
        )

        if not visible_items:
            if emit:
                self._show_page_for_selection(main_menu_text, None, record_history=True)
            else:
                self._current_submenu = None

    def _on_secondary_menu_changed(self, index: int, text: str) -> None:
        """Handle secondary menu selection change."""
        if self._suppress_menu_signals:
            return
        self._current_submenu = text
        self._show_page_for_selection(self._current_main_menu, text)

    def _show_page_for_selection(
        self,
        main_menu: str | None,
        submenu: str | None,
        *,
        record_history: bool = True,
    ) -> None:
        """Show the appropriate page for the current menu selection."""

        if not main_menu:
            return

        submenu_map = self._active_navigation_map.get(main_menu)
        if submenu_map is None:
            return

        target = submenu_map.get(submenu)
        selected_submenu = submenu

        if target is None:
            if submenu is not None:
                return
            if None in submenu_map:
                target = submenu_map.get(None)
                selected_submenu = None
            elif submenu_map:
                first_key = next(iter(submenu_map))
                target = submenu_map[first_key]
                selected_submenu = first_key

        if not target:
            return

        page_key = target.get("page_key")
        if not page_key or page_key not in self._pages:
            return

        view_state = target.get("view_state")
        extra_payload = target.get("payload")
        title_override = target.get("title")

        if self._active_context_id == "player_detail":
            payload = dict(extra_payload or {})
            if self._player_detail_payload:
                payload = {**self._player_detail_payload, **payload}
            title = self._current_detail_title or title_override or (selected_submenu or main_menu).upper()
        else:
            payload = extra_payload
            title = title_override or (selected_submenu or main_menu).upper()

        self._navigate_to(
            page_key,
            title=title,
            record_history=record_history,
            main_menu=main_menu,
            submenu=selected_submenu,
            payload=payload,
            view_state=view_state,
            navigation_context=self._active_context_id,
        )

    def _show_player_detail(self, player_payload: dict[str, Any]) -> None:
        """Display the dedicated player detail page."""

        title = str(player_payload.get("full_name") or "Player Detail")
        self._player_detail_payload = dict(player_payload)
        self._current_detail_title = title.upper()
        self._navigate_to(
            "player_detail",
            title=self._current_detail_title,
            record_history=True,
            main_menu="Profile",
            submenu="Summary",
            payload=self._player_detail_payload,
            view_state={"section": "Profile", "subsection": "Summary"},
            navigation_context="player_detail",
        )

    def _on_player_detail_back(self) -> None:
        """Return to the player search results page."""

        self._on_nav_back()

    def _navigate_to(
        self,
        page_key: str,
        *,
        title: str,
        record_history: bool,
        main_menu: str | None = None,
        submenu: str | None = None,
        payload: dict[str, Any] | None = None,
        view_state: dict[str, Any] | None = None,
        navigation_context: str | None = None,
    ) -> None:
        context_id = navigation_context or self._active_context_id
        payload_copy = dict(payload) if isinstance(payload, dict) else payload
        view_state_copy = dict(view_state) if isinstance(view_state, dict) else view_state

        entry = {
            "page_key": page_key,
            "title": title,
            "main_menu": main_menu,
            "submenu": submenu,
            "payload": payload_copy,
            "view_state": view_state_copy,
            "context_id": context_id,
        }
        self._render_entry(entry)
        if record_history:
            if self._history_index < len(self._history) - 1:
                self._history = self._history[: self._history_index + 1]
            self._history.append(entry)
            self._history_index = len(self._history) - 1
        self._update_nav_history_buttons()

    def _render_entry(self, entry: dict[str, Any]) -> None:
        """Render the requested entry without altering history."""
        page_key = entry["page_key"]
        if page_key not in self._pages:
            return

        context_id = entry.get("context_id") or "default"
        main_menu = entry.get("main_menu")
        submenu = entry.get("submenu")

        self._ensure_navigation_context(context_id, main_menu, submenu)

        if page_key == "player_detail":
            payload = dict(entry.get("payload") or self._player_detail_payload or {})
            detail_page = cast(PlayerDetailPage, self._pages["player_detail"])
            if payload:
                self._player_detail_payload = dict(payload)
            self._current_detail_title = entry.get("title") or self._current_detail_title
            view_state = entry.get("view_state") or {}
            detail_page.display_player(payload)
            detail_page.set_view_state(
                view_state.get("section", "Profile"),
                view_state.get("subsection"),
            )
        else:
            if context_id != "player_detail":
                self._player_detail_payload = None
                self._current_detail_title = None

        widget = self._pages[page_key]
        self._content_stack.setCurrentWidget(widget)
        self._context_bar.set_title(entry["title"])

    def _select_navigation(self, main_menu: str | None, submenu: str | None) -> None:
        """Synchronise menu highlights without triggering new navigation."""
        self._suppress_menu_signals = True
        try:
            if main_menu and main_menu in self._active_navigation_map:
                if self._current_main_menu != main_menu:
                    self._main_menu.select_text(main_menu, emit_signal=False)
                self._current_main_menu = main_menu
                self._update_secondary_menu(main_menu, default_submenu=submenu, emit=False)
                if submenu:
                    self._secondary_menu.select_text(submenu, emit_signal=False)
                    self._current_submenu = submenu
                else:
                    self._current_submenu = None
            else:
                self._current_main_menu = None
                self._current_submenu = None
                self._secondary_menu.set_items([], emit_signal=False)
                self._secondary_menu.setVisible(False)
        finally:
            self._suppress_menu_signals = False

    def _on_nav_back(self) -> None:
        if self._history_index <= 0:
            return
        self._history_index -= 1
        entry = self._history[self._history_index]
        self._render_entry(entry)
        self._update_nav_history_buttons()

    def _on_nav_forward(self) -> None:
        if self._history_index >= len(self._history) - 1:
            return
        self._history_index += 1
        entry = self._history[self._history_index]
        self._render_entry(entry)
        self._update_nav_history_buttons()

    def _on_nav_home(self) -> None:
        current_entry = self._history[self._history_index] if 0 <= self._history_index < len(self._history) else None
        if current_entry and current_entry.get("page_key") == "players_find":
            self._apply_navigation_context("default", main_menu="Players", submenu="Find A Player", trigger=False)
            self._context_bar.set_title("FIND A PLAYER")
            return
        self._navigate_to(
            "players_find",
            title="FIND A PLAYER",
            record_history=True,
            main_menu="Players",
            submenu="Find A Player",
            payload=None,
            navigation_context="default",
        )

    def _update_nav_history_buttons(self) -> None:
        can_back = self._history_index > 0
        can_forward = self._history_index < len(self._history) - 1
        if hasattr(self, "_nav_bar"):
            self._nav_bar.set_history_enabled(can_go_back=can_back, can_go_forward=can_forward)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        """Handle resize events."""
        super().resizeEvent(event)
        if hasattr(self, "_grid_layout"):
            self._grid_layout.update_layout()

