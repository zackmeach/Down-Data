"""Temporary player detail page shown from the search results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import logging
import math
from typing import Any, Iterable

import polars as pl
from PySide6.QtCore import Qt, Signal, QObject, QRunnable, QThreadPool
from PySide6.QtWidgets import (
    QLabel,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
    QSizePolicy,
    QHeaderView,
)

from down_data.backend.player_service import PlayerService
from down_data.core import Player, PlayerQuery, PlayerNotFoundError, SeasonNotAvailableError
from down_data.ui.widgets import (
    GridCell,
    GridLayoutManager,
    Panel,
    TablePanel,
    PersonalDetailsWidget,
    BasicRatingsWidget,
)
from down_data.core.ratings import RatingBreakdown

from .base_page import SectionPage


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlayerTypeFlags:
    is_defensive: bool = False
    is_quarterback: bool = False
    is_running_back: bool = False
    is_receiver: bool = False
    is_defensive_front: bool = False
    is_defensive_back: bool = False
    is_offensive_lineman: bool = False
    is_kicker: bool = False
    is_punter: bool = False


@dataclass(frozen=True)
class PlayerDetailComputationResult:
    token: int
    player: Player
    table_columns: list[str]
    season_rows: list[list[str]]
    summary: dict[str, float]
    basic_ratings: list[RatingBreakdown]
    stats_frame: pl.DataFrame
    flags: PlayerTypeFlags
    fetch_impacts: bool


class PlayerDetailWorkerSignals(QObject):
    """Signals emitted by the player detail background worker."""

    def __init__(self) -> None:
        super().__init__()

    finished = Signal(object)
    error = Signal(str)


class PlayerDetailWorker(QRunnable):
    """Background job for loading player stats without blocking the UI thread."""

    def __init__(
        self,
        page: "PlayerDetailPage",
        payload: dict[str, Any],
        token: int,
        *,
        fetch_impacts: bool,
        cached_player: Player | None = None,
        cached_stats: pl.DataFrame | None = None,
    ) -> None:
        super().__init__()
        self._page = page
        self._payload = dict(payload)
        self._token = token
        self._fetch_impacts = fetch_impacts
        self._cached_player = cached_player
        self._cached_stats = cached_stats
        self.signals = PlayerDetailWorkerSignals()

    def run(self) -> None:  # pragma: no cover - executed in worker thread
        try:
            result = self._page._compute_player_detail(  # type: ignore[attr-defined]
                self._payload,
                token=self._token,
                fetch_impacts=self._fetch_impacts,
                cached_player=self._cached_player,
                cached_stats=self._cached_stats,
            )
            self.signals.finished.emit(result)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Player detail worker failed: %s", exc)
            self.signals.error.emit(str(exc))


class PlayerDetailSectionScaffold(QWidget):
    """Reusable section layout with structured panels arranged in the grid."""

    PANEL_LAYOUT = [
        {
            "title": "Personal Details",
            "cell": GridCell(col=0, row=0, col_span=2, row_span=11),
            "variant": "panel",
        },
        {
            "title": "Basic Ratings",
            "cell": GridCell(col=2, row=0, col_span=4, row_span=11),
            "variant": "panel",
        },
        {
            "title": "Summary",
            "cell": GridCell(col=6, row=0, col_span=3, row_span=11),
            "variant": "panel",
        },
        {
            "title": "Position Rankings",
            "cell": GridCell(col=9, row=0, col_span=3, row_span=11),
            "variant": "panel",
        },
        {
            "title": "Status",
            "cell": GridCell(col=0, row=11, col_span=2, row_span=11),
            "variant": "panel",
        },
        {
            "title": None,
            "cell": GridCell(col=2, row=11, col_span=10, row_span=11),
            "variant": "table",
        },
    ]

    PERSONAL_DETAIL_FIELDS = [
        "Age",
        "Date of Birth",
        "City of Birth",
        "Nationality",
        "Primary Position",
        "Handedness",
        "Current Team",
        "Salary (AAV)",
        "Signed Through",
        "College",
        "Service (Years, Games, Snaps)",
    ]

    def __init__(
        self,
        *,
        section: str,
        subsection: str | None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._section = section
        self._subsection = subsection
        self._grid_layout = GridLayoutManager(self, columns=12, rows=24)
        self._panels: list[Panel] = []
        self._table_panel: TablePanel | None = None
        self._personal_details_widget: PersonalDetailsWidget | None = None
        self._basic_ratings_widget: BasicRatingsWidget | None = None
        self._build_panels()

    def _build_panels(self) -> None:
        """Create placeholder panels according to the specified layout."""

        for config in self.PANEL_LAYOUT:
            title = config.get("title")
            cell = config["cell"]
            variant = config.get("variant", "panel")

            if variant == "table":
                panel = TablePanel(
                    title=None,
                    columns=["Season", "Team", "Pos", "GP", "Snps"],
                    sortable=False,
                    alternating_rows=True,
                    parent=self,
                )
                panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

                table = panel.table
                table.horizontalHeader().setStretchLastSection(True)
                table.verticalHeader().hide()
                table.setShowGrid(False)
                table.setRowCount(0)
                if hasattr(table, "setPlaceholderText"):
                    table.setPlaceholderText("No regular-season stats available.")

                self._grid_layout.add_widget(panel, cell)
                self._panels.append(panel)
                self._table_panel = panel
                continue

            panel_title = title.upper() if title else None
            panel = Panel(title=panel_title, parent=self)
            panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

            if title == "Personal Details":
                widget = PersonalDetailsWidget(self.PERSONAL_DETAIL_FIELDS, parent=panel)
                panel.content_layout.addWidget(widget)
                panel.content_layout.addStretch(1)
                self._personal_details_widget = widget
            elif title == "Basic Ratings":
                widget = BasicRatingsWidget(parent=panel)
                panel.content_layout.addWidget(widget)
                self._basic_ratings_widget = widget
            else:
                placeholder = QLabel("CONTENT COMING SOON", panel)
                placeholder.setAlignment(Qt.AlignCenter)
                placeholder.setStyleSheet(
                    "color: #7A8894; font-size: 14px; letter-spacing: 1px;"
                )

                panel.content_layout.addStretch(1)
                panel.content_layout.addWidget(placeholder, alignment=Qt.AlignCenter)
                panel.content_layout.addStretch(1)

            self._grid_layout.add_widget(panel, cell)
            self._panels.append(panel)

        self._grid_layout.update_layout()

    def update_table_rows(self, rows: list[list[str]]) -> None:
        """Populate the embedded table panel with the provided rows."""

        if self._table_panel is None:
            return

        self._table_panel.clear_data()

        if not rows:
            self._table_panel.table.setRowCount(0)
            return

        for row in rows:
            self._table_panel.add_row(row)

    def update_table_columns(self, columns: list[str]) -> None:
        """Update the table header/columns."""
        if self._table_panel is None:
            return
        if not columns:
            columns = ["Season", "Team", "Primary Pos", "Games", "Snaps"]
        self._table_panel.set_columns(columns)
        self._configure_stats_table_header(len(columns))

    def _configure_stats_table_header(self, column_count: int) -> None:
        if self._table_panel is None or column_count <= 0:
            return
        header = self._table_panel.table.horizontalHeader()
        header.setStretchLastSection(False)
        for index in range(column_count):
            mode = QHeaderView.ResizeToContents if index == column_count - 1 else QHeaderView.Stretch
            header.setSectionResizeMode(index, mode)

    def update_personal_details(self, details: list[tuple[str, str]]) -> None:
        if self._personal_details_widget is not None:
            self._personal_details_widget.set_details(details)

    def update_basic_ratings(self, ratings: list[RatingBreakdown]) -> None:
        if self._basic_ratings_widget is not None:
            self._basic_ratings_widget.set_ratings(ratings)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        """Ensure panels stay aligned with the grid when the widget resizes."""
        super().resizeEvent(event)
        self._grid_layout.update_layout()


class PlayerDetailPage(SectionPage):
    """Player detail page scaffold prepared for future specification."""
    PRIMARY_SECTIONS = ["Profile", "Stats", "History"]
    PROFILE_SUBSECTIONS = ["Summary", "Contract", "Injury History"]
    RUNNING_BACK_POSITIONS = {"RB", "FB", "HB"}
    RECEIVER_POSITIONS = {"WR", "TE"}
    OFFENSIVE_LINE_POSITIONS = {"OL", "LT", "LG", "RT", "RG", "C", "T", "G"}
    KICKER_POSITIONS = {"K"}
    PUNTER_POSITIONS = {"P"}
    DEFENSIVE_BACK_POSITIONS = {"DB", "CB", "FS", "SS", "S", "NB"}
    DEFENSIVE_FRONT_POSITIONS = {"DE", "DT", "DL", "NT", "LB", "ILB", "OLB", "MLB", "EDGE"}
    DEFENSIVE_BACK_POSITIONS = {"DB", "CB", "FS", "SS", "S", "NB"}
    DEFENSIVE_FRONT_POSITIONS = {"DE", "DT", "DL", "NT", "LB", "ILB", "OLB", "MLB", "EDGE"}
    RUNNING_BACK_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "Tot TD",
        "Tot Yds",
        "Rush Att",
        "Rush Yds",
        "Rush TD",
        "Rush Y/A",
        "Rush 20+",
        "Targets",
        "Catches",
        "Rec Yds",
        "Rec Y/Rec",
        "Rec TD",
        "Rec 20+",
        "Yds/Tgt",
        "Touches",
        "Yds/Touch",
        "Fum",
    ]
    RECEIVER_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "Tot TD",
        "Tot Yds",
        "Targets",
        "Catches",
        "Rec Yds",
        "Rec Y/Rec",
        "Rec TD",
        "Rec 20+",
        "Catch %",
        "Yds/Tgt",
        "1st Dn",
        "Fum",
    ]
    OFFENSIVE_LINE_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "Off Snps",
        "Off Snps%",
        "Hold",
        "False Start",
        "Decl/Offs",
        "Pen",
    ]
    DEFENSIVE_FRONT_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "Tkl",
        "Solo",
        "Ast",
        "TFL",
        "Sacks",
        "QB Hits",
        "FF",
        "FR",
        "Saf",
        "PD",
        "INT",
        "TD",
    ]
    DEFENSIVE_BACK_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "INT",
        "TD",
        "PD",
        "Tkl",
        "Solo",
        "Ast",
        "TFL",
        "Sacks",
        "QB Hits",
        "FF",
        "FR",
        "Saf",
    ]
    KICKER_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "FGM",
        "FGA",
        "FG%",
        "XPM",
        "XPA",
        "XP%",
        "FG Long",
        "FGM 0-29",
        "FGA 0-29",
        "FGM 30-39",
        "FGA 30-39",
        "FGM 40-49",
        "FGA 40-49",
        "FGM 50-59",
        "FGA 50-59",
        "FGM 60+",
        "FGA 60+",
        "Kickoffs",
        "TB%",
    ]
    PUNTER_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "Punts",
        "Punt Yds",
        "Yds/Punt",
        "Punt Long",
        "Opp Ret Yds",
        "Net Yds",
        "Net Y/Punt",
        "Touchbacks",
        "TB%",
        "In 20",
        "In 20%",
        "Blocked",
    ]
    DEFENSIVE_FRONT_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "Tkl",
        "Solo",
        "Ast",
        "TFL",
        "Sacks",
        "QB Hits",
        "FF",
        "FR",
        "Saf",
        "PD",
        "INT",
        "TD",
    ]
    DEFENSIVE_BACK_COLUMNS = [
        "Season",
        "Age",
        "Team",
        "Tm Rec",
        "GP",
        "Snps",
        "WPA",
        "EPA",
        "INT",
        "TD",
        "PD",
        "Tkl",
        "Solo",
        "Ast",
        "TFL",
        "Sacks",
        "QB Hits",
        "FF",
        "FR",
        "Saf",
    ]

    backRequested = Signal()

    def __init__(
        self,
        *,
        service: PlayerService,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title="Player Detail", parent=parent)
        self.root_layout.setContentsMargins(0, 0, 0, 0)

        self._service = service
        self._season_rows: list[list[str]] = []
        self._table_columns: list[str] = ["Season", "Team", "Pos", "GP", "Snps"]
        self._is_defensive: bool = False
        self._is_quarterback: bool = False
        self._is_running_back: bool = False
        self._is_receiver: bool = False
        self._is_defensive_front: bool = False
        self._is_defensive_back: bool = False
        self._is_offensive_lineman: bool = False
        self._is_kicker: bool = False
        self._is_punter: bool = False
        self._current_player: Player | None = None
        self._basic_ratings: list[RatingBreakdown] = []
        self._thread_pool = QThreadPool.globalInstance()
        self._advanced_worker_running: bool = False
        self._payload_token: int = 0
        self._last_stats_frame: pl.DataFrame | None = None
        self._advanced_metrics_ready: bool = False
        self._loading_message: str | None = None

        self._current_payload: dict[str, Any] | None = None
        self._active_section: str = self.PRIMARY_SECTIONS[0]
        self._active_subsection: str | None = self.PROFILE_SUBSECTIONS[0]

        self._content_stack = QStackedWidget(self)
        self._content_panels: dict[tuple[str, str | None], QWidget] = {}
        self._initialize_content_panels()

        self._content_container = QWidget(self)
        self._content_container.setObjectName("PlayerDetailContent")
        self._content_layout = QVBoxLayout(self._content_container)
        self._content_layout.setContentsMargins(0, 0, 0, 0)
        self._content_layout.setSpacing(0)
        self._content_layout.addWidget(self._content_stack, 1)

        self._back_button = QPushButton("BACK TO SEARCH", self)
        self._back_button.setObjectName("PrimaryButton")
        self._back_button.setMinimumHeight(32)
        self._back_button.clicked.connect(self.backRequested.emit)

        layout = self.root_layout
        layout.addWidget(self._content_container, 1)
        layout.addWidget(self._back_button, alignment=Qt.AlignRight)

        self._update_table_views()

    def display_player(self, payload: dict[str, Any]) -> None:
        """Store the latest payload and clear the content area for forthcoming layout."""

        self._current_payload = dict(payload) if payload is not None else None
        self._current_player = None
        self._season_rows = []
        self._basic_ratings = []
        self._last_stats_frame = None
        self._advanced_metrics_ready = False
        self._advanced_worker_running = False
        self._payload_token += 1
        self._update_personal_details_views()
        self._set_loading_state(True, "Loading player...")
        self._start_player_detail_worker(fetch_impacts=False)
        self._show_content(self._active_section, self._active_subsection)

    def clear_display(self) -> None:
        """Reset any stored payload and ensure the content area is empty."""

        self._payload_token += 1
        self._current_payload = None
        self._season_rows = []
        self._current_player = None
        self._basic_ratings = []
        self._last_stats_frame = None
        self._advanced_worker_running = False
        self._advanced_metrics_ready = False
        self._update_personal_details_views()
        self._update_basic_ratings_views()
        self._update_table_views()
        self._set_loading_state(False)
        self._show_content(self.PRIMARY_SECTIONS[0], self.PROFILE_SUBSECTIONS[0])

    def _initialize_content_panels(self) -> None:
        """Create placeholder panels for each section/subsection combination."""

        for section in self.PRIMARY_SECTIONS:
            if section == "Profile":
                for subsection in self.PROFILE_SUBSECTIONS:
                    widget = self._create_placeholder_panel(section, subsection)
                    key = (section, subsection)
                    self._content_panels[key] = widget
                    self._content_stack.addWidget(widget)
            else:
                widget = self._create_placeholder_panel(section, None)
                key = (section, None)
                self._content_panels[key] = widget
                self._content_stack.addWidget(widget)

    def _create_placeholder_panel(self, section: str, subsection: str | None) -> QWidget:
        """Return a scaffold widget containing placeholder panels."""

        return PlayerDetailSectionScaffold(section=section, subsection=subsection, parent=self)

    def _show_content(self, section: str, subsection: str | None) -> None:
        """Display the placeholder widget for the requested section."""

        section_key = section
        subsection_key = subsection if section == "Profile" else None

        self._active_section = section_key
        self._active_subsection = subsection_key

        key = (section_key, subsection_key)
        widget = self._content_panels.get(key)
        if widget is not None:
            self._content_stack.setCurrentWidget(widget)

    def set_view_state(self, section: str, subsection: str | None) -> None:
        """Update the displayed section/subsection."""

        normalized_section = section if section in self.PRIMARY_SECTIONS else self.PRIMARY_SECTIONS[0]

        if normalized_section == "Profile":
            normalized_subsection = subsection if subsection in self.PROFILE_SUBSECTIONS else self.PROFILE_SUBSECTIONS[0]
        else:
            normalized_subsection = None

        self._show_content(normalized_section, normalized_subsection)

    def _update_table_views(self) -> None:
        """Push the current table rows into every section scaffold."""

        for widget in self._content_panels.values():
            if isinstance(widget, PlayerDetailSectionScaffold):
                widget.update_table_columns(self._table_columns)
                widget.update_table_rows(self._season_rows)

    def _update_personal_details_views(self) -> None:
        details = self._build_personal_detail_rows()
        for widget in self._content_panels.values():
            if isinstance(widget, PlayerDetailSectionScaffold):
                widget.update_personal_details(details)

    def _update_basic_ratings_views(self) -> None:
        ratings = self._basic_ratings if self._basic_ratings else []
        for widget in self._content_panels.values():
            if isinstance(widget, PlayerDetailSectionScaffold):
                widget.update_basic_ratings(ratings)

    def _set_loading_state(self, loading: bool, message: str | None = None) -> None:
        """Track loading state for stats; message reserved for future UI hooks."""

        self._loading_message = message if loading else None
        if loading:
            self._season_rows = []
            self._update_table_views()

    def _start_player_detail_worker(
        self,
        *,
        fetch_impacts: bool,
        cached_player: Player | None = None,
        cached_stats: pl.DataFrame | None = None,
    ) -> None:
        if self._current_payload is None:
            return

        worker = PlayerDetailWorker(
            self,
            self._current_payload,
            self._payload_token,
            fetch_impacts=fetch_impacts,
            cached_player=cached_player,
            cached_stats=cached_stats,
        )
        worker.signals.finished.connect(self._on_player_detail_loaded)
        worker.signals.error.connect(self._on_player_detail_error)
        if fetch_impacts:
            self._advanced_worker_running = True
        self._thread_pool.start(worker)

    def _on_player_detail_loaded(self, payload: object) -> None:
        """Handle completion of the background stats worker."""

        if not isinstance(payload, PlayerDetailComputationResult):
            return
        if payload.token != self._payload_token:
            return  # stale result

        self._current_player = payload.player
        self._apply_player_flags(payload.flags)
        self._season_rows = payload.season_rows
        self._table_columns = payload.table_columns
        self._basic_ratings = payload.basic_ratings
        self._last_stats_frame = payload.stats_frame.clone()

        self._update_table_views()
        self._update_basic_ratings_views()
        self._set_loading_state(False)

        if payload.fetch_impacts:
            self._advanced_worker_running = False
            self._advanced_metrics_ready = True
        else:
            self._advanced_metrics_ready = False
            self._request_advanced_metrics()

    def _on_player_detail_error(self, message: str) -> None:
        """Surface worker errors in logs while keeping UI responsive."""

        logger.debug("Failed to load player details: %s", message)
        self._set_loading_state(False)
        self._advanced_worker_running = False

    def _apply_player_flags(self, flags: PlayerTypeFlags) -> None:
        """Copy computed player-type flags into the page state."""

        self._is_defensive = flags.is_defensive
        self._is_quarterback = flags.is_quarterback
        self._is_running_back = flags.is_running_back
        self._is_receiver = flags.is_receiver
        self._is_defensive_front = flags.is_defensive_front
        self._is_defensive_back = flags.is_defensive_back
        self._is_offensive_lineman = flags.is_offensive_lineman
        self._is_kicker = flags.is_kicker
        self._is_punter = flags.is_punter

    def _request_advanced_metrics(self) -> None:
        """Kick off the deferred EPA/WPA calculation once base stats are shown."""

        if self._advanced_metrics_ready or self._advanced_worker_running:
            return
        if not self._season_rows:
            return
        if self._current_player is None or self._last_stats_frame is None:
            return
        self._start_player_detail_worker(
            fetch_impacts=True,
            cached_player=self._current_player,
            cached_stats=self._last_stats_frame.clone(),
        )

    def _build_personal_detail_rows(self) -> list[tuple[str, str]]:
        payload = self._current_payload or {}
        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}

        details: list[tuple[str, str]] = []

        age_value = payload.get("age") or (raw.get("age") if isinstance(raw, dict) else None)
        details.append(("Age", self._format_optional_int(age_value)))

        birth_value = self._first_non_empty(
            payload.get("birth_date"),
            payload.get("birthdate"),
            raw.get("birth_date") if isinstance(raw, dict) else None,
            raw.get("birthdate") if isinstance(raw, dict) else None,
            raw.get("dob") if isinstance(raw, dict) else None,
        )
        details.append(("Date of Birth", self._format_birth_date(birth_value)))

        birth_city = self._first_non_empty(
            raw.get("birth_city") if isinstance(raw, dict) else None,
            raw.get("birthplace") if isinstance(raw, dict) else None,
            raw.get("birth_place") if isinstance(raw, dict) else None,
            raw.get("city_of_birth") if isinstance(raw, dict) else None,
            payload.get("birth_city"),
        )
        birth_state = self._first_non_empty(
            raw.get("birth_state") if isinstance(raw, dict) else None,
            raw.get("state_of_birth") if isinstance(raw, dict) else None,
            raw.get("birth_state_province") if isinstance(raw, dict) else None,
        )
        birth_country = self._first_non_empty(
            raw.get("birth_country") if isinstance(raw, dict) else None,
            raw.get("country_of_birth") if isinstance(raw, dict) else None,
            payload.get("birth_country"),
        )
        birthplace_parts = [
            part.strip()
            for part in [birth_city or "", birth_state or "", birth_country or ""]
            if isinstance(part, str) and part.strip()
        ]
        details.append(("City of Birth", ", ".join(birthplace_parts) if birthplace_parts else "—"))

        nationality = self._first_non_empty(
            payload.get("nationality"),
            raw.get("nationality") if isinstance(raw, dict) else None,
            raw.get("citizenship") if isinstance(raw, dict) else None,
            birth_country,
        )
        details.append(("Nationality", self._format_text(nationality)))

        position = self._first_non_empty(
            payload.get("position"),
            raw.get("position") if isinstance(raw, dict) else None,
            raw.get("player_position") if isinstance(raw, dict) else None,
        )
        details.append(("Primary Position", self._format_text(position)))

        handedness = self._first_non_empty(
            raw.get("handedness") if isinstance(raw, dict) else None,
            raw.get("throws") if isinstance(raw, dict) else None,
            raw.get("dominant_hand") if isinstance(raw, dict) else None,
            raw.get("hand") if isinstance(raw, dict) else None,
            payload.get("handedness"),
        )
        details.append(("Handedness", self._format_text(handedness)))

        team = self._first_non_empty(
            payload.get("team"),
            payload.get("current_team"),
            raw.get("team_abbr") if isinstance(raw, dict) else None,
            raw.get("current_team_abbr") if isinstance(raw, dict) else None,
            raw.get("recent_team") if isinstance(raw, dict) else None,
            raw.get("team") if isinstance(raw, dict) else None,
        )
        details.append(("Current Team", self._format_text(team)))

        salary_text = self._derive_salary_text(
            payload if isinstance(payload, dict) else {},
            raw if isinstance(raw, dict) else {},
        )
        details.append(("Salary (AAV)", salary_text))

        signed_through = self._first_non_empty(
            payload.get("signed_through"),
            payload.get("contract_end"),
            raw.get("signed_through") if isinstance(raw, dict) else None,
            raw.get("contract_end") if isinstance(raw, dict) else None,
            raw.get("contract_year_to") if isinstance(raw, dict) else None,
            raw.get("contract_years") if isinstance(raw, dict) else None,
        )
        signed_text = self._format_signed_through(signed_through)
        details.append(("Signed Through", signed_text))

        college = self._first_non_empty(
            payload.get("college"),
            raw.get("college") if isinstance(raw, dict) else None,
            raw.get("college_name") if isinstance(raw, dict) else None,
        )
        details.append(("College", self._format_text(college)))

        years_value = self._extract_numeric_value(
            payload,
            ["experience", "service_years", "seasons", "years_exp"],
        )
        if years_value is None and isinstance(raw, dict):
            years_value = self._extract_numeric_value(
                raw,
                [
                    "experience",
                    "service_years",
                    "seasons",
                    "years_exp",
                    "years_pro",
                ],
            )

        games_value = self._extract_numeric_value(
            payload,
            ["career_games", "games", "games_played"],
        )
        if games_value is None and isinstance(raw, dict):
            games_value = self._extract_numeric_value(
                raw,
                ["career_games", "games", "games_played", "games_active"],
            )

        snaps_value = None
        if isinstance(raw, dict):
            snaps_value = self._extract_numeric_value(
                raw,
                [
                    "snaps",
                    "offense_snaps",
                    "defense_snaps",
                    "special_teams_snaps",
                    "snaps_total",
                ],
            )
        if snaps_value is None:
            snaps_value = self._extract_numeric_value(
                payload,
                ["snaps", "career_snaps"],
            )

        service_parts: list[str] = []
        if years_value is not None:
            service_parts.append(f"{int(round(years_value))} yrs")
        if games_value is not None:
            service_parts.append(f"{int(round(games_value))} g")
        if snaps_value is not None:
            service_parts.append(f"{int(round(snaps_value)):,} snaps")
        service_text = ", ".join(service_parts) if service_parts else "—"
        details.append(("Service (Years, Games, Snaps)", service_text))

        return details

    def _compute_player_detail(
        self,
        payload: dict[str, Any],
        *,
        token: int,
        fetch_impacts: bool,
        cached_player: Player | None = None,
        cached_stats: pl.DataFrame | None = None,
    ) -> PlayerDetailComputationResult:
        if self._service is None:
            raise RuntimeError("Player service unavailable.")
        if not payload:
            raise ValueError("Player payload is empty.")

        full_name = self._safe_str(payload.get("full_name") or payload.get("name"))
        if not full_name:
            raise ValueError("Player payload missing full_name.")
        team = self._safe_str(payload.get("team"))
        position = self._safe_str(payload.get("position"))

        query = PlayerQuery(name=full_name, team=team or None, position=position or None)
        player = cached_player or self._service.load_player(query)

        flags = self._determine_player_flags(player)
        table_columns = self._determine_table_columns(flags)

        stats = cached_stats.clone() if cached_stats is not None else self._load_stats_for_player(player, flags)

        if stats.height == 0:
            season_rows: list[list[str]] = []
            summary: dict[str, float] = {}
        else:
            season_rows, summary = self._build_table_rows(player, stats, flags=flags, fetch_impacts=fetch_impacts)

        basic_ratings = self._service.get_basic_ratings(
            player,
            summary=summary,
            is_defensive=flags.is_defensive,
        ) if summary else []

        return PlayerDetailComputationResult(
            token=token,
            player=player,
            table_columns=table_columns,
            season_rows=season_rows,
            summary=summary,
            basic_ratings=basic_ratings,
            stats_frame=stats,
            flags=flags,
            fetch_impacts=fetch_impacts,
        )

    def _determine_player_flags(self, player: Player) -> PlayerTypeFlags:
        position_value = (player.profile.position or player.profile.position_group or "").upper()
        is_defensive = bool(getattr(player, "is_defensive")() if hasattr(player, "is_defensive") else False)
        is_quarterback = position_value == "QB"
        is_running_back = position_value in self.RUNNING_BACK_POSITIONS
        is_receiver = position_value in self.RECEIVER_POSITIONS
        is_offensive_lineman = position_value in self.OFFENSIVE_LINE_POSITIONS
        is_kicker = position_value in self.KICKER_POSITIONS
        is_punter = position_value in self.PUNTER_POSITIONS
        is_defensive_back = is_defensive and position_value in self.DEFENSIVE_BACK_POSITIONS
        is_defensive_front = is_defensive and not is_defensive_back
        return PlayerTypeFlags(
            is_defensive=is_defensive,
            is_quarterback=is_quarterback,
            is_running_back=is_running_back,
            is_receiver=is_receiver,
            is_defensive_front=is_defensive_front,
            is_defensive_back=is_defensive_back,
            is_offensive_lineman=is_offensive_lineman,
            is_kicker=is_kicker,
            is_punter=is_punter,
        )

    def _determine_table_columns(self, flags: PlayerTypeFlags) -> list[str]:
        if flags.is_quarterback:
            return [
                "Season",
                "Age",
                "Team",
                "Tm Rec",
                "GP",
                "Snps",
                "WPA",
                "EPA",
                "QBR",
                "Tot TD",
                "Tot TO",
                "Tot Yds",
                "CMP",
                "ATT",
                "CMP%",
                "Pass Yds",
                "Pass TD",
                "TD%",
                "INT",
                "INT%",
                "Y/A",
                "Y/C",
                "Sacks",
                "Sack%",
                "Sack Yds",
            ]
        if flags.is_running_back:
            return list(self.RUNNING_BACK_COLUMNS)
        if flags.is_receiver:
            return list(self.RECEIVER_COLUMNS)
        if flags.is_offensive_lineman:
            return list(self.OFFENSIVE_LINE_COLUMNS)
        if flags.is_defensive_back:
            return list(self.DEFENSIVE_BACK_COLUMNS)
        if flags.is_defensive_front:
            return list(self.DEFENSIVE_FRONT_COLUMNS)
        if flags.is_kicker:
            return list(self.KICKER_COLUMNS)
        if flags.is_punter:
            return list(self.PUNTER_COLUMNS)
        return [
            "Season",
            "Team",
            "Pos",
            "GP",
            "Snps",
            "Pass Yds",
            "Rush Yds",
            "Rec Yds",
            "Tot TD",
        ]

    def _load_stats_for_player(self, player: Player, flags: PlayerTypeFlags) -> pl.DataFrame:
        """Load stats preferring cached aggregates before hitting nflreadpy."""

        stats = pl.DataFrame()
        player_id = player.profile.gsis_id
        if player_id:
            if flags.is_quarterback:
                stats = self._service.get_basic_offense_stats(player_id=player_id)
            if stats.is_empty():
                stats = self._service.get_basic_player_stats(player_id=player_id, position=player.profile.position)
        if stats.is_empty():
            stats = self._service.get_player_stats(
                player,
                seasons=True,
                season_type="REG",
                summary_level="season",
            )
        return stats if stats is not None else pl.DataFrame()


    def _build_table_rows(
        self,
        player: Player,
        stats: pl.DataFrame,
        *,
        flags: PlayerTypeFlags,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Transform raw stats into table-ready rows and summary metrics."""

        if stats is None or stats.height == 0:
            return [], {}

        data = stats

        if "season_type" in data.columns:
            data = data.filter(pl.col("season_type") == "REG")

        if data.height == 0 or "season" not in data.columns:
            return [], {}

        if flags.is_quarterback:
            if "pass_completions" in stats.columns:
                return self._build_table_rows_from_cached(player, stats, fetch_impacts=fetch_impacts)
            return self._build_qb_table_rows(player, data, stats, fetch_impacts=fetch_impacts)
        if flags.is_running_back:
            return self._build_running_back_table_rows(player, data, stats, fetch_impacts=fetch_impacts)
        if flags.is_receiver:
            return self._build_receiver_table_rows(player, data, stats, fetch_impacts=fetch_impacts)
        if flags.is_defensive:
            return self._build_defensive_table_rows(player, data, stats, flags=flags, fetch_impacts=fetch_impacts)
        if flags.is_offensive_lineman:
            return self._build_offensive_line_table_rows(player, data, stats, fetch_impacts=fetch_impacts)
        if flags.is_kicker:
            return self._build_kicker_table_rows(player, data, stats, fetch_impacts=fetch_impacts)
        if flags.is_punter:
            return self._build_punter_table_rows(player, data, stats, fetch_impacts=fetch_impacts)

        return self._build_standard_table_rows(data, stats)

    @staticmethod
    def _extract_seasons_from_frame(frame: pl.DataFrame) -> list[int]:
        """Return a sorted list of distinct season values from the supplied frame."""

        if "season" not in frame.columns:
            return []
        try:
            series = frame["season"].drop_nulls()
        except Exception:
            return []
        seasons: list[int] = []
        for value in series.unique().to_list():
            try:
                seasons.append(int(value))
            except (TypeError, ValueError):
                continue
        return sorted(set(seasons))

    def _get_quarterback_impact_map(
        self,
        player: Player,
        seasons: Iterable[int],
        *,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Fetch (or compute) QB EPA/WPA totals for the specified seasons."""

        if self._service is None:
            return {}
        season_list = [int(season) for season in seasons if season is not None]
        if not season_list:
            return {}
        try:
            return self._service.get_quarterback_epa_wpa(
                player,
                seasons=season_list,
                season_type=season_type,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load QB EPA/WPA data for %s: %s", player.profile.full_name, exc)
            return {}

    def _get_skill_player_impact_map(
        self,
        player: Player,
        data: pl.DataFrame,
        *,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Fetch cached EPA/WPA plus explosive-play info for RB/WR roles."""

        if player is None or self._service is None or data.height == 0:
            return {}

        seasons = self._extract_seasons_from_frame(data)
        if not seasons:
            return {}

        try:
            return self._service.get_skill_player_impacts(
                player,
                seasons=seasons,
                season_type=season_type,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load skill impact metrics for %s: %s", player.profile.full_name, exc)
            return {}

    def _get_defensive_impact_map(
        self,
        player: Player,
        data: pl.DataFrame,
        *,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        """Fetch (or compute) defensive EPA/WPA totals for the specified seasons."""

        if player is None or self._service is None or data.height == 0:
            return {}

        seasons = self._extract_seasons_from_frame(data)
        if not seasons:
            return {}

        try:
            return self._service.get_defensive_player_impacts(
                player,
                seasons=seasons,
                season_type=season_type,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load defensive impact data for %s: %s", player.profile.full_name, exc)
            return {}

    def _get_offensive_line_impact_map(
        self,
        player: Player,
        data: pl.DataFrame,
        *,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        if player is None or self._service is None or data.height == 0:
            return {}

        seasons = self._extract_seasons_from_frame(data)
        if not seasons:
            return {}

        try:
            return self._service.get_offensive_line_impacts(
                player,
                seasons=seasons,
                season_type=season_type,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load offensive line impact data for %s: %s", player.profile.full_name, exc)
            return {}

    def _get_kicker_impact_map(
        self,
        player: Player,
        data: pl.DataFrame,
        *,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        if player is None or self._service is None or data.height == 0:
            return {}

        seasons = self._extract_seasons_from_frame(data)
        if not seasons:
            return {}

        try:
            return self._service.get_kicker_impacts(
                player,
                seasons=seasons,
                season_type=season_type,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load kicker impact data for %s: %s", player.profile.full_name, exc)
            return {}

    def _get_punter_impact_map(
        self,
        player: Player,
        data: pl.DataFrame,
        *,
        season_type: str = "REG",
    ) -> dict[int, dict[str, float]]:
        if player is None or self._service is None or data.height == 0:
            return {}

        seasons = self._extract_seasons_from_frame(data)
        if not seasons:
            return {}

        try:
            return self._service.get_punter_impacts(
                player,
                seasons=seasons,
                season_type=season_type,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load punter impact data for %s: %s", player.profile.full_name, exc)
            return {}

    def _build_standard_table_rows(
        self,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct season rows for non-quarterbacks using legacy layout."""

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )
        position_expr = (
            self._coalesce_expr(
                data,
                ["player_position", "position", "position_group"],
                alias="_position",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_position")
        )
        games_expr = (
            self._coalesce_expr(
                data,
                ["games", "games_played", "games_active"],
                alias="_games_raw",
                default=0,
                dtype=pl.Float64,
            )
            .cast(pl.Float64, strict=False)
            .alias("_games_raw")
        )

        snap_cols = [col for col in ("offense_snaps", "defense_snaps", "special_teams_snaps", "snaps") if col in data.columns]
        if snap_cols:
            snap_expr = pl.sum_horizontal(
                [pl.col(col).cast(pl.Int64, strict=False).fill_null(0) for col in snap_cols]
            ).alias("_snaps")
        else:
            snap_expr = pl.lit(0).alias("_snaps")

        week_col = "week" if "week" in data.columns else None

        select_columns: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team"),
            pl.col("_position"),
            pl.col("_games_raw"),
            pl.col("_snaps"),
        ]
        if week_col is not None:
            select_columns.append(
                pl.col(week_col).cast(pl.Int64, strict=False).alias("_week")
            )
        else:
            select_columns.append(pl.lit(None).alias("_week"))

        select_columns.extend(
            [
                pl.col("_def_solo"),
                pl.col("_def_ast"),
                pl.col("_def_int"),
                pl.col("_pass_yds"),
                pl.col("_rush_yds"),
                pl.col("_rec_yds"),
                pl.col("_total_td"),
            ]
        )

        def _num(col: str) -> pl.Expr:
            return (
                pl.col(col).cast(pl.Float64, strict=False).fill_null(0)
                if col in data.columns
                else pl.lit(0.0)
            )

        def_solo = _num("def_tackles_solo").alias("_def_solo")
        def_ast_a = _num("def_tackle_assists")
        def_ast_b = _num("def_tackles_with_assist")
        def_ast = pl.sum_horizontal([def_ast_a, def_ast_b]).alias("_def_ast")
        def_int = _num("def_interceptions").alias("_def_int")

        pass_yds = _num("passing_yards").alias("_pass_yds")
        rush_yds = _num("rushing_yards").alias("_rush_yds")
        rec_yds = _num("receiving_yards").alias("_rec_yds")
        pass_tds = _num("passing_tds")
        rush_tds = _num("rushing_tds")
        rec_tds = _num("receiving_tds")
        total_td = pl.sum_horizontal([pass_tds, rush_tds, rec_tds]).alias("_total_td")

        prepared = (
            data.with_columns(
                [
                    team_expr,
                    position_expr,
                    games_expr,
                    snap_expr,
                    def_solo,
                    def_ast,
                    def_int,
                    pass_yds,
                    rush_yds,
                    rec_yds,
                    total_td,
                ]
            )
            .select(select_columns)
            .with_columns(
                pl.when(pl.col("_team").str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col("_team"))
                .alias("_team"),
                pl.when(pl.col("_position").str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col("_position"))
                .alias("_position"),
            )
        )

        aggregated = (
            prepared.group_by("season", "_team")
            .agg(
                pl.col("_position").drop_nulls().first().alias("_position"),
                pl.col("_games_raw").max().alias("_games_raw"),
                pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
                pl.col("_snaps").sum().alias("_snaps"),
                pl.col("_def_solo").sum().alias("_def_solo"),
                pl.col("_def_ast").sum().alias("_def_ast"),
                pl.col("_def_int").sum().alias("_def_int"),
                pl.col("_pass_yds").sum().alias("_pass_yds"),
                pl.col("_rush_yds").sum().alias("_rush_yds"),
                pl.col("_rec_yds").sum().alias("_rec_yds"),
                pl.col("_total_td").sum().alias("_total_td"),
            )
            .with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games"),
                pl.col("_position")
                .fill_null("")
                .alias("_position"),
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        rows: list[list[str]] = []
        for row in aggregated.iter_rows(named=True):
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)
            position_value = row.get("_position") or ""
            if not position_value:
                position_value = self._infer_position_for_row(row, original_stats)
            base = [
                self._format_value(row.get("season")),
                self._format_value(team_value),
                self._format_value(position_value),
                self._format_int(row.get("_games")),
                self._format_int(row.get("_snaps")),
            ]
            if self._is_defensive:
                tackles_total = (row.get("_def_solo") or 0) + (row.get("_def_ast") or 0)
                extra = [
                    self._format_int(tackles_total),
                    self._format_int(row.get("_def_solo")),
                    self._format_int(row.get("_def_ast")),
                    self._format_int(row.get("_def_int")),
                ]
            else:
                extra = [
                    self._format_int(row.get("_pass_yds")),
                    self._format_int(row.get("_rush_yds")),
                    self._format_int(row.get("_rec_yds")),
                    self._format_int(row.get("_total_td")),
                ]
            rows.append(base + extra)

        if self._is_defensive:
            summary = {
                "def_tackles_total": float((aggregated["_def_solo"].sum() or 0) + (aggregated["_def_ast"].sum() or 0)),
                "def_tackles_solo": float(aggregated["_def_solo"].sum() or 0),
                "def_tackles_assisted": float(aggregated["_def_ast"].sum() or 0),
                "def_interceptions": float(aggregated["_def_int"].sum() or 0),
                "games": float(aggregated["_games"].sum() or 0),
                "snaps": float(aggregated["_snaps"].sum() or 0),
            }
        else:
            summary = {
                "pass_yards": float(aggregated["_pass_yds"].sum() or 0),
                "rush_yards": float(aggregated["_rush_yds"].sum() or 0),
                "receiving_yards": float(aggregated["_rec_yds"].sum() or 0),
                "total_touchdowns": float(aggregated["_total_td"].sum() or 0),
                "games": float(aggregated["_games"].sum() or 0),
                "snaps": float(aggregated["_snaps"].sum() or 0),
            }
        return rows, summary

    def _aggregate_offensive_skill_stats(self, data: pl.DataFrame) -> pl.DataFrame:
        """Aggregate rushing/receiving stats for offensive skill players."""

        if data.height == 0:
            return pl.DataFrame()

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )
        games_expr = self._coalesce_expr(
            data,
            ["games", "games_played", "games_active"],
            alias="_games_raw",
            default=0,
            dtype=pl.Float64,
        )

        snap_cols = [col for col in ("offense_snaps", "defense_snaps", "special_teams_snaps", "snaps") if col in data.columns]
        if snap_cols:
            snap_expr = pl.sum_horizontal(
                [pl.col(col).cast(pl.Int64, strict=False).fill_null(0) for col in snap_cols]
            ).alias("_snaps")
        else:
            snap_expr = pl.lit(0).alias("_snaps")

        rush_att_expr = self._coalesce_expr(
            data,
            ["rushing_attempts", "rush_attempts", "carries"],
            alias="_rush_att",
            default=0,
            dtype=pl.Float64,
        )
        rush_yds_expr = self._coalesce_expr(
            data,
            ["rushing_yards", "rush_yards"],
            alias="_rush_yds",
            default=0,
            dtype=pl.Float64,
        )
        rush_td_expr = self._coalesce_expr(
            data,
            ["rushing_tds", "rush_tds"],
            alias="_rush_td",
            default=0,
            dtype=pl.Float64,
        )
        targets_expr = self._coalesce_expr(
            data,
            ["receiving_targets", "targets"],
            alias="_targets",
            default=0,
            dtype=pl.Float64,
        )
        receptions_expr = self._coalesce_expr(
            data,
            ["receiving_receptions", "receptions"],
            alias="_receptions",
            default=0,
            dtype=pl.Float64,
        )
        rec_yards_expr = self._coalesce_expr(
            data,
            ["receiving_yards", "rec_yards"],
            alias="_rec_yds",
            default=0,
            dtype=pl.Float64,
        )
        rec_td_expr = self._coalesce_expr(
            data,
            ["receiving_tds", "rec_tds"],
            alias="_rec_td",
            default=0,
            dtype=pl.Float64,
        )
        fumbles_expr = self._coalesce_expr(
            data,
            ["total_fumbles", "fumbles", "fumbles_lost"],
            alias="_fumbles",
            default=0,
            dtype=pl.Float64,
        )

        week_col = "week" if "week" in data.columns else None

        prepared = data.with_columns(
            [
                team_expr,
                games_expr,
                snap_expr,
                rush_att_expr,
                rush_yds_expr,
                rush_td_expr,
                targets_expr,
                receptions_expr,
                rec_yards_expr,
                rec_td_expr,
                fumbles_expr,
            ]
        )

        select_columns: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team"),
            pl.col("_games_raw"),
            pl.col("_snaps"),
            pl.col("_rush_att"),
            pl.col("_rush_yds"),
            pl.col("_rush_td"),
            pl.col("_targets"),
            pl.col("_receptions"),
            pl.col("_rec_yds"),
            pl.col("_rec_td"),
            pl.col("_fumbles"),
        ]
        if week_col is not None:
            select_columns.append(pl.col(week_col).cast(pl.Int64, strict=False).alias("_week"))
        else:
            select_columns.append(pl.lit(None).alias("_week"))

        aggregated = (
            prepared.select(select_columns)
            .group_by("season", "_team")
            .agg(
                pl.col("_games_raw").max().alias("_games_raw"),
                pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
                pl.col("_snaps").sum().alias("_snaps"),
                pl.col("_rush_att").sum().alias("_rush_att"),
                pl.col("_rush_yds").sum().alias("_rush_yds"),
                pl.col("_rush_td").sum().alias("_rush_td"),
                pl.col("_targets").sum().alias("_targets"),
                pl.col("_receptions").sum().alias("_receptions"),
                pl.col("_rec_yds").sum().alias("_rec_yds"),
                pl.col("_rec_td").sum().alias("_rec_td"),
                pl.col("_fumbles").sum().alias("_fumbles"),
            )
            .with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games"),
                pl.col("_team").fill_null("").alias("_team"),
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        return aggregated

    def _aggregate_defensive_stats(self, data: pl.DataFrame) -> pl.DataFrame:
        """Aggregate defensive counting stats for season rows."""

        if data.height == 0:
            return pl.DataFrame()

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )
        position_expr = (
            self._coalesce_expr(
                data,
                ["player_position", "position", "position_group"],
                alias="_position",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_position")
        )
        games_expr = self._coalesce_expr(
            data,
            ["games", "games_played", "games_active"],
            alias="_games_raw",
            default=0,
            dtype=pl.Float64,
        )

        snap_cols = [col for col in ("defense_snaps", "snaps", "offense_snaps") if col in data.columns]
        if snap_cols:
            snap_expr = pl.sum_horizontal(
                [pl.col(col).cast(pl.Int64, strict=False).fill_null(0) for col in snap_cols]
            ).alias("_snps")
        else:
            snap_expr = pl.lit(0).alias("_snps")

        def _stat_expr(columns: list[str], alias: str) -> pl.Expr:
            return self._coalesce_expr(
                data,
                columns,
                alias=alias,
                default=0,
                dtype=pl.Float64,
            )

        solo_expr = _stat_expr(["def_tackles_solo", "solo_tackles", "tackles_solo"], "_solo")
        assist_expr = _stat_expr(["def_tackle_assists", "assist_tackles"], "_assist")
        tfl_expr = _stat_expr(["def_tackles_for_loss", "tackles_for_loss"], "_tfl")
        sacks_expr = _stat_expr(["def_sacks", "sacks"], "_sacks")
        qb_hits_expr = _stat_expr(["def_qb_hits", "qb_hits"], "_qb_hits")
        ff_expr = _stat_expr(["def_forced_fumbles", "forced_fumbles"], "_ff")
        fr_expr = _stat_expr(["fumble_recovery_opp", "fumble_recoveries"], "_fr")
        safeties_expr = _stat_expr(["def_safeties", "safeties"], "_safeties")
        pass_def_expr = _stat_expr(["def_pass_defended", "passes_defended", "pass_defended"], "_pass_def")
        int_expr = _stat_expr(["def_interceptions", "interceptions"], "_ints")
        td_expr = _stat_expr(["def_tds", "defensive_touchdowns"], "_def_td")

        week_col = "week" if "week" in data.columns else None

        prepared = data.with_columns(
            [
                team_expr,
                position_expr,
                games_expr,
                snap_expr,
                solo_expr,
                assist_expr,
                tfl_expr,
                sacks_expr,
                qb_hits_expr,
                ff_expr,
                fr_expr,
                safeties_expr,
                pass_def_expr,
                int_expr,
                td_expr,
            ]
        )

        select_columns: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team"),
            pl.col("_position"),
            pl.col("_games_raw"),
            pl.col("_snps"),
            pl.col("_solo"),
            pl.col("_assist"),
            pl.col("_tfl"),
            pl.col("_sacks"),
            pl.col("_qb_hits"),
            pl.col("_ff"),
            pl.col("_fr"),
            pl.col("_safeties"),
            pl.col("_pass_def"),
            pl.col("_ints"),
            pl.col("_def_td"),
        ]

        if week_col is not None:
            select_columns.append(pl.col(week_col).cast(pl.Int64, strict=False).alias("_week"))
        else:
            select_columns.append(pl.lit(None).alias("_week"))

        aggregated = (
            prepared.select(select_columns)
            .group_by("season", "_team")
            .agg(
                pl.col("_position").drop_nulls().first().alias("_position"),
                pl.col("_games_raw").max().alias("_games_raw"),
                pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
                pl.col("_snps").sum().alias("_snps"),
                pl.col("_solo").sum().alias("_solo"),
                pl.col("_assist").sum().alias("_assist"),
                pl.col("_tfl").sum().alias("_tfl"),
                pl.col("_sacks").sum().alias("_sacks"),
                pl.col("_qb_hits").sum().alias("_qb_hits"),
                pl.col("_ff").sum().alias("_ff"),
                pl.col("_fr").sum().alias("_fr"),
                pl.col("_safeties").sum().alias("_safeties"),
                pl.col("_pass_def").sum().alias("_pass_def"),
                pl.col("_ints").sum().alias("_ints"),
                pl.col("_def_td").sum().alias("_def_td"),
            )
            .with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games"),
                pl.col("_team").fill_null("").alias("_team"),
                pl.col("_position").fill_null("").alias("_position"),
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        return aggregated

    def _aggregate_offensive_line_stats(self, data: pl.DataFrame) -> pl.DataFrame:
        """Aggregate key stats for offensive linemen."""

        if data.height == 0:
            return pl.DataFrame()

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )
        games_expr = self._coalesce_expr(
            data,
            ["games", "games_played", "games_active"],
            alias="_games_raw",
            default=0,
            dtype=pl.Float64,
        )

        snap_cols = [col for col in ("offense_snaps", "snaps") if col in data.columns]
        if snap_cols:
            snap_expr = pl.sum_horizontal(
                [pl.col(col).cast(pl.Int64, strict=False).fill_null(0) for col in snap_cols]
            ).alias("_snps")
        else:
            snap_expr = pl.lit(0).alias("_snps")

        def _stat_expr(columns: list[str], alias: str) -> pl.Expr:
            return self._coalesce_expr(
                data,
                columns,
                alias=alias,
                default=0,
                dtype=pl.Float64,
            )

        off_snps_expr = _stat_expr(
            ["offense_snaps", "offense_snaps_played", "offensive_snaps"],
            "_off_snps",
        )
        off_snps_avail_expr = _stat_expr(
            ["offense_snaps_available", "team_offense_snaps", "offense_total_snaps"],
            "_off_snps_avail",
        )
        holding_expr = _stat_expr(
            ["penalties_holding", "holding_penalties"],
            "_holding",
        )
        false_start_expr = _stat_expr(
            ["penalties_false_start", "false_start_penalties"],
            "_false_start",
        )
        pen_decl_expr = _stat_expr(
            ["penalties_declined"],
            "_pen_decl",
        )
        pen_offset_expr = _stat_expr(
            ["penalties_offsetting", "penalties_offset"],
            "_pen_offset",
        )
        penalties_expr = _stat_expr(
            ["penalties", "total_penalties"],
            "_pen_total",
        )

        week_col = "week" if "week" in data.columns else None

        prepared = data.with_columns(
            [
                team_expr,
                games_expr,
                snap_expr,
                off_snps_expr,
                off_snps_avail_expr,
                holding_expr,
                false_start_expr,
                pen_decl_expr,
                pen_offset_expr,
                penalties_expr,
            ]
        )

        select_columns: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team"),
            pl.col("_games_raw"),
            pl.col("_snps"),
            pl.col("_off_snps"),
            pl.col("_off_snps_avail"),
            pl.col("_holding"),
            pl.col("_false_start"),
            pl.col("_pen_decl"),
            pl.col("_pen_offset"),
            pl.col("_pen_total"),
        ]
        if week_col is not None:
            select_columns.append(pl.col(week_col).cast(pl.Int64, strict=False).alias("_week"))
        else:
            select_columns.append(pl.lit(None).alias("_week"))

        aggregated = (
            prepared.select(select_columns)
            .group_by("season", "_team")
            .agg(
                pl.col("_games_raw").max().alias("_games_raw"),
                pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
                pl.col("_snps").sum().alias("_snps"),
                pl.col("_off_snps").sum().alias("_off_snps"),
                pl.col("_off_snps_avail").sum().alias("_off_snps_avail"),
                pl.col("_holding").sum().alias("_holding"),
                pl.col("_false_start").sum().alias("_false_start"),
                pl.col("_pen_decl").sum().alias("_pen_decl"),
                pl.col("_pen_offset").sum().alias("_pen_offset"),
                pl.col("_pen_total").sum().alias("_pen_total"),
            )
            .with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games"),
                pl.col("_team").fill_null("").alias("_team"),
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        return aggregated

    def _aggregate_kicker_stats(self, data: pl.DataFrame) -> pl.DataFrame:
        """Aggregate field-goal/extra-point stats for kickers."""

        if data.height == 0:
            return pl.DataFrame()

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )
        games_expr = self._coalesce_expr(
            data,
            ["games", "games_played", "games_active"],
            alias="_games_raw",
            default=0,
            dtype=pl.Float64,
        )

        snap_cols = [col for col in ("special_teams_snaps", "snaps") if col in data.columns]
        if snap_cols:
            snap_expr = pl.sum_horizontal(
                [pl.col(col).cast(pl.Int64, strict=False).fill_null(0) for col in snap_cols]
            ).alias("_snps")
        else:
            snap_expr = pl.lit(0).alias("_snps")

        def _stat_expr(columns: list[str], alias: str) -> pl.Expr:
            return self._coalesce_expr(
                data,
                columns,
                alias=alias,
                default=0,
                dtype=pl.Float64,
            )

        def _sum_expr(columns: list[str], alias: str) -> pl.Expr:
            exprs = [
                pl.col(col).cast(pl.Float64, strict=False).fill_null(0)
                for col in columns
                if col in data.columns
            ]
            if not exprs:
                return pl.lit(0.0).alias(alias)
            if len(exprs) == 1:
                return exprs[0].alias(alias)
            return pl.sum_horizontal(exprs).alias(alias)

        fgm_expr = _stat_expr(["fgm", "field_goals_made"], "_fgm")
        fga_expr = _stat_expr(["fga", "field_goals_attempted"], "_fga")
        xpm_expr = _stat_expr(["xpm", "extra_points_made"], "_xpm")
        xpa_expr = _stat_expr(["xpa", "extra_points_attempted"], "_xpa")
        fg_long_expr = _stat_expr(["fg_long", "field_goal_long"], "_fg_long")

        fgm_029_expr = _sum_expr(
            ["fgm_0_19", "fgm_20_29", "field_goals_made_0_19", "field_goals_made_20_29"],
            "_fgm_029",
        )
        fga_029_expr = _sum_expr(
            ["fga_0_19", "fga_20_29", "field_goals_attempted_0_19", "field_goals_attempted_20_29"],
            "_fga_029",
        )
        fgm_3039_expr = _sum_expr(
            ["fgm_30_39", "field_goals_made_30_39"],
            "_fgm_3039",
        )
        fga_3039_expr = _sum_expr(
            ["fga_30_39", "field_goals_attempted_30_39"],
            "_fga_3039",
        )
        fgm_4049_expr = _sum_expr(
            ["fgm_40_49", "field_goals_made_40_49"],
            "_fgm_4049",
        )
        fga_4049_expr = _sum_expr(
            ["fga_40_49", "field_goals_attempted_40_49"],
            "_fga_4049",
        )
        fgm_5059_expr = _sum_expr(
            ["fgm_50_59", "field_goals_made_50_59"],
            "_fgm_5059",
        )
        fga_5059_expr = _sum_expr(
            ["fga_50_59", "field_goals_attempted_50_59"],
            "_fga_5059",
        )
        fgm_60_expr = _stat_expr(["fgm_60_plus", "field_goals_made_60_plus"], "_fgm_60")
        fga_60_expr = _stat_expr(["fga_60_plus", "field_goals_attempted_60_plus"], "_fga_60")

        kickoffs_expr = _stat_expr(["kickoffs", "kickoff_attempts"], "_kickoffs")
        touchbacks_expr = _stat_expr(["kickoff_touchbacks", "touchbacks"], "_kick_touchbacks")

        week_col = "week" if "week" in data.columns else None

        prepared = data.with_columns(
            [
                team_expr,
                games_expr,
                snap_expr,
                fgm_expr,
                fga_expr,
                xpm_expr,
                xpa_expr,
                fg_long_expr,
                fgm_029_expr,
                fga_029_expr,
                fgm_3039_expr,
                fga_3039_expr,
                fgm_4049_expr,
                fga_4049_expr,
                fgm_5059_expr,
                fga_5059_expr,
                fgm_60_expr,
                fga_60_expr,
                kickoffs_expr,
                touchbacks_expr,
            ]
        )

        select_columns: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team"),
            pl.col("_games_raw"),
            pl.col("_snps"),
            pl.col("_fgm"),
            pl.col("_fga"),
            pl.col("_xpm"),
            pl.col("_xpa"),
            pl.col("_fg_long"),
            pl.col("_fgm_029"),
            pl.col("_fga_029"),
            pl.col("_fgm_3039"),
            pl.col("_fga_3039"),
            pl.col("_fgm_4049"),
            pl.col("_fga_4049"),
            pl.col("_fgm_5059"),
            pl.col("_fga_5059"),
            pl.col("_fgm_60"),
            pl.col("_fga_60"),
            pl.col("_kickoffs"),
            pl.col("_kick_touchbacks"),
        ]
        if week_col is not None:
            select_columns.append(pl.col(week_col).cast(pl.Int64, strict=False).alias("_week"))
        else:
            select_columns.append(pl.lit(None).alias("_week"))

        aggregated = (
            prepared.select(select_columns)
            .group_by("season", "_team")
            .agg(
                pl.col("_games_raw").max().alias("_games_raw"),
                pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
                pl.col("_snps").sum().alias("_snps"),
                pl.col("_fgm").sum().alias("_fgm"),
                pl.col("_fga").sum().alias("_fga"),
                pl.col("_xpm").sum().alias("_xpm"),
                pl.col("_xpa").sum().alias("_xpa"),
                pl.col("_fg_long").max().alias("_fg_long"),
                pl.col("_fgm_029").sum().alias("_fgm_029"),
                pl.col("_fga_029").sum().alias("_fga_029"),
                pl.col("_fgm_3039").sum().alias("_fgm_3039"),
                pl.col("_fga_3039").sum().alias("_fga_3039"),
                pl.col("_fgm_4049").sum().alias("_fgm_4049"),
                pl.col("_fga_4049").sum().alias("_fga_4049"),
                pl.col("_fgm_5059").sum().alias("_fgm_5059"),
                pl.col("_fga_5059").sum().alias("_fga_5059"),
                pl.col("_fgm_60").sum().alias("_fgm_60"),
                pl.col("_fga_60").sum().alias("_fga_60"),
                pl.col("_kickoffs").sum().alias("_kickoffs"),
                pl.col("_kick_touchbacks").sum().alias("_kick_touchbacks"),
            )
            .with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games"),
                pl.col("_team").fill_null("").alias("_team"),
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        return aggregated

    def _aggregate_punter_stats(self, data: pl.DataFrame) -> pl.DataFrame:
        """Aggregate punting stats for season rows."""

        if data.height == 0:
            return pl.DataFrame()

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )
        games_expr = self._coalesce_expr(
            data,
            ["games", "games_played", "games_active"],
            alias="_games_raw",
            default=0,
            dtype=pl.Float64,
        )

        snap_expr = self._coalesce_expr(
            data,
            ["special_teams_snaps", "snaps"],
            alias="_snps",
            default=0,
            dtype=pl.Float64,
        )

        def _stat_expr(columns: list[str], alias: str) -> pl.Expr:
            return self._coalesce_expr(
                data,
                columns,
                alias=alias,
                default=0,
                dtype=pl.Float64,
            )

        punts_expr = _stat_expr(["punts"], "_punts")
        punt_yards_expr = _stat_expr(["punt_yards", "punting_yards"], "_punt_yds")
        long_punt_expr = _stat_expr(["punt_long", "long_punt"], "_punt_long")
        return_yds_expr = _stat_expr(["punt_return_yards_allowed", "opponent_punt_return_yards"], "_opp_ret_yds")
        net_yds_expr = _stat_expr(["net_punt_yards", "punt_net_yards"], "_net_yds")
        touchbacks_expr = _stat_expr(["punt_touchbacks", "touchbacks"], "_touchbacks")
        inside20_expr = _stat_expr(["punts_inside_20"], "_inside_20")
        punts_blocked_expr = _stat_expr(["punts_blocked"], "_punts_blocked")

        week_col = "week" if "week" in data.columns else None

        prepared = data.with_columns(
            [
                team_expr,
                games_expr,
                snap_expr,
                punts_expr,
                punt_yards_expr,
                long_punt_expr,
                return_yds_expr,
                net_yds_expr,
                touchbacks_expr,
                inside20_expr,
                punts_blocked_expr,
            ]
        )

        select_columns: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team"),
            pl.col("_games_raw"),
            pl.col("_snps"),
            pl.col("_punts"),
            pl.col("_punt_yds"),
            pl.col("_punt_long"),
            pl.col("_opp_ret_yds"),
            pl.col("_net_yds"),
            pl.col("_touchbacks"),
            pl.col("_inside_20"),
            pl.col("_punts_blocked"),
        ]
        if week_col is not None:
            select_columns.append(pl.col(week_col).cast(pl.Int64, strict=False).alias("_week"))
        else:
            select_columns.append(pl.lit(None).alias("_week"))

        aggregated = (
            prepared.select(select_columns)
            .group_by("season", "_team")
            .agg(
                pl.col("_games_raw").max().alias("_games_raw"),
                pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
                pl.col("_snps").sum().alias("_snps"),
                pl.col("_punts").sum().alias("_punts"),
                pl.col("_punt_yds").sum().alias("_punt_yds"),
                pl.col("_punt_long").max().alias("_punt_long"),
                pl.col("_opp_ret_yds").sum().alias("_opp_ret_yds"),
                pl.col("_net_yds").sum().alias("_net_yds"),
                pl.col("_touchbacks").sum().alias("_touchbacks"),
                pl.col("_inside_20").sum().alias("_inside_20"),
                pl.col("_punts_blocked").sum().alias("_punts_blocked"),
            )
            .with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games"),
                pl.col("_team").fill_null("").alias("_team"),
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        return aggregated

    def _build_running_back_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct running back/fullback season rows."""

        aggregated = self._aggregate_offensive_skill_stats(data)
        if aggregated.height == 0:
            return [], {}

        impact_map = self._get_skill_player_impact_map(player, data) if fetch_impacts else {}

        rows: list[list[str]] = []
        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            season_int = int(season) if season is not None else None
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)

            games_played = row.get("_games")
            snaps_played = row.get("_snaps")
            rush_att = float(row.get("_rush_att") or 0.0)
            rush_yds = float(row.get("_rush_yds") or 0.0)
            rush_td = float(row.get("_rush_td") or 0.0)
            targets = float(row.get("_targets") or 0.0)
            receptions = float(row.get("_receptions") or 0.0)
            rec_yds = float(row.get("_rec_yds") or 0.0)
            rec_td = float(row.get("_rec_td") or 0.0)
            fumbles = float(row.get("_fumbles") or 0.0)

            total_yards = rush_yds + rec_yds
            total_td = rush_td + rec_td
            touches = rush_att + receptions
            rush_y_per_attempt = rush_yds / rush_att if rush_att > 0 else None
            rec_y_per_rec = rec_yds / receptions if receptions > 0 else None
            yards_per_target = rec_yds / targets if targets > 0 else None
            yards_per_touch = total_yards / touches if touches > 0 else None

            impact = impact_map.get(season_int, {}) if season_int is not None else {}
            wpa_value = impact.get("wpa")
            epa_value = impact.get("epa")
            rush_20_plus = int(round(impact.get("rush_20_plus", 0.0))) if impact else 0
            rec_20_plus = int(round(impact.get("rec_20_plus", 0.0))) if impact else 0

            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            team_record = ""
            if team_value and season_int is not None and self._service is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:  # pragma: no cover - defensive
                    team_record = ""

            row_values = [
                self._format_value(season),
                self._format_optional_int(age_value),
                self._format_value(team_value),
                team_record or "—",
                self._format_int(games_played),
                self._format_int(snaps_played),
                self._format_float(wpa_value, 3),
                self._format_float(epa_value, 1),
                self._format_int(total_td),
                self._format_int(total_yards),
                self._format_int(rush_att),
                self._format_int(rush_yds),
                self._format_int(rush_td),
                self._format_float(rush_y_per_attempt, 2),
                self._format_int(rush_20_plus),
                self._format_int(targets),
                self._format_int(receptions),
                self._format_int(rec_yds),
                self._format_float(rec_y_per_rec, 2),
                self._format_int(rec_td),
                self._format_int(rec_20_plus),
                self._format_float(yards_per_target, 2),
                self._format_int(touches),
                self._format_float(yards_per_touch, 2),
                self._format_int(fumbles),
            ]
            rows.append(row_values)

        summary = {
            "pass_yards": 0.0,
            "rush_yards": float(aggregated["_rush_yds"].sum() or 0),
            "receiving_yards": float(aggregated["_rec_yds"].sum() or 0),
            "total_touchdowns": float((aggregated["_rush_td"].sum() or 0) + (aggregated["_rec_td"].sum() or 0)),
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(aggregated["_snaps"].sum() or 0),
        }
        return rows, summary

    def _build_receiver_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct wide receiver/tight end season rows."""

        aggregated = self._aggregate_offensive_skill_stats(data)
        if aggregated.height == 0:
            return [], {}

        impact_map = self._get_skill_player_impact_map(player, data) if fetch_impacts else {}
        rows: list[list[str]] = []

        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            season_int = int(season) if season is not None else None
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)

            games_played = row.get("_games")
            snaps_played = row.get("_snaps")
            rush_yds = float(row.get("_rush_yds") or 0.0)
            rush_td = float(row.get("_rush_td") or 0.0)
            targets = float(row.get("_targets") or 0.0)
            receptions = float(row.get("_receptions") or 0.0)
            rec_yds = float(row.get("_rec_yds") or 0.0)
            rec_td = float(row.get("_rec_td") or 0.0)
            fumbles = float(row.get("_fumbles") or 0.0)

            total_yards = rush_yds + rec_yds
            total_td = rush_td + rec_td
            rec_y_per_rec = rec_yds / receptions if receptions > 0 else None
            yards_per_target = rec_yds / targets if targets > 0 else None
            catch_pct = (receptions / targets * 100.0) if targets > 0 else None

            impact = impact_map.get(season_int, {}) if season_int is not None else {}
            wpa_value = impact.get("wpa")
            epa_value = impact.get("epa")
            rec_20_plus = int(round(impact.get("rec_20_plus", 0.0))) if impact else 0
            first_downs = int(round(impact.get("rec_first_downs", 0.0))) if impact else 0

            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            team_record = ""
            if team_value and season_int is not None and self._service is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:  # pragma: no cover - defensive
                    team_record = ""

            row_values = [
                self._format_value(season),
                self._format_optional_int(age_value),
                self._format_value(team_value),
                team_record or "—",
                self._format_int(games_played),
                self._format_int(snaps_played),
                self._format_float(wpa_value, 3),
                self._format_float(epa_value, 1),
                self._format_int(total_td),
                self._format_int(total_yards),
                self._format_int(targets),
                self._format_int(receptions),
                self._format_int(rec_yds),
                self._format_float(rec_y_per_rec, 2),
                self._format_int(rec_td),
                self._format_int(rec_20_plus),
                self._format_float(catch_pct, 1),
                self._format_float(yards_per_target, 2),
                self._format_int(first_downs),
                self._format_int(fumbles),
            ]
            rows.append(row_values)

        summary = {
            "pass_yards": 0.0,
            "rush_yards": float(aggregated["_rush_yds"].sum() or 0),
            "receiving_yards": float(aggregated["_rec_yds"].sum() or 0),
            "total_touchdowns": float((aggregated["_rush_td"].sum() or 0) + (aggregated["_rec_td"].sum() or 0)),
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(aggregated["_snaps"].sum() or 0),
        }
        return rows, summary

    def _build_offensive_line_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        aggregated = self._aggregate_offensive_line_stats(data)
        if aggregated.height == 0:
            return [], {}

        impact_map = self._get_offensive_line_impact_map(player, data) if fetch_impacts else {}
        rows: list[list[str]] = []

        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            season_int = int(season) if season is not None else None
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)

            games_played = row.get("_games")
            snaps_played = row.get("_snps")
            off_snps = float(row.get("_off_snps") or 0.0)
            off_snps_avail = float(row.get("_off_snps_avail") or 0.0)
            off_snps_pct = (off_snps / off_snps_avail * 100.0) if off_snps_avail > 0 else None
            holding = float(row.get("_holding") or 0.0)
            false_start = float(row.get("_false_start") or 0.0)
            decl_offs = float(row.get("_pen_decl") or 0.0) + float(row.get("_pen_offset") or 0.0)
            penalties = float(row.get("_pen_total") or 0.0)

            impact = impact_map.get(season_int, {}) if season_int is not None else {}
            wpa_value = impact.get("wpa")
            epa_value = impact.get("epa")

            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            team_record = ""
            if team_value and season_int is not None and self._service is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:
                    team_record = ""

            rows.append(
                [
                    self._format_value(season),
                    self._format_optional_int(age_value),
                    self._format_value(team_value),
                    team_record or "—",
                    self._format_int(games_played),
                    self._format_int(snaps_played),
                    self._format_float(wpa_value, 3),
                    self._format_float(epa_value, 1),
                    self._format_int(off_snps),
                    self._format_float(off_snps_pct, 1),
                    self._format_int(holding),
                    self._format_int(false_start),
                    self._format_int(decl_offs),
                    self._format_int(penalties),
                ]
            )

        summary = {
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(aggregated["_snps"].sum() or 0),
        }
        return rows, summary

    def _build_kicker_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        aggregated = self._aggregate_kicker_stats(data)
        if aggregated.height == 0:
            return [], {}

        impact_map = self._get_kicker_impact_map(player, data) if fetch_impacts else {}
        rows: list[list[str]] = []

        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            season_int = int(season) if season is not None else None
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)

            games_played = row.get("_games")
            snaps_played = row.get("_snps")
            fgm = float(row.get("_fgm") or 0.0)
            fga = float(row.get("_fga") or 0.0)
            xpm = float(row.get("_xpm") or 0.0)
            xpa = float(row.get("_xpa") or 0.0)
            fg_long = float(row.get("_fg_long") or 0.0)
            fgm_029 = float(row.get("_fgm_029") or 0.0)
            fga_029 = float(row.get("_fga_029") or 0.0)
            fgm_3039 = float(row.get("_fgm_3039") or 0.0)
            fga_3039 = float(row.get("_fga_3039") or 0.0)
            fgm_4049 = float(row.get("_fgm_4049") or 0.0)
            fga_4049 = float(row.get("_fga_4049") or 0.0)
            fgm_5059 = float(row.get("_fgm_5059") or 0.0)
            fga_5059 = float(row.get("_fga_5059") or 0.0)
            fgm_60 = float(row.get("_fgm_60") or 0.0)
            fga_60 = float(row.get("_fga_60") or 0.0)
            kickoffs = float(row.get("_kickoffs") or 0.0)
            kick_touchbacks = float(row.get("_kick_touchbacks") or 0.0)

            fg_pct = (fgm / fga * 100.0) if fga > 0 else None
            xp_pct = (xpm / xpa * 100.0) if xpa > 0 else None
            tb_pct = (kick_touchbacks / kickoffs * 100.0) if kickoffs > 0 else None

            impact = impact_map.get(season_int, {}) if season_int is not None else {}
            wpa_value = impact.get("wpa")
            epa_value = impact.get("epa")

            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            team_record = ""
            if team_value and season_int is not None and self._service is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:
                    team_record = ""

            rows.append(
                [
                    self._format_value(season),
                    self._format_optional_int(age_value),
                    self._format_value(team_value),
                    team_record or "—",
                    self._format_int(games_played),
                    self._format_int(snaps_played),
                    self._format_float(wpa_value, 3),
                    self._format_float(epa_value, 1),
                    self._format_int(fgm),
                    self._format_int(fga),
                    self._format_float(fg_pct, 1),
                    self._format_int(xpm),
                    self._format_int(xpa),
                    self._format_float(xp_pct, 1),
                    self._format_int(fg_long),
                    self._format_int(fgm_029),
                    self._format_int(fga_029),
                    self._format_int(fgm_3039),
                    self._format_int(fga_3039),
                    self._format_int(fgm_4049),
                    self._format_int(fga_4049),
                    self._format_int(fgm_5059),
                    self._format_int(fga_5059),
                    self._format_int(fgm_60),
                    self._format_int(fga_60),
                    self._format_int(kickoffs),
                    self._format_float(tb_pct, 1),
                ]
            )

        summary = {
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(aggregated["_snps"].sum() or 0),
        }
        return rows, summary

    def _build_punter_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        aggregated = self._aggregate_punter_stats(data)
        if aggregated.height == 0:
            return [], {}

        impact_map = self._get_punter_impact_map(player, data) if fetch_impacts else {}
        rows: list[list[str]] = []

        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            season_int = int(season) if season is not None else None
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)

            games_played = row.get("_games")
            snaps_played = row.get("_snps")
            punts = float(row.get("_punts") or 0.0)
            punt_yards = float(row.get("_punt_yds") or 0.0)
            punt_long = float(row.get("_punt_long") or 0.0)
            opp_ret_yds = float(row.get("_opp_ret_yds") or 0.0)
            net_yds = float(row.get("_net_yds") or 0.0)
            touchbacks = float(row.get("_touchbacks") or 0.0)
            inside_20 = float(row.get("_inside_20") or 0.0)
            punts_blocked = float(row.get("_punts_blocked") or 0.0)

            yds_per_punt = (punt_yards / punts) if punts > 0 else None
            net_yds_per_punt = (net_yds / punts) if punts > 0 else None
            touchback_pct = (touchbacks / punts * 100.0) if punts > 0 else None
            inside20_pct = (inside_20 / punts * 100.0) if punts > 0 else None

            impact = impact_map.get(season_int, {}) if season_int is not None else {}
            wpa_value = impact.get("wpa")
            epa_value = impact.get("epa")

            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            team_record = ""
            if team_value and season_int is not None and self._service is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:
                    team_record = ""

            rows.append(
                [
                    self._format_value(season),
                    self._format_optional_int(age_value),
                    self._format_value(team_value),
                    team_record or "—",
                    self._format_int(games_played),
                    self._format_int(snaps_played),
                    self._format_float(wpa_value, 3),
                    self._format_float(epa_value, 1),
                    self._format_int(punts),
                    self._format_int(punt_yards),
                    self._format_float(yds_per_punt, 1),
                    self._format_int(punt_long),
                    self._format_int(opp_ret_yds),
                    self._format_int(net_yds),
                    self._format_float(net_yds_per_punt, 1),
                    self._format_int(touchbacks),
                    self._format_float(touchback_pct, 1),
                    self._format_int(inside_20),
                    self._format_float(inside20_pct, 1),
                    self._format_int(punts_blocked),
                ]
            )

        summary = {
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(aggregated["_snps"].sum() or 0),
        }
        return rows, summary

    def _build_defensive_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        flags: PlayerTypeFlags,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct defensive season rows for front-seven or secondary players."""

        aggregated = self._aggregate_defensive_stats(data)
        if aggregated.height == 0:
            return [], {}

        impact_map = self._get_defensive_impact_map(player, data) if fetch_impacts else {}
        rows: list[list[str]] = []

        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            season_int = int(season) if season is not None else None
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)

            games_played = row.get("_games")
            snaps_played = row.get("_snps")
            solo = float(row.get("_solo") or 0.0)
            assist = float(row.get("_assist") or 0.0)
            total_tackles = solo + assist
            tfl = float(row.get("_tfl") or 0.0)
            sacks = float(row.get("_sacks") or 0.0)
            qb_hits = float(row.get("_qb_hits") or 0.0)
            forced_fumbles = float(row.get("_ff") or 0.0)
            fumble_recoveries = float(row.get("_fr") or 0.0)
            safeties = float(row.get("_safeties") or 0.0)
            pass_defended = float(row.get("_pass_def") or 0.0)
            interceptions = float(row.get("_ints") or 0.0)
            touchdowns = float(row.get("_def_td") or 0.0)

            impact = impact_map.get(season_int, {}) if season_int is not None else {}
            wpa_value = impact.get("wpa")
            epa_value = impact.get("epa")

            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            team_record = ""
            if team_value and season_int is not None and self._service is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:  # pragma: no cover - defensive
                    team_record = ""

            common_values = [
                self._format_value(season),
                self._format_optional_int(age_value),
                self._format_value(team_value),
                team_record or "—",
                self._format_int(games_played),
                self._format_int(snaps_played),
                self._format_float(wpa_value, 3),
                self._format_float(epa_value, 1),
            ]

            if flags.is_defensive_back:
                row_values = common_values + [
                    self._format_int(interceptions),
                    self._format_int(touchdowns),
                    self._format_int(pass_defended),
                    self._format_int(total_tackles),
                    self._format_int(solo),
                    self._format_int(assist),
                    self._format_int(tfl),
                    self._format_int(sacks),
                    self._format_int(qb_hits),
                    self._format_int(forced_fumbles),
                    self._format_int(fumble_recoveries),
                    self._format_int(safeties),
                ]
            else:
                row_values = common_values + [
                    self._format_int(total_tackles),
                    self._format_int(solo),
                    self._format_int(assist),
                    self._format_int(tfl),
                    self._format_int(sacks),
                    self._format_int(qb_hits),
                    self._format_int(forced_fumbles),
                    self._format_int(fumble_recoveries),
                    self._format_int(safeties),
                    self._format_int(pass_defended),
                    self._format_int(interceptions),
                    self._format_int(touchdowns),
                ]
            rows.append(row_values)

        summary = {
            "def_tackles_total": float((aggregated["_solo"].sum() or 0) + (aggregated["_assist"].sum() or 0)),
            "def_tackles_solo": float(aggregated["_solo"].sum() or 0),
            "def_tackles_assisted": float(aggregated["_assist"].sum() or 0),
            "def_interceptions": float(aggregated["_ints"].sum() or 0),
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(aggregated["_snps"].sum() or 0),
        }
        return rows, summary

    def _build_table_rows_from_cached(
        self,
        player: Player,
        cached_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct quarterback rows from the cached basic offense stats."""

        if cached_stats.is_empty():
            return [], {}

        data = cached_stats.sort("season", descending=True)
        rows: list[list[str]] = []
        snaps_total = 0.0

        wpa_columns = ["total_wpa", "wpa_total", "wpa", "qb_wpa"]
        epa_columns = ["total_epa", "epa_total", "epa", "qb_epa"]
        needs_wpa = not any(column in data.columns for column in wpa_columns)
        needs_epa = not any(column in data.columns for column in epa_columns)
        qb_impact_map: dict[int, dict[str, float]] = {}
        if fetch_impacts and player and (needs_wpa or needs_epa):
            season_candidates = self._extract_seasons_from_frame(data)
            qb_impact_map = self._get_quarterback_impact_map(player, season_candidates)

        for record in data.iter_rows(named=True):
            season = record.get("season")
            team_value = str(record.get("team") or "").upper()
            games_played = int(record.get("games_played") or 0)
            season_int = int(season) if season is not None else None

            completions = float(record.get("pass_completions") or 0.0)
            attempts = float(record.get("pass_attempts") or 0.0)
            pass_yards_total = float(record.get("passing_yards") or 0.0)
            pass_touchdowns = float(record.get("passing_tds") or 0.0)
            interceptions = float(record.get("passing_ints") or 0.0)
            sacks_taken = float(record.get("sacks_taken") or 0.0)
            sack_yards_lost = float(record.get("sack_yards") or 0.0)

            rush_attempts = float(record.get("rushing_attempts") or 0.0)
            rush_yards_total = float(record.get("rushing_yards") or 0.0)
            rush_touchdowns = float(record.get("rushing_tds") or 0.0)

            fumbles_lost = float(record.get("fumbles_lost") or 0.0)

            dropbacks = attempts + sacks_taken
            snaps_played = dropbacks + rush_attempts
            snaps_total += snaps_played

            wpa_value = self._extract_numeric_value(
                record,
                ["total_wpa", "wpa_total", "wpa"],
            )
            epa_value = self._extract_numeric_value(
                record,
                ["total_epa", "epa_total", "epa"],
            )
            impact = qb_impact_map.get(season_int) if season_int is not None else None
            if wpa_value is None and impact:
                wpa_value = impact.get("wpa")
            if epa_value is None and impact:
                epa_value = impact.get("epa")

            completion_pct = (completions / attempts * 100.0) if attempts > 0 else 0.0
            touchdown_pct = (pass_touchdowns / attempts * 100.0) if attempts > 0 else 0.0
            interception_pct = (interceptions / attempts * 100.0) if attempts > 0 else 0.0
            yards_per_attempt = (pass_yards_total / attempts) if attempts > 0 else 0.0
            yards_per_completion = (pass_yards_total / completions) if completions > 0 else 0.0
            sack_pct = (sacks_taken / dropbacks * 100.0) if dropbacks > 0 else 0.0
            negative_yards_per_attempt = (-sack_yards_lost / dropbacks) if dropbacks > 0 else 0.0
            any_yards_per_attempt = (
                (pass_yards_total + 20 * pass_touchdowns - 45 * interceptions - sack_yards_lost) / dropbacks
                if dropbacks > 0
                else 0.0
            )
            rush_yards_per_attempt = (rush_yards_total / rush_attempts) if rush_attempts > 0 else 0.0

            total_touchdowns = pass_touchdowns + rush_touchdowns
            total_turnovers = interceptions + fumbles_lost
            total_yards = pass_yards_total + rush_yards_total

            age_value = self._compute_age_for_season(player, season_int)
            team_record = ""
            if team_value and season_int is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                    team_record = self._format_team_record(*record_tuple)
                except Exception:  # pragma: no cover - defensive
                    team_record = ""

            qb_rating = self._calculate_passer_rating(
                completions,
                attempts,
                pass_yards_total,
                pass_touchdowns,
                interceptions,
            )

            row_values = [
                self._format_value(season),
                self._format_optional_int(age_value),
                self._format_value(team_value),
                team_record or "—",
                self._format_int(games_played),
                self._format_int(snaps_played),
                self._format_float(wpa_value, 3),
                self._format_float(epa_value, 1),
                self._format_float(qb_rating, 1),
                self._format_int(total_touchdowns),
                self._format_int(total_turnovers),
                self._format_int(total_yards),
                self._format_int(completions),
                self._format_int(attempts),
                self._format_float(completion_pct, 1),
                self._format_int(pass_yards_total),
                self._format_int(pass_touchdowns),
                self._format_float(touchdown_pct, 2),
                self._format_int(interceptions),
                self._format_float(interception_pct, 2),
                self._format_float(yards_per_attempt, 2),
                self._format_float(yards_per_completion, 2),
                self._format_int(sacks_taken),
                self._format_float(sack_pct, 2),
                self._format_int(-abs(sack_yards_lost)),
            ]
            rows.append(row_values)

        def _sum(column: str) -> float:
            try:
                return float(data[column].sum())
            except Exception:
                return 0.0

        summary = {
            "pass_yards": _sum("passing_yards"),
            "rush_yards": _sum("rushing_yards"),
            "receiving_yards": _sum("receiving_yards"),
            "total_touchdowns": _sum("passing_tds") + _sum("rushing_tds"),
            "games": _sum("games_played"),
            "snaps": float(snaps_total),
        }
        return rows, summary

    def _build_qb_table_rows(
        self,
        player: Player,
        data: pl.DataFrame,
        original_stats: pl.DataFrame,
        *,
        fetch_impacts: bool = True,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct quarterback-specific season rows with advanced metrics."""

        team_expr = (
            self._coalesce_expr(
                data,
                ["team", "team_abbr", "recent_team", "current_team_abbr"],
                alias="_team",
                default="",
                dtype=pl.Utf8,
            )
            .str.strip_chars()
            .str.to_uppercase()
            .alias("_team")
        )

        games_expr = (
            self._coalesce_expr(
                data,
                ["games", "games_played", "games_active"],
                alias="_games_raw",
                default=0,
                dtype=pl.Float64,
            )
            .cast(pl.Float64, strict=False)
            .alias("_games_raw")
        )

        week_col = "week" if "week" in data.columns else None

        def _num(col: str) -> pl.Expr:
            return (
                pl.col(col).cast(pl.Float64, strict=False).fill_null(0)
                if col in data.columns
                else pl.lit(0.0)
            )

        def _num_candidates(columns: list[str], alias: str) -> pl.Expr:
            available = [
                pl.col(column).cast(pl.Float64, strict=False)
                for column in columns
                if column in data.columns
            ]
            if not available:
                return pl.lit(0.0).alias(alias)
            expr = available[0] if len(available) == 1 else pl.coalesce(available)
            return expr.fill_null(0.0).alias(alias)

        wpa_columns = ["total_wpa", "wpa_total", "wpa", "qb_wpa"]
        epa_columns = ["total_epa", "epa_total", "epa", "qb_epa"]
        has_wpa_source = any(column in data.columns for column in wpa_columns)
        has_epa_source = any(column in data.columns for column in epa_columns)
        season_candidates = self._extract_seasons_from_frame(data)
        qb_impact_map: dict[int, dict[str, float]] = {}
        if fetch_impacts and player and season_candidates and (not has_wpa_source or not has_epa_source):
            qb_impact_map = self._get_quarterback_impact_map(player, season_candidates)

        pass_comp = _num("completions").alias("_pass_comp")
        pass_att = _num("attempts").alias("_pass_att")
        pass_yds = _num("passing_yards").alias("_pass_yds")
        pass_td = _num("passing_tds").alias("_pass_td")
        pass_int = _num("passing_interceptions").alias("_pass_int")
        sacks_taken = _num("sacks_suffered").alias("_sacks_taken")
        sack_yards = _num("sack_yards_lost").alias("_sack_yards")
        sack_fumbles = _num("sack_fumbles").alias("_sack_fumbles")
        sack_fumbles_lost = _num("sack_fumbles_lost").alias("_sack_fumbles_lost")
        rush_att = _num("carries").alias("_rush_att")
        rush_yds = _num("rushing_yards").alias("_rush_yds")
        rush_td = _num("rushing_tds").alias("_rush_td")
        rush_fumbles = _num("rushing_fumbles").alias("_rush_fumbles")
        rush_fumbles_lost = _num("rushing_fumbles_lost").alias("_rush_fumbles_lost")
        wpa_expr = _num_candidates(wpa_columns, "_wpa")
        epa_expr = _num_candidates(epa_columns, "_epa")

        prepared = data.with_columns(
            [
                team_expr,
                games_expr,
                pass_comp,
                pass_att,
                pass_yds,
                pass_td,
                pass_int,
                sacks_taken,
                sack_yards,
                sack_fumbles,
                sack_fumbles_lost,
                rush_att,
                rush_yds,
                rush_td,
                rush_fumbles,
                rush_fumbles_lost,
                wpa_expr,
                epa_expr,
            ]
        )
        prepared = prepared.with_columns(
            pl.col("_team")
            .replace("", None)
            .alias("_team"),
        )
        if week_col is not None:
            prepared = prepared.with_columns(
                pl.col(week_col).cast(pl.Int64, strict=False).alias("_week")
            )
        else:
            prepared = prepared.with_columns(pl.lit(None).alias("_week"))

        agg_exprs: list[pl.Expr] = [
            pl.col("season").alias("season"),
            pl.col("_team").alias("_team"),
            pl.col("_games_raw").max().alias("_games_raw"),
        ]
        if week_col is not None:
            agg_exprs.append(pl.col(week_col).cast(pl.Int64, strict=False).alias("_week"))

        aggregation = prepared.group_by("season", "_team").agg(
            pl.col("_games_raw").max().alias("_games_raw"),
            pl.col("_week").drop_nulls().n_unique().alias("_games_from_weeks"),
            pl.col("_pass_comp").sum().alias("_pass_comp"),
            pl.col("_pass_att").sum().alias("_pass_att"),
            pl.col("_pass_yds").sum().alias("_pass_yds"),
            pl.col("_pass_td").sum().alias("_pass_td"),
            pl.col("_pass_int").sum().alias("_pass_int"),
            pl.col("_sacks_taken").sum().alias("_sacks_taken"),
            pl.col("_sack_yards").sum().alias("_sack_yards"),
            pl.col("_sack_fumbles").sum().alias("_sack_fumbles"),
            pl.col("_sack_fumbles_lost").sum().alias("_sack_fumbles_lost"),
            pl.col("_rush_att").sum().alias("_rush_att"),
            pl.col("_rush_yds").sum().alias("_rush_yds"),
            pl.col("_rush_td").sum().alias("_rush_td"),
            pl.col("_rush_fumbles").sum().alias("_rush_fumbles"),
            pl.col("_rush_fumbles_lost").sum().alias("_rush_fumbles_lost"),
            pl.col("_wpa").sum().alias("_wpa"),
            pl.col("_epa").sum().alias("_epa"),
        )

        aggregated = (
            aggregation.with_columns(
                pl.when(pl.col("_games_raw") > 0)
                .then(pl.col("_games_raw"))
                .otherwise(pl.col("_games_from_weeks"))
                .cast(pl.Int64, strict=False)
                .alias("_games")
            )
            .sort(["season", "_team"], descending=[True, False])
        )

        rows: list[list[str]] = []
        snaps_total = 0.0
        for row in aggregated.iter_rows(named=True):
            season = row.get("season")
            team_value = row.get("_team") or ""
            if not team_value:
                team_value = self._infer_team_for_row(row, original_stats)
            season_int = int(season) if season is not None else None
            age_value = self._compute_age_for_season(player, season_int) if season_int is not None else None
            record_tuple = (0, 0, 0)
            if season_int is not None and team_value and hasattr(self._service, "get_team_record"):
                try:
                    record_tuple = self._service.get_team_record(team_value, season_int)
                except Exception as exc:  # pragma: no cover - defensive
                    logger.debug("Failed to fetch team record for %s %s: %s", team_value, season_int, exc)
                    record_tuple = (0, 0, 0)
            team_record = self._format_team_record(*record_tuple)

            games_played = int(row.get("_games") or 0)
            completions = float(row.get("_pass_comp") or 0.0)
            attempts = float(row.get("_pass_att") or 0.0)
            pass_yards_total = float(row.get("_pass_yds") or 0.0)
            pass_touchdowns = float(row.get("_pass_td") or 0.0)
            interceptions = float(row.get("_pass_int") or 0.0)
            sacks_taken = float(row.get("_sacks_taken") or 0.0)
            sack_yards_lost = float(row.get("_sack_yards") or 0.0)
            sack_fumbles_total = float(row.get("_sack_fumbles") or 0.0)
            sack_fumbles_lost = float(row.get("_sack_fumbles_lost") or 0.0)
            rush_attempts = float(row.get("_rush_att") or 0.0)
            rush_yards_total = float(row.get("_rush_yds") or 0.0)
            rush_touchdowns = float(row.get("_rush_td") or 0.0)
            rush_fumbles_total = float(row.get("_rush_fumbles") or 0.0)
            rush_fumbles_lost = float(row.get("_rush_fumbles_lost") or 0.0)

            dropbacks = attempts + sacks_taken
            snaps_played = dropbacks + rush_attempts
            snaps_total += snaps_played

            wpa_value = float(row.get("_wpa") or 0.0) if has_wpa_source else None
            epa_value = float(row.get("_epa") or 0.0) if has_epa_source else None
            impact = qb_impact_map.get(season_int) if season_int is not None else None
            if wpa_value is None and impact:
                wpa_value = impact.get("wpa")
            if epa_value is None and impact:
                epa_value = impact.get("epa")
            completion_pct = (completions / attempts * 100.0) if attempts > 0 else 0.0
            touchdown_pct = (pass_touchdowns / attempts * 100.0) if attempts > 0 else 0.0
            interception_pct = (interceptions / attempts * 100.0) if attempts > 0 else 0.0
            yards_per_attempt = (pass_yards_total / attempts) if attempts > 0 else 0.0
            yards_per_completion = (pass_yards_total / completions) if completions > 0 else 0.0
            sack_pct = (sacks_taken / dropbacks * 100.0) if dropbacks > 0 else 0.0
            negative_yards_per_attempt = (-sack_yards_lost / dropbacks) if dropbacks > 0 else 0.0
            any_yards_per_attempt = (
                (pass_yards_total + 20 * pass_touchdowns - 45 * interceptions - sack_yards_lost) / dropbacks
                if dropbacks > 0
                else 0.0
            )
            rush_yards_per_attempt = (rush_yards_total / rush_attempts) if rush_attempts > 0 else 0.0
            total_touchdowns = pass_touchdowns + rush_touchdowns
            total_turnovers = interceptions + rush_fumbles_lost + sack_fumbles_lost
            total_yards = pass_yards_total + rush_yards_total
            qb_rating = self._calculate_passer_rating(completions, attempts, pass_yards_total, pass_touchdowns, interceptions)

            row_values = [
                self._format_value(season),
                self._format_optional_int(age_value),
                self._format_value(team_value),
                team_record or "—",
                self._format_int(games_played),
                self._format_int(snaps_played),
                self._format_float(wpa_value, 3),
                self._format_float(epa_value, 1),
                self._format_float(qb_rating, 1),
                self._format_int(total_touchdowns),
                self._format_int(total_turnovers),
                self._format_int(total_yards),
                self._format_int(completions),
                self._format_int(attempts),
                self._format_float(completion_pct, 1),
                self._format_int(pass_yards_total),
                self._format_int(pass_touchdowns),
                self._format_float(touchdown_pct, 2),
                self._format_int(interceptions),
                self._format_float(interception_pct, 2),
                self._format_float(yards_per_attempt, 2),
                self._format_float(yards_per_completion, 2),
                self._format_int(sacks_taken),
                self._format_float(sack_pct, 2),
                self._format_int(-abs(sack_yards_lost)),
            ]
            rows.append(row_values)

        summary = {
            "pass_yards": float(aggregated["_pass_yds"].sum() or 0),
            "rush_yards": float(aggregated["_rush_yds"].sum() or 0),
            "receiving_yards": 0.0,
            "total_touchdowns": float((aggregated["_pass_td"].sum() or 0) + (aggregated["_rush_td"].sum() or 0)),
            "games": float(aggregated["_games"].sum() or 0),
            "snaps": float(snaps_total),
        }
        return rows, summary

    def _compute_age_for_season(self, player: Player, season: int | None) -> int | None:
        """Compute player age on September 1 of the given season."""

        if season is None or not player.profile.birth_date:
            return None
        birth = player.profile.birth_date
        try:
            reference = date(season, 9, 1)
        except ValueError:
            reference = date(season, 9, 1 if season >= birth.year else birth.day)
        age = reference.year - birth.year - (
            (reference.month, reference.day) < (birth.month, birth.day)
        )
        return age

    @staticmethod
    def _calculate_passer_rating(
        completions: float,
        attempts: float,
        yards: float,
        touchdowns: float,
        interceptions: float,
    ) -> float:
        """Calculate the traditional NFL passer rating."""

        if attempts <= 0:
            return 0.0

        a = max(0.0, min(2.375, (completions / attempts - 0.3) * 5))
        b = max(0.0, min(2.375, (yards / attempts - 3) * 0.25))
        c = max(0.0, min(2.375, (touchdowns / attempts) * 20))
        d = max(0.0, min(2.375, 2.375 - (interceptions / attempts) * 25))
        return ((a + b + c + d) / 6) * 100

    @staticmethod
    def _format_float(value: float | int | None, decimals: int = 1) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and math.isnan(value):
            return ""
        format_str = f"{{:.{decimals}f}}"
        return format_str.format(float(value))

    @staticmethod
    def _format_team_record(wins: int, losses: int, ties: int) -> str:
        if wins == losses == ties == 0:
            return ""
        if ties:
            return f"{wins}-{losses}-{ties}"
        return f"{wins}-{losses}"

    def _infer_team_for_row(self, row: dict[str, Any], stats: pl.DataFrame) -> str:
        """Infer team abbreviation for a given season row when missing."""

        season = row.get("season")
        if season is None or "season" not in stats.columns:
            return ""

        season_df = stats.filter(pl.col("season") == season)
        if season_df.height == 0:
            return ""

        for column in ["team", "team_abbr", "recent_team", "current_team_abbr"]:
            if column not in season_df.columns:
                continue
            series = season_df[column].drop_nulls()
            if series.len() == 0:
                continue
            series = series.cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase()
            mask = series.str.len_chars() > 0
            if mask.any():
                filtered = series.filter(mask)
                if filtered.len() > 0:
                    return str(filtered[0])
        return ""

    def _infer_position_for_row(self, row: dict[str, Any], stats: pl.DataFrame) -> str:
        """Infer position for a given season row when missing."""

        season = row.get("season")
        if season is None or "season" not in stats.columns:
            return ""

        season_df = stats.filter(pl.col("season") == season)
        if season_df.height == 0:
            return ""

        for column in ["player_position", "position", "position_group"]:
            if column not in season_df.columns:
                continue
            series = season_df[column].drop_nulls()
            if series.len() == 0:
                continue
            series = series.cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase()
            mask = series.str.len_chars() > 0
            if mask.any():
                filtered = series.filter(mask)
                if filtered.len() > 0:
                    return str(filtered[0])
        return ""

    @staticmethod
    def _safe_str(value: Any) -> str:
        return str(value).strip() if isinstance(value, str) else ""

    @staticmethod
    def _format_value(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return text if text.lower() not in {"none", "nan"} else ""

    @staticmethod
    def _format_int(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            if math.isnan(value):
                return ""
            value = int(round(value))
        if isinstance(value, int):
            return str(value)
        text = str(value).strip()
        if not text:
            return ""
        try:
            return str(int(float(text)))
        except ValueError:
            return text

    @staticmethod
    def _coalesce_expr(
        frame: pl.DataFrame,
        columns: list[str],
        *,
        alias: str,
        default: Any = None,
        dtype: pl.DataType | None = None,
    ) -> pl.Expr:
        exprs = [pl.col(column) for column in columns if column in frame.columns]
        if not exprs:
            expr: pl.Expr = pl.lit(default)
        elif len(exprs) == 1:
            expr = exprs[0]
        else:
            expr = pl.coalesce(exprs)
        if dtype is not None:
            expr = expr.cast(dtype, strict=False)
        if default is not None:
            expr = expr.fill_null(default)
        return expr.alias(alias)

    @staticmethod
    def _clamp_rating(value: int) -> int:
        value = max(20, min(80, int(round(value))))
        remainder = value % 5
        if remainder:
            value = value - remainder if remainder < 3 else value + (5 - remainder)
        return max(20, min(80, value))

    @staticmethod
    def _format_optional_int(value: Any) -> str:
        text = PlayerDetailPage._format_int(value)
        return text if text else "—"

    @staticmethod
    def _first_non_empty(*values: Any) -> Any | None:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str):
                if value.strip() == "":
                    continue
                return value
            return value
        return None

    @staticmethod
    def _format_text(value: Any) -> str:
        if value is None:
            return "—"
        if isinstance(value, str):
            text = value.strip()
            return text if text else "—"
        return str(value)

    @staticmethod
    def _parse_numeric(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            if isinstance(value, float) and math.isnan(value):
                return None
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            cleaned = cleaned.replace("$", "").replace(",", "")
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    @staticmethod
    def _extract_numeric_value(source: Any, keys: list[str]) -> float | None:
        if not isinstance(source, dict):
            return None
        for key in keys:
            if key not in source:
                continue
            parsed = PlayerDetailPage._parse_numeric(source.get(key))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _derive_salary_text(payload: dict[str, Any], raw: dict[str, Any]) -> str:
        value = PlayerDetailPage._extract_numeric_value(
            payload,
            [
                "salary_aav",
                "salary",
                "contract_aav",
                "apy",
                "aav",
                "average_salary",
                "apy_millions",
            ],
        )

        if value is None:
            value = PlayerDetailPage._extract_numeric_value(
                raw,
                [
                    "salary_aav",
                    "contract_aav",
                    "apy",
                    "apy_current",
                    "average_salary",
                    "aav",
                    "avg_salary",
                    "apy_millions",
                ],
            )

        if value is None:
            return "—"

        assume_millions = False
        if isinstance(payload, dict):
            assume_millions = any(key in payload for key in ("apy_millions",))
        if isinstance(raw, dict) and not assume_millions:
            assume_millions = any(key in raw for key in ("apy_millions", "apy_cap_millions"))

        return PlayerDetailPage._format_currency(value, assume_millions=assume_millions)

    @staticmethod
    def _format_currency(value: float, *, assume_millions: bool = False) -> str:
        try:
            amount = float(value)
        except (TypeError, ValueError):
            return "—"

        if assume_millions:
            millions = amount
        else:
            if abs(amount) >= 1_000_000:
                millions = amount / 1_000_000.0
            elif abs(amount) <= 200:
                millions = amount
            else:
                millions = amount / 1_000_000.0

        return f"${millions:,.1f}M"

    @staticmethod
    def _format_signed_through(value: Any) -> str:
        if value is None:
            return "—"
        if isinstance(value, (int, float)):
            if isinstance(value, float) and math.isnan(value):
                return "—"
            year = int(round(value))
            return str(year)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return "—"
            if text.isdigit():
                return text
            parsed = PlayerDetailPage._parse_date_value(text)
            if parsed:
                return str(parsed.year)
            return text
        parsed_date = PlayerDetailPage._parse_date_value(value)
        if parsed_date:
            return str(parsed_date.year)
        return PlayerDetailPage._format_text(value)

    @staticmethod
    def _parse_date_value(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return date.fromisoformat(text)
            except ValueError:
                pass
            for fmt in (
                "%m/%d/%Y",
                "%m/%d/%y",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%b %d %Y",
                "%B %d %Y",
            ):
                try:
                    return datetime.strptime(text, fmt).date()
                except ValueError:
                    continue
        return None

    @staticmethod
    def _format_birth_date(value: Any) -> str:
        parsed = PlayerDetailPage._parse_date_value(value)
        if not parsed:
            return "—"
        suffix = PlayerDetailPage._ordinal_suffix(parsed.day)
        month = parsed.strftime("%B")
        return f"{month} {parsed.day}{suffix}, {parsed.year}"

    @staticmethod
    def _ordinal_suffix(day: int) -> str:
        if 11 <= day % 100 <= 13:
            return "th"
        return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
