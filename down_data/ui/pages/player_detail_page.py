"""Temporary player detail page shown from the search results."""

from __future__ import annotations

from datetime import date, datetime
import logging
import math
from typing import Any

import polars as pl
from PySide6.QtCore import Qt, Signal
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
                    columns=["Season", "Team", "Primary Pos", "Games", "Snaps"],
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
        self._table_columns: list[str] = ["Season", "Team", "Primary Pos", "Games", "Snaps"]
        self._is_defensive: bool = False
        self._is_quarterback: bool = False
        self._current_player: Player | None = None
        self._basic_ratings: list[RatingBreakdown] = []

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
        self._update_personal_details_views()
        self._load_player_stats()
        self._show_content(self._active_section, self._active_subsection)

    def clear_display(self) -> None:
        """Reset any stored payload and ensure the content area is empty."""

        self._current_payload = None
        self._season_rows = []
        self._current_player = None
        self._basic_ratings = []
        self._update_personal_details_views()
        self._update_basic_ratings_views()
        self._update_table_views()
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

    def _load_player_stats(self) -> None:
        """Fetch player stats and update table rows."""

        self._season_rows = []

        if self._service is None or not self._current_payload:
            self._update_table_views()
            return

        full_name = self._safe_str(self._current_payload.get("full_name") or self._current_payload.get("name"))
        if not full_name:
            self._update_table_views()
            return

        team = self._safe_str(self._current_payload.get("team"))
        position = self._safe_str(self._current_payload.get("position"))

        try:
            query = PlayerQuery(name=full_name, team=team or None, position=position or None)
            player = self._service.load_player(query)
            self._current_player = player
            position_value = (player.profile.position or player.profile.position_group or "").upper()
            self._is_quarterback = position_value == "QB"
            self._is_defensive = bool(getattr(player, "is_defensive")() if hasattr(player, "is_defensive") else False)
            if self._is_quarterback:
                self._table_columns = [
                    "Season",
                    "Age",
                    "Team",
                    "Team Record",
                    "Games Played",
                    "Snaps Played",
                    "QB Rating",
                    "Total Touchdowns",
                    "Total Turnovers",
                    "Total Yards",
                    "Completions",
                    "Attempts",
                    "Completion Percentage",
                    "Yards",
                    "Touchdowns",
                    "Touchdown %",
                    "Interceptions",
                    "Interceptions %",
                    "Yards/Attempt",
                    "Yards/Completion",
                    "Sacks Taken",
                    "Sack %",
                    "Sack Yards",
                ]
            elif self._is_defensive:
                self._table_columns = [
                    "Season",
                    "Team",
                    "Primary Pos",
                    "Games",
                    "Snaps",
                    "Tackles",
                    "Solo Tk",
                    "Ast Tk",
                    "INT",
                ]
            else:
                self._table_columns = [
                    "Season",
                    "Team",
                    "Primary Pos",
                    "Games",
                    "Snaps",
                    "Pass Yds",
                    "Rush Yds",
                    "Rec Yds",
                    "Total TD",
                ]

            cached_stats = pl.DataFrame()
            player_id = player.profile.gsis_id
            if player_id:
                cached_stats = self._service.get_basic_offense_stats(player_id=player_id)

            if cached_stats.height > 0:
                self._season_rows, summary = self._build_table_rows_from_cached(player, cached_stats)
                self._basic_ratings = self._service.get_basic_ratings(
                    player,
                    summary=summary,
                    is_defensive=self._is_defensive,
                )
                self._update_table_views()
                self._update_basic_ratings_views()
                return

            stats = self._service.get_player_stats(player, seasons=True)
        except (PlayerNotFoundError, SeasonNotAvailableError) as exc:
            logger.debug("No stats available for %s: %s", full_name, exc)
            self._update_table_views()
            self._basic_ratings = []
            return
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load stats for %s: %s", full_name, exc)
            self._update_table_views()
            self._basic_ratings = []
            return

        try:
            self._season_rows, summary = self._build_table_rows(player, stats)
            self._basic_ratings = self._service.get_basic_ratings(
                player,
                summary=summary,
                is_defensive=self._is_defensive,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to process stats for %s: %s", full_name, exc)
            self._season_rows = []
            self._basic_ratings = []

        self._update_table_views()
        self._update_basic_ratings_views()

    def _build_table_rows(self, player: Player, stats: pl.DataFrame) -> tuple[list[list[str]], dict[str, float]]:
        """Transform raw stats into table-ready rows and summary metrics."""

        if stats is None or stats.height == 0:
            return [], {}

        data = stats

        if "season_type" in data.columns:
            data = data.filter(pl.col("season_type") == "REG")

        if data.height == 0 or "season" not in data.columns:
            return [], {}

        if self._is_quarterback:
            return self._build_qb_table_rows(player, data, stats)

        return self._build_standard_table_rows(data, stats)

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

    def _build_table_rows_from_cached(
        self,
        player: Player,
        cached_stats: pl.DataFrame,
    ) -> tuple[list[list[str]], dict[str, float]]:
        """Construct quarterback rows from the cached basic offense stats."""

        if cached_stats.is_empty():
            return [], {}

        data = cached_stats.sort("season", descending=True)
        rows: list[list[str]] = []
        snaps_total = 0.0

        for record in data.iter_rows(named=True):
            season = record.get("season")
            team_value = str(record.get("team") or "").upper()
            games_played = int(record.get("games_played") or 0)

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

            age_value = self._compute_age_for_season(player, int(season) if season is not None else None)
            team_record = ""
            if team_value and season is not None:
                try:
                    record_tuple = self._service.get_team_record(team_value, int(season))
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
