"""Temporary player detail page shown from the search results."""

from __future__ import annotations

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
)

from down_data.backend.player_service import PlayerService
from down_data.core import PlayerQuery, PlayerNotFoundError, SeasonNotAvailableError
from down_data.ui.widgets import GridCell, GridLayoutManager, Panel, TablePanel

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
        self._load_player_stats()
        self._show_content(self._active_section, self._active_subsection)

    def clear_display(self) -> None:
        """Reset any stored payload and ensure the content area is empty."""

        self._current_payload = None
        self._season_rows = []
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
                widget.update_table_rows(self._season_rows)

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
            stats = player.fetch_stats(seasons=True)
        except (PlayerNotFoundError, SeasonNotAvailableError) as exc:
            logger.debug("No stats available for %s: %s", full_name, exc)
            self._update_table_views()
            return
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to load stats for %s: %s", full_name, exc)
            self._update_table_views()
            return

        try:
            self._season_rows = self._build_table_rows(stats)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to process stats for %s: %s", full_name, exc)
            self._season_rows = []

        self._update_table_views()

    def _build_table_rows(self, stats: pl.DataFrame) -> list[list[str]]:
        """Transform raw stats into table-ready rows."""

        if stats is None or stats.height == 0:
            return []

        data = stats

        if "season_type" in data.columns:
            data = data.filter(pl.col("season_type") == "REG")

        if data.height == 0 or "season" not in data.columns:
            return []

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

        prepared = (
            data.with_columns(
                [
                    team_expr,
                    position_expr,
                    games_expr,
                    snap_expr,
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
                team_value = self._infer_team_for_row(row, stats)
            position_value = row.get("_position") or ""
            if not position_value:
                position_value = self._infer_position_for_row(row, stats)
            rows.append(
                [
                    self._format_value(row.get("season")),
                    self._format_value(team_value),
                    self._format_value(position_value),
                    self._format_int(row.get("_games")),
                    self._format_int(row.get("_snaps")),
                ]
            )
        return rows

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
