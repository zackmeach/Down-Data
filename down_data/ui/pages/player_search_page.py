"""Player search landing page."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from typing import Any

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal
from PySide6.QtWidgets import (
    QAbstractSpinBox,
    QComboBox,
    QDoubleSpinBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from down_data.backend.player_service import PlayerService
from down_data.ui.widgets import GridCell, GridLayoutManager, FilterPanel, RangeSelector, TablePanel

from .base_page import SectionPage


@dataclass(frozen=True)
class Threshold:
    """Numeric threshold paired with a comparison operator."""

    value: float
    operator: str


@dataclass(frozen=True)
class SearchCriteria:
    """Normalized filter set for executing a player search."""

    is_offense: bool
    age_min: int
    age_max: int
    service_min: int
    service_max: int
    position_filter: str
    team_filter: str
    year_filter: str
    draft_round_value: str
    draft_position_value: str
    draft_team_value: str
    contract_years: Threshold | None
    contract_value: Threshold | None
    contract_guaranteed: Threshold | None
    contract_apy: Threshold | None
    contract_apy_cap_pct: Threshold | None
    contract_year_signed: str
    value_variant: str


class SearchWorkerSignals(QObject):
    """Signals emitted by the background search worker."""

    def __init__(self) -> None:
        super().__init__()

    finished = Signal(object)
    error = Signal(str)


class SearchWorker(QRunnable):
    """Execute player filtering on a background thread."""

    def __init__(self, service: PlayerService, criteria: SearchCriteria) -> None:
        super().__init__()
        self._service = service
        self._criteria = criteria
        self.signals = SearchWorkerSignals()

    def run(self) -> None:  # pragma: no cover - background execution
        try:
            raw_players_df = self._service.get_all_players()
            prepared = PlayerSearchPage._prepare_player_directory_frame(raw_players_df)
            filtered = PlayerSearchPage._filter_players_by_criteria(prepared, self._criteria)
            self.signals.finished.emit(filtered)
        except Exception as exc:  # pragma: no cover - surfaced to UI
            self.signals.error.emit(str(exc))


class PlayerSearchPage(SectionPage):
    """Find Player page - search and filter players.
    
    This page is displayed when navigating to Players > Find A Player.
    Navigation bars are handled by the parent ContentPage.
    
    Layout:
    - Filter panel: rows 0-23, columns 0-2 (left sidebar, 3 columns wide)
    - Results table: rows 0-23, columns 3-11 (center/right area, 9 columns wide)
    - Detail panel: TBD (optional right sidebar for preview)
    """

    playerSelected = Signal(dict)

    def __init__(
        self, 
        *, 
        service: PlayerService, 
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title="Player Search", parent=parent)
        self._service = service
        self._page_size = 100
        self._current_page = 0
        self._total_pages = 0
        self._current_results_df = None  # polars DataFrame storing filtered results
        self._search_in_progress = False
        self._initialize_filter_options()

        # Filter control references per tab
        self._age_filters: dict[str, tuple[QComboBox, QComboBox]] = {}
        self._service_filters: dict[str, tuple[QComboBox, QComboBox]] = {}
        self._position_filters: dict[str, QComboBox] = {}
        self._team_filters: dict[str, QComboBox] = {}
        self._year_filters: dict[str, QComboBox] = {}
        self._contract_years_filters: dict[str, tuple[QSpinBox, QComboBox]] = {}
        self._contract_value_filters: dict[str, tuple[QDoubleSpinBox, QComboBox]] = {}
        self._contract_guaranteed_filters: dict[str, tuple[QDoubleSpinBox, QComboBox]] = {}
        self._contract_apy_filters: dict[str, tuple[QDoubleSpinBox, QComboBox]] = {}
        self._contract_apy_cap_pct_filters: dict[str, tuple[QDoubleSpinBox, QComboBox]] = {}
        self._contract_year_signed_filters: dict[str, QComboBox] = {}
        self._draft_round_filters: dict[str, QComboBox] = {}
        self._draft_position_filters: dict[str, QComboBox] = {}
        self._draft_team_filters: dict[str, QComboBox] = {}
        self._value_variant_toggles: dict[str, QPushButton] = {}
        self._value_variant_state: str = "original"
        
        # Remove the default margins from SectionPage's root_layout
        self.root_layout.setContentsMargins(0, 0, 0, 0)
        self.root_layout.setSpacing(0)
        
        # Create grid layout manager (12 columns x 24 rows)
        # This page is embedded in ContentPage, so it uses its own coordinate system
        self._grid_layout = GridLayoutManager(
            parent=self,
            columns=12,
            rows=24
        )

        self._thread_pool = QThreadPool.globalInstance()
        self._active_worker: SearchWorker | None = None
        
        # Build the UI panels
        self._build_filter_panel()
        self._build_results_table()
        
        # TODO: Add detail panel (optional right sidebar for preview)

    def _initialize_filter_options(self) -> None:
        """Prepare option lists used by filter dropdowns."""
        # Position group options
        self._offense_positions = [
            "Any",
            "QB",
            "RB",
            "WR",
            "TE",
            "FB",
            "HB",
            "T",
            "G",
            "C",
            "OT",
            "OG",
        ]
        self._defense_positions = [
            "Any",
            "DL",
            "DE",
            "DT",
            "NT",
            "LB",
            "ILB",
            "OLB",
            "MLB",
            "DB",
            "CB",
            "S",
            "SS",
            "FS",
        ]

        # Team options - gather from player directory
        self._team_options = ["Any"]
        players_df = None
        try:
            players_df = self._service.get_all_players()
            if isinstance(players_df, pl.DataFrame) and players_df.height > 0:
                team_values: set[str] = set()
                for column in ["team_abbr", "current_team_abbr", "recent_team", "team"]:
                    if column in players_df.columns:
                        values = (
                            players_df[column]
                            .drop_nulls()
                            .unique()
                            .to_list()
                        )
                        team_values.update(str(value).upper() for value in values if value)
                if team_values:
                    self._team_options = ["Any"] + sorted(team_values)
        except Exception as exc:
            print(f"[Search] Warning: failed to load team options ({exc})")

        # Year options - current season plus previous 5 seasons
        current_year = datetime.now().year
        seasons = []
        for start_year in range(current_year - 5, current_year + 1):
            end_year_two = str((start_year + 1) % 100).zfill(2)
            seasons.append(f"{start_year}-{end_year_two}")
        self._year_options = ["Any"] + seasons if seasons else ["Any"]
        self._default_year = seasons[-1] if seasons else "Any"
        # Contract year signed options
        self._contract_year_signed_options = ["Any"]
        self._default_contract_year_signed = "Any"
        if isinstance(players_df, pl.DataFrame) and "year_signed" in players_df.columns:
            years_signed = (
                players_df["year_signed"]
                .drop_nulls()
                .unique()
                .cast(pl.Int64, strict=False)
                .sort()
                .to_list()
            )
            years_signed = [int(y) for y in years_signed if y is not None]
            if years_signed:
                self._contract_year_signed_options = ["Any"] + [str(y) for y in years_signed]
                self._default_contract_year_signed = str(years_signed[-1])
        # Draft round and pick options (defaults for fallback)
        self._draft_round_options = ["Any"] + [str(i) for i in range(1, 8)] + ["Undrafted"]
        self._draft_position_options = ["Any"] + [str(i) for i in range(1, 33)] + ["N/A"]
        self._draft_team_options = self._team_options.copy()

    @staticmethod
    def _format_cell_value(value: Any) -> str:
        """Format cell values for table display."""
        if value is None:
            return ""
        if isinstance(value, float):
            if math.isnan(value):
                return ""
            if value.is_integer():
                return str(int(value))
            formatted = f"{value:.1f}".rstrip("0").rstrip(".")
            return formatted or "0"
        if isinstance(value, int):
            return str(value)
        text = str(value).strip()
        return "" if text.lower() in {"nan", "none"} else text
        # Draft-related options
        self._draft_round_options = ["Any"] + [str(i) for i in range(1, 8)] + ["Undrafted"]
        self._draft_position_options = ["Any"] + [str(i) for i in range(1, 33)] + ["N/A"]
        self._draft_team_options = self._team_options.copy()

    @staticmethod
    def _prepare_player_directory_frame(frame: pl.DataFrame | None) -> pl.DataFrame:
        """Return a normalised copy of the player directory with derived fields."""
        if frame is None:
            return pl.DataFrame()
        if frame.height == 0:
            return frame

        prepared = frame.clone()

        # Ensure consistent position formatting
        if "position" in prepared.columns:
            prepared = prepared.with_columns(
                pl.col("position")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.to_uppercase()
                .alias("position")
            )

        # Add missing team abbreviation columns using available sources
        if "team_abbr" not in prepared.columns:
            for source in ("recent_team", "current_team_abbr", "team"):
                if source in prepared.columns:
                    prepared = prepared.with_columns(pl.col(source).alias("team_abbr"))
                    break
        if "current_team_abbr" not in prepared.columns:
            for source in ("team_abbr", "recent_team", "team"):
                if source in prepared.columns:
                    prepared = prepared.with_columns(pl.col(source).alias("current_team_abbr"))
                    break

        for team_col in ("team_abbr", "current_team_abbr", "recent_team", "team"):
            if team_col in prepared.columns:
                prepared = prepared.with_columns(
                    pl.col(team_col)
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .str.to_uppercase()
                    .alias(team_col)
                )

        # Derive age from birth date if needed
        birth_column = next(
            (col for col in ("birth_date", "birthdate", "dob") if col in prepared.columns),
            None,
        )

        if birth_column:
            birth_expr = pl.col(birth_column)
            if prepared.schema.get(birth_column) == pl.Utf8:
                birth_expr = birth_expr.str.strptime(pl.Date, strict=False)
            else:
                birth_expr = birth_expr.cast(pl.Date, strict=False)

            prepared = prepared.with_columns(birth_expr.alias("_birth_date_tmp"))

            today = date.today()
            computed_age_expr = (
                pl.when(pl.col("_birth_date_tmp").is_not_null())
                .then(
                    pl.lit(today.year)
                    - pl.col("_birth_date_tmp").dt.year()
                    - (
                        (
                            (pl.col("_birth_date_tmp").dt.month() > today.month)
                            | (
                                (pl.col("_birth_date_tmp").dt.month() == today.month)
                                & (pl.col("_birth_date_tmp").dt.day() > today.day)
                            )
                        ).cast(pl.Int16)
                    )
                )
                .otherwise(None)
                .cast(pl.Int16)
                .alias("_computed_age")
            )

            prepared = prepared.with_columns(computed_age_expr)
            prepared = prepared.drop("_birth_date_tmp")

            if "_computed_age" in prepared.columns:
                if "age" in prepared.columns:
                    prepared = prepared.with_columns(
                        pl.when(
                            pl.col("age")
                            .cast(pl.Int16, strict=False)
                            .is_null()
                        )
                        .then(pl.col("_computed_age"))
                        .otherwise(
                            pl.col("age").cast(pl.Int16, strict=False)
                        )
                        .alias("age")
                    )
                else:
                    prepared = prepared.with_columns(pl.col("_computed_age").alias("age"))
                prepared = prepared.drop("_computed_age")

        return prepared

    def _build_filter_panel(self) -> None:
        """Create the left filter panel for search controls."""
        self._filter_panel = FilterPanel(title="FIND PLAYER", parent=self)
        
        # Ensure panel has a minimum size
        self._filter_panel.setMinimumWidth(250)
        self._filter_panel.setMinimumHeight(400)
        
        # Add Offense/Defense tabs at the top (like Pitchers/Hitters in OOTP)
        self._position_tabs = QTabWidget(self._filter_panel)
        self._position_tabs.setObjectName("PositionTabs")
        
        # Create tab content widgets using grid layouts for consistent alignment
        self._offense_widget = QWidget()
        offense_layout = QGridLayout(self._offense_widget)
        offense_layout.setContentsMargins(8, 8, 8, 8)
        offense_layout.setHorizontalSpacing(8)
        offense_layout.setVerticalSpacing(12)

        offense_layout.addWidget(self._add_age_filter("offense"), 0, 0)
        offense_layout.addWidget(self._add_service_time_filter("offense"), 0, 1)
        offense_layout.addWidget(self._add_value_variant_filter("offense"), 0, 2)

        offense_layout.addWidget(self._add_position_filter("offense"), 1, 0)
        offense_layout.addWidget(self._add_team_filter("offense"), 1, 1)
        offense_layout.addWidget(self._add_year_filter("offense"), 1, 2)

        offense_layout.addWidget(self._add_contract_years_filter("offense"), 2, 0)
        offense_layout.addWidget(self._add_contract_year_signed_filter("offense"), 2, 1)
        offense_layout.addWidget(self._add_contract_apy_cap_pct_filter("offense"), 2, 2)

        offense_layout.addWidget(self._add_contract_value_filter("offense"), 3, 0)
        offense_layout.addWidget(self._add_contract_guaranteed_filter("offense"), 3, 1)
        offense_layout.addWidget(self._add_contract_apy_filter("offense"), 3, 2)

        offense_layout.addWidget(self._add_draft_round_filter("offense"), 4, 0)
        offense_layout.addWidget(self._add_draft_position_filter("offense"), 4, 1)
        offense_layout.addWidget(self._add_draft_team_filter("offense"), 4, 2)

        for column in range(3):
            offense_layout.setColumnStretch(column, 1)
        offense_layout.setRowStretch(5, 1)

        # Ensure draft position combo starts disabled if round = Undrafted
        self._on_draft_round_changed("offense", self._draft_round_filters["offense"].currentText())

        self._defense_widget = QWidget()
        defense_layout = QGridLayout(self._defense_widget)
        defense_layout.setContentsMargins(8, 8, 8, 8)
        defense_layout.setHorizontalSpacing(8)
        defense_layout.setVerticalSpacing(12)

        defense_layout.addWidget(self._add_age_filter("defense"), 0, 0)
        defense_layout.addWidget(self._add_service_time_filter("defense"), 0, 1)
        defense_layout.addWidget(self._add_value_variant_filter("defense"), 0, 2)

        defense_layout.addWidget(self._add_position_filter("defense"), 1, 0)
        defense_layout.addWidget(self._add_team_filter("defense"), 1, 1)
        defense_layout.addWidget(self._add_year_filter("defense"), 1, 2)

        defense_layout.addWidget(self._add_contract_years_filter("defense"), 2, 0)
        defense_layout.addWidget(self._add_contract_year_signed_filter("defense"), 2, 1)
        defense_layout.addWidget(self._add_contract_apy_cap_pct_filter("defense"), 2, 2)

        defense_layout.addWidget(self._add_contract_value_filter("defense"), 3, 0)
        defense_layout.addWidget(self._add_contract_guaranteed_filter("defense"), 3, 1)
        defense_layout.addWidget(self._add_contract_apy_filter("defense"), 3, 2)

        defense_layout.addWidget(self._add_draft_round_filter("defense"), 4, 0)
        defense_layout.addWidget(self._add_draft_position_filter("defense"), 4, 1)
        defense_layout.addWidget(self._add_draft_team_filter("defense"), 4, 2)

        for column in range(3):
            defense_layout.setColumnStretch(column, 1)
        defense_layout.setRowStretch(5, 1)
        self._on_draft_round_changed("defense", self._draft_round_filters["defense"].currentText())
        
        self._position_tabs.addTab(self._offense_widget, "Offense")
        self._position_tabs.addTab(self._defense_widget, "Defense")
        
        self._filter_panel.content_layout.addWidget(self._position_tabs)
        
        # Add spacer to push buttons to bottom
        self._filter_panel.content_layout.addStretch()
        
        # Add action buttons at the bottom spanning full width
        button_container = QWidget(self._filter_panel)
        button_layout = QVBoxLayout(button_container)
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.setSpacing(8)
        
        self._search_button = QPushButton("SEARCH", button_container)
        self._search_button.setObjectName("PrimaryButton")
        self._search_button.setMinimumHeight(32)
        button_layout.addWidget(self._search_button)
        
        self._clear_button = QPushButton("CLEAR TABLE", button_container)
        self._clear_button.setMinimumHeight(32)
        button_layout.addWidget(self._clear_button)
        
        self._reset_button = QPushButton("RESET FILTERS", button_container)
        self._reset_button.setMinimumHeight(32)
        button_layout.addWidget(self._reset_button)
        
        self._filter_panel.content_layout.addWidget(button_container)
        
        # Wire up button signals
        self._search_button.clicked.connect(self._perform_search)
        self._clear_button.clicked.connect(self._clear_table)
        self._reset_button.clicked.connect(self._reset_filters)
        
        # Position in grid: columns 0-2 (3 columns wide), rows 0-23 (full height)
        self._grid_layout.add_widget(
            self._filter_panel,
            GridCell(col=0, row=0, col_span=3, row_span=24)
        )
        
        # TODO: Add more filter controls inside offense/defense tabs
    
    def _add_age_filter(self, tab_key: str) -> QGroupBox:
        """Create age range filter group."""
        age_group = QGroupBox("Age")
        age_layout = QVBoxLayout()
        age_layout.setContentsMargins(4, 4, 4, 4)
        age_layout.setSpacing(4)
        age_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        age_from = QComboBox()
        age_from.setObjectName("FilterComboBox")
        for age in range(20, 46):
            age_from.addItem(str(age))
        age_from.setCurrentText("20")
        age_to = QComboBox()
        age_to.setObjectName("FilterComboBox")
        for age in range(20, 46):
            age_to.addItem(str(age))
        age_to.setCurrentText("45")

        range_widget = RangeSelector(age_from, age_to, parent=age_group)
        age_layout.addWidget(range_widget)
        age_group.setLayout(age_layout)
        # Store references for later access keyed by tab
        self._age_filters[tab_key] = (age_from, age_to)
        return age_group
    
    def _add_service_time_filter(self, tab_key: str) -> QGroupBox:
        """Create service time (years in league) range filter group."""
        service_group = QGroupBox("Service Time (Yrs)")
        service_layout = QVBoxLayout()
        service_layout.setContentsMargins(4, 4, 4, 4)
        service_layout.setSpacing(4)
        service_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        
        service_from = QComboBox()
        service_from.setObjectName("FilterComboBox")
        # Typical NFL career: 0-25 years
        for years in range(0, 26):
            service_from.addItem(str(years))
        service_from.setCurrentText("0")
        service_to = QComboBox()
        service_to.setObjectName("FilterComboBox")
        for years in range(0, 26):
            service_to.addItem(str(years))
        service_to.setCurrentText("25")
        
        range_widget = RangeSelector(service_from, service_to, parent=service_group)
        service_layout.addWidget(range_widget)
        service_group.setLayout(service_layout)
        # Store references for later access keyed by tab
        self._service_filters[tab_key] = (service_from, service_to)
        return service_group

    def _add_position_filter(self, tab_key: str) -> QGroupBox:
        """Create position dropdown group."""
        position_group = QGroupBox("Position")
        position_layout = QVBoxLayout()
        position_layout.setContentsMargins(4, 4, 4, 4)
        position_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        positions = self._offense_positions if tab_key == "offense" else self._defense_positions
        for option in positions:
            combo.addItem(option)
        combo.setCurrentIndex(0)  # Default to "Any"

        position_layout.addWidget(combo)
        position_group.setLayout(position_layout)
        self._position_filters[tab_key] = combo
        return position_group

    def _add_team_filter(self, tab_key: str) -> QGroupBox:
        """Create team dropdown group."""
        team_group = QGroupBox("Team")
        team_layout = QVBoxLayout()
        team_layout.setContentsMargins(4, 4, 4, 4)
        team_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        for option in self._team_options:
            combo.addItem(option)
        combo.setCurrentIndex(0)  # "Any"

        team_layout.addWidget(combo)
        team_group.setLayout(team_layout)
        self._team_filters[tab_key] = combo
        return team_group

    def _add_year_filter(self, tab_key: str) -> QGroupBox:
        """Create league year dropdown group."""
        year_group = QGroupBox("Year")
        year_layout = QVBoxLayout()
        year_layout.setContentsMargins(4, 4, 4, 4)
        year_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        for option in self._year_options:
            combo.addItem(option)
        # Default to most current league year
        if self._default_year in self._year_options:
            combo.setCurrentText(self._default_year)
        else:
            combo.setCurrentIndex(0)

        year_layout.addWidget(combo)
        year_group.setLayout(year_layout)
        self._year_filters[tab_key] = combo
        return year_group

    def _add_draft_round_filter(self, tab_key: str) -> QGroupBox:
        """Create draft round dropdown group."""
        group = QGroupBox("Draft Round")
        layout = QVBoxLayout()
        layout.setContentsMargins(4, 4, 4, 4)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        for option in self._draft_round_options:
            combo.addItem(option)
        combo.setCurrentIndex(0)

        layout.addWidget(combo)
        group.setLayout(layout)

        self._draft_round_filters[tab_key] = combo
        combo.currentTextChanged.connect(lambda value, key=tab_key: self._on_draft_round_changed(key, value))
        return group

    def _add_draft_position_filter(self, tab_key: str) -> QGroupBox:
        """Create draft position dropdown group."""
        group = QGroupBox("Draft Pick (Round)")
        layout = QVBoxLayout()
        layout.setContentsMargins(4, 4, 4, 4)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        for option in self._draft_position_options:
            combo.addItem(option)
        combo.setCurrentIndex(0)

        layout.addWidget(combo)
        group.setLayout(layout)

        self._draft_position_filters[tab_key] = combo
        return group

    def _add_draft_team_filter(self, tab_key: str) -> QGroupBox:
        """Create draft team dropdown group."""
        group = QGroupBox("Draft Team")
        layout = QVBoxLayout()
        layout.setContentsMargins(4, 4, 4, 4)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        for option in self._draft_team_options:
            combo.addItem(option)
        combo.setCurrentIndex(0)

        layout.addWidget(combo)
        group.setLayout(layout)

        self._draft_team_filters[tab_key] = combo
        return group

    def _create_comparator_combo(self, spin: QAbstractSpinBox) -> QComboBox:
        """Create a comparator selector (Any, <, >) and wire it to a spin box."""
        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        combo.addItems(["Any", "<", ">"])
        combo.setCurrentIndex(0)
        spin.setEnabled(False)

        def _toggle_spin(text: str) -> None:
            spin.setEnabled(text != "Any")

        combo.currentTextChanged.connect(_toggle_spin)
        return combo

    def _add_contract_years_filter(self, tab_key: str) -> QGroupBox:
        """Create contract years threshold filter."""
        group = QGroupBox("Contract Years (</>)")
        layout = QHBoxLayout()
        layout.setSpacing(8)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        spin = QSpinBox()
        spin.setRange(0, 20)
        spin.setValue(spin.maximum())

        combo = self._create_comparator_combo(spin)

        layout.addWidget(spin, 1)
        layout.addWidget(combo, 0)

        group.setLayout(layout)
        self._contract_years_filters[tab_key] = (spin, combo)
        return group

    def _add_contract_value_filter(self, tab_key: str) -> QGroupBox:
        """Create contract value threshold filter (millions)."""
        group = QGroupBox("Contract Value (M, </>)")
        layout = QHBoxLayout()
        layout.setSpacing(8)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        spin = QDoubleSpinBox()
        spin.setRange(0.0, 500.0)
        spin.setDecimals(1)
        spin.setSingleStep(1.0)
        spin.setValue(spin.maximum())

        combo = self._create_comparator_combo(spin)

        layout.addWidget(spin, 1)
        layout.addWidget(combo, 0)

        group.setLayout(layout)
        self._contract_value_filters[tab_key] = (spin, combo)
        return group

    def _add_contract_guaranteed_filter(self, tab_key: str) -> QGroupBox:
        """Create guaranteed money threshold filter (millions)."""
        group = QGroupBox("Guaranteed (M, </>)")
        layout = QHBoxLayout()
        layout.setSpacing(8)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        spin = QDoubleSpinBox()
        spin.setRange(0.0, 300.0)
        spin.setDecimals(1)
        spin.setSingleStep(1.0)
        spin.setValue(spin.maximum())

        combo = self._create_comparator_combo(spin)

        layout.addWidget(spin, 1)
        layout.addWidget(combo, 0)

        group.setLayout(layout)
        self._contract_guaranteed_filters[tab_key] = (spin, combo)
        return group

    def _add_contract_apy_filter(self, tab_key: str) -> QGroupBox:
        """Create contract APY threshold filter (millions)."""
        group = QGroupBox("APY (M, </>)")
        layout = QHBoxLayout()
        layout.setSpacing(8)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        spin = QDoubleSpinBox()
        spin.setRange(0.0, 80.0)
        spin.setDecimals(1)
        spin.setSingleStep(0.5)
        spin.setValue(spin.maximum())

        combo = self._create_comparator_combo(spin)

        layout.addWidget(spin, 1)
        layout.addWidget(combo, 0)

        group.setLayout(layout)
        self._contract_apy_filters[tab_key] = (spin, combo)
        return group

    def _add_contract_apy_cap_pct_filter(self, tab_key: str) -> QGroupBox:
        """Create contract APY cap percentage threshold filter."""
        group = QGroupBox("APY Cap Pct (</>)")
        layout = QHBoxLayout()
        layout.setSpacing(8)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        spin = QDoubleSpinBox()
        spin.setRange(0.0, 1.0)
        spin.setDecimals(3)
        spin.setSingleStep(0.01)
        spin.setValue(spin.maximum())

        combo = self._create_comparator_combo(spin)

        layout.addWidget(spin, 1)
        layout.addWidget(combo, 0)

        group.setLayout(layout)
        self._contract_apy_cap_pct_filters[tab_key] = (spin, combo)
        return group

    def _add_contract_year_signed_filter(self, tab_key: str) -> QGroupBox:
        """Create contract year signed dropdown."""
        group = QGroupBox("Year Signed")
        layout = QVBoxLayout()
        layout.setContentsMargins(4, 4, 4, 4)
        group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        combo = QComboBox()
        combo.setObjectName("FilterComboBox")
        for option in self._contract_year_signed_options:
            combo.addItem(option)
        if self._default_contract_year_signed in self._contract_year_signed_options:
            combo.setCurrentText(self._default_contract_year_signed)
        else:
            combo.setCurrentIndex(0)

        layout.addWidget(combo)
        group.setLayout(layout)
        self._contract_year_signed_filters[tab_key] = combo
        return group

    def _add_value_variant_filter(self, tab_key: str) -> QGroupBox:
        """Create value variant toggle group."""
        group = QGroupBox("Value Variant")
        layout = QHBoxLayout()
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(8)

        toggle = QPushButton("Original Value")
        toggle.setCheckable(True)
        toggle.setChecked(self._value_variant_state == "inflated")
        toggle.clicked.connect(lambda checked, key=tab_key: self._on_value_variant_toggled(key, checked))
        layout.addWidget(toggle)

        group.setLayout(layout)
        self._value_variant_toggles[tab_key] = toggle
        self._sync_value_variant_toggle(toggle)
        return group

    def _sync_value_variant_toggle(self, toggle: QPushButton) -> None:
        """Ensure toggle reflects the current value variant state."""
        inflated = self._value_variant_state == "inflated"
        text = "Inflated Value" if inflated else "Original Value"
        toggle.blockSignals(True)
        toggle.setChecked(inflated)
        toggle.setText(text)
        toggle.blockSignals(False)

    def _set_value_variant_state(self, state: str) -> None:
        """Update internal value variant state and sync toggles."""
        self._value_variant_state = state
        for button in self._value_variant_toggles.values():
            self._sync_value_variant_toggle(button)

    def _on_value_variant_toggled(self, tab_key: str, checked: bool) -> None:
        """Handle value variant toggle changes."""
        state = "inflated" if checked else "original"
        self._set_value_variant_state(state)

    def _build_results_table(self) -> None:
        """Create the center results table panel."""
        self._results_table = TablePanel(
            title="SEARCH RESULTS",
            columns=["Name", "Position", "Team", "Age", "Height", "Weight", "College"],
            sortable=True,
            alternating_rows=True,
            parent=self
        )

        # Pagination controls at top of results panel
        controls_container = QWidget(self._results_table)
        controls_layout = QHBoxLayout(controls_container)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(8)
        controls_layout.addStretch()

        self._prev_page_button = QPushButton("Previous Page", controls_container)
        self._prev_page_button.setMinimumHeight(28)
        controls_layout.addWidget(self._prev_page_button)

        self._page_display = QLabel("Page 0/0", controls_container)
        self._page_display.setStyleSheet("color: #C6CED6; padding: 0 8px;")
        controls_layout.addWidget(self._page_display)

        self._next_page_button = QPushButton("Next Page", controls_container)
        self._next_page_button.setMinimumHeight(28)
        controls_layout.addWidget(self._next_page_button)

        # Insert controls above the table within the panel
        self._results_table.content_layout.insertWidget(0, controls_container)

        # Wire up pagination buttons
        self._prev_page_button.clicked.connect(self._go_to_previous_page)
        self._next_page_button.clicked.connect(self._go_to_next_page)
        self._update_page_controls()
        
        # Position: columns 3-11 (9 columns wide), rows 0-23 (full height)
        self._grid_layout.add_widget(
            self._results_table,
            GridCell(col=3, row=0, col_span=9, row_span=24)
        )
        
        self._results_table.table.cellClicked.connect(self._on_results_row_activated)

        # Table starts empty - user must click SEARCH to populate
    
    def _perform_search(self) -> None:
        """Execute search based on current filter criteria on a worker thread."""
        criteria = self._build_search_criteria()
        if criteria is None:
            return
        if self._active_worker is not None:
            print("[Search] A search is already running; ignoring new request")
            return

        self._current_results_df = None
        self._total_pages = 0
        self._current_page = 0
        self._set_search_in_progress(True)
        self._results_table.clear_data()
        self._results_table.add_row(["Searching...", "", "", "", "", "", ""])
        self._update_page_controls()

        worker = SearchWorker(self._service, criteria)
        worker.signals.finished.connect(self._on_search_finished)
        worker.signals.error.connect(self._on_search_failed)
        self._active_worker = worker

        print(
            "[Search] "
            f"Age: {criteria.age_min}-{criteria.age_max}, Service: {criteria.service_min}-{criteria.service_max}, "
            f"Offense: {criteria.is_offense}, Position: {criteria.position_filter}, Team: {criteria.team_filter}, "
            f"Year: {criteria.year_filter}, Draft Round: {criteria.draft_round_value}, Draft Pick: {criteria.draft_position_value}, "
            f"Draft Team: {criteria.draft_team_value}, Contract Variant: {criteria.value_variant}"
        )

        self._thread_pool.start(worker)

    def _build_search_criteria(self) -> SearchCriteria | None:
        """Collect the current filter selections into a structured criteria object."""
        is_offense = self._position_tabs.currentIndex() == 0
        tab_key = "offense" if is_offense else "defense"

        age_from_combo, age_to_combo = self._age_filters.get(tab_key, (None, None))
        service_from_combo, service_to_combo = self._service_filters.get(tab_key, (None, None))
        position_combo = self._position_filters.get(tab_key)
        team_combo = self._team_filters.get(tab_key)
        year_combo = self._year_filters.get(tab_key)

        if (
            age_from_combo is None
            or age_to_combo is None
            or service_from_combo is None
            or service_to_combo is None
            or position_combo is None
            or team_combo is None
            or year_combo is None
        ):
            print("[Search] Filter controls not configured correctly")
            return None

        def _to_int(text: str) -> int:
            try:
                return int(text)
            except ValueError:
                return 0

        age_min = _to_int(age_from_combo.currentText())
        age_max = _to_int(age_to_combo.currentText())
        service_min = _to_int(service_from_combo.currentText())
        service_max = _to_int(service_to_combo.currentText())

        def _extract_threshold(mapping: dict[str, tuple[QAbstractSpinBox, QComboBox]], key: str) -> Threshold | None:
            widgets = mapping.get(key)
            if widgets is None:
                return None
            spin_widget, operator_combo = widgets
            operator = operator_combo.currentText()
            if operator == "Any":
                return None
            return Threshold(float(spin_widget.value()), operator)

        contract_years_threshold = _extract_threshold(self._contract_years_filters, tab_key)
        contract_value_threshold = _extract_threshold(self._contract_value_filters, tab_key)
        contract_guaranteed_threshold = _extract_threshold(self._contract_guaranteed_filters, tab_key)
        contract_apy_threshold = _extract_threshold(self._contract_apy_filters, tab_key)
        contract_apy_cap_pct_threshold = _extract_threshold(self._contract_apy_cap_pct_filters, tab_key)

        contract_year_signed_combo = self._contract_year_signed_filters.get(tab_key)
        contract_year_signed_value = contract_year_signed_combo.currentText() if contract_year_signed_combo else "Any"

        return SearchCriteria(
            is_offense=is_offense,
            age_min=age_min,
            age_max=age_max,
            service_min=service_min,
            service_max=service_max,
            position_filter=position_combo.currentText(),
            team_filter=team_combo.currentText(),
            year_filter=year_combo.currentText(),
            draft_round_value=self._draft_round_filters[tab_key].currentText(),
            draft_position_value=self._draft_position_filters[tab_key].currentText(),
            draft_team_value=self._draft_team_filters[tab_key].currentText(),
            contract_years=contract_years_threshold,
            contract_value=contract_value_threshold,
            contract_guaranteed=contract_guaranteed_threshold,
            contract_apy=contract_apy_threshold,
            contract_apy_cap_pct=contract_apy_cap_pct_threshold,
            contract_year_signed=contract_year_signed_value,
            value_variant=self._get_value_variant(),
        )

    def _on_search_finished(self, results_df: pl.DataFrame) -> None:
        """Handle completion of a background search."""
        self._active_worker = None
        self._current_results_df = results_df

        total_rows = results_df.height if isinstance(results_df, pl.DataFrame) else 0
        self._total_pages = math.ceil(total_rows / self._page_size) if total_rows > 0 else 0
        self._current_page = 1 if self._total_pages > 0 else 0

        print(f"[Search] Found {total_rows} players across {self._total_pages} pages")

        if self._total_pages == 0:
            self._results_table.clear_data()

        self._load_current_page()
        self._set_search_in_progress(False)

    def _on_search_failed(self, message: str) -> None:
        """Display errors raised during background search execution."""
        self._active_worker = None
        self._current_results_df = None
        self._total_pages = 0
        self._current_page = 0
        print(f"[Search] Error: {message}")

        self._results_table.clear_data()
        self._results_table.add_row(["Error loading players", message, "", "", "", "", ""])
        self._update_page_controls()
        self._set_search_in_progress(False)

    def _set_search_in_progress(self, in_progress: bool) -> None:
        """Toggle UI affordances while a search is running."""
        self._search_in_progress = in_progress
        self._search_button.setEnabled(not in_progress)
        if in_progress:
            self._prev_page_button.setEnabled(False)
            self._next_page_button.setEnabled(False)
        else:
            self._update_page_controls()

    @staticmethod
    def _filter_players_by_criteria(
        players_df: pl.DataFrame, criteria: SearchCriteria
    ) -> pl.DataFrame:
        """Apply UI search filters to the player directory frame."""
        if players_df is None:
            return pl.DataFrame()
        if players_df.height == 0:
            return players_df

        df = players_df

        if "position" in df.columns:
            offense_positions = ['QB', 'RB', 'WR', 'TE', 'FB', 'HB', 'T', 'G', 'C', 'OT', 'OG']
            defense_positions = ['DE', 'DT', 'NT', 'LB', 'ILB', 'OLB', 'MLB', 'CB', 'S', 'SS', 'FS', 'DB']
            df = df.filter(
                df['position'].is_in(offense_positions if criteria.is_offense else defense_positions)
            )

        if "age" in df.columns:
            df = df.filter(
                pl.col("age").cast(pl.Float64, strict=False).is_between(
                    criteria.age_min, criteria.age_max, closed="both"
                )
            )

        for service_column in ["years_pro", "experience", "seasons", "seasons_played"]:
            if service_column in df.columns:
                df = df.filter(
                    pl.col(service_column).cast(pl.Float64, strict=False).is_between(
                        criteria.service_min, criteria.service_max, closed="both"
                    )
                )
                break

        if criteria.position_filter != "Any" and "position" in df.columns:
            df = df.filter(
                pl.col("position").str.to_uppercase() == criteria.position_filter.upper()
            )

        if criteria.team_filter != "Any":
            team_conditions: list[pl.Expr] = []
            for column in ["team_abbr", "current_team_abbr", "recent_team", "team"]:
                if column in df.columns:
                    team_conditions.append(
                        pl.col(column).str.to_uppercase() == criteria.team_filter.upper()
                    )
            if team_conditions:
                condition = team_conditions[0]
                for expr in team_conditions[1:]:
                    condition = condition | expr
                df = df.filter(condition)

        if criteria.year_filter != "Any":
            try:
                start_year = int(criteria.year_filter.split("-")[0])
                for column in ["last_season", "season", "year"]:
                    if column in df.columns:
                        df = df.filter(
                            pl.col(column).cast(pl.Int64, strict=False) >= start_year
                        )
                        break
            except ValueError:
                pass

        def apply_threshold(
            current_df: pl.DataFrame,
            threshold: Threshold | None,
            column_name: str,
            *,
            cast_type: pl.DataType = pl.Float64,
        ) -> pl.DataFrame:
            if threshold is None or column_name not in current_df.columns:
                return current_df
            expr = pl.col(column_name).cast(cast_type, strict=False)
            if threshold.operator == "<":
                return current_df.filter(expr <= threshold.value)
            return current_df.filter(expr >= threshold.value)

        df = apply_threshold(df, criteria.contract_years, "years")

        if criteria.contract_year_signed != "Any" and "year_signed" in df.columns:
            try:
                contract_year_int = int(criteria.contract_year_signed)
                df = df.filter(
                    pl.col("year_signed").cast(pl.Int64, strict=False) == contract_year_int
                )
            except ValueError:
                pass

        value_column = (
            "inflated_value"
            if criteria.value_variant == "inflated" and "inflated_value" in df.columns
            else "value"
        )
        df = apply_threshold(df, criteria.contract_value, value_column)

        guaranteed_column = (
            "inflated_guaranteed"
            if criteria.value_variant == "inflated" and "inflated_guaranteed" in df.columns
            else "guaranteed"
        )
        df = apply_threshold(df, criteria.contract_guaranteed, guaranteed_column)

        apy_column = (
            "inflated_apy"
            if criteria.value_variant == "inflated" and "inflated_apy" in df.columns
            else "apy"
        )
        df = apply_threshold(df, criteria.contract_apy, apy_column)

        df = apply_threshold(df, criteria.contract_apy_cap_pct, "apy_cap_pct")

        draft_round_int: int | None = None
        if criteria.draft_round_value != "Any":
            if criteria.draft_round_value == "Undrafted":
                conditions: list[pl.Expr] = []
                if "draft_round" in df.columns:
                    conditions.append(pl.col("draft_round").is_null() | (pl.col("draft_round") == 0))
                if "draft_pick" in df.columns:
                    conditions.append(pl.col("draft_pick").is_null() | (pl.col("draft_pick") == 0))
                if conditions:
                    condition = conditions[0]
                    for expr in conditions[1:]:
                        condition = condition | expr
                    df = df.filter(condition)
            else:
                try:
                    draft_round_int = int(criteria.draft_round_value)
                except ValueError:
                    draft_round_int = None
                if draft_round_int is not None:
                    if "draft_round" in df.columns:
                        df = df.filter(
                            pl.col("draft_round").cast(pl.Int64, strict=False) == draft_round_int
                        )
                    elif "draft_pick" in df.columns:
                        df = df.filter(
                            (((pl.col("draft_pick").cast(pl.Int64, strict=False) - 1) // 32) + 1)
                            == draft_round_int
                        )

        if criteria.draft_position_value not in ("Any", "N/A"):
            try:
                draft_pick_in_round = int(criteria.draft_position_value)
            except ValueError:
                draft_pick_in_round = None
            if draft_pick_in_round is not None and "draft_pick" in df.columns:
                if draft_round_int is None and criteria.draft_round_value not in ("Any", "Undrafted"):
                    try:
                        draft_round_int = int(criteria.draft_round_value)
                    except ValueError:
                        draft_round_int = None
                if draft_round_int is not None:
                    df = df.filter(
                        (((pl.col("draft_pick").cast(pl.Int64, strict=False) - 1) % 32) + 1)
                        == draft_pick_in_round
                    )

        if criteria.draft_team_value != "Any":
            draft_team_conditions: list[pl.Expr] = []
            for column in ["draft_team", "draft_team_abbr", "draft_team_code"]:
                if column in df.columns:
                    draft_team_conditions.append(
                        pl.col(column).str.to_uppercase() == criteria.draft_team_value.upper()
                    )
            if not draft_team_conditions:
                for column in ["team_abbr", "current_team_abbr", "recent_team", "team"]:
                    if column in df.columns:
                        draft_team_conditions.append(
                            pl.col(column).str.to_uppercase() == criteria.draft_team_value.upper()
                        )
            if draft_team_conditions:
                condition = draft_team_conditions[0]
                for expr in draft_team_conditions[1:]:
                    condition = condition | expr
                df = df.filter(condition)

        return df

    def _load_current_page(self) -> None:
        """Load the rows for the current page into the results table."""
        self._results_table.clear_data()

        if self._current_results_df is None or self._total_pages == 0 or self._current_page == 0:
            self._update_page_controls()
            return

        start = (self._current_page - 1) * self._page_size
        page_df = self._current_results_df.slice(start, self._page_size)

        for row in page_df.iter_rows(named=True):
            name_value = (
                row.get("display_name")
                or row.get("full_name")
                or row.get("name")
                or "Unknown"
            )
            name = self._format_cell_value(name_value) or "Unknown"

            position = self._format_cell_value(row.get("position"))

            team_value = (
                row.get("team_abbr")
                or row.get("current_team_abbr")
                or row.get("recent_team")
                or row.get("team")
            )
            team = self._format_cell_value(team_value)

            age = self._format_cell_value(row.get("age"))
            height = self._format_cell_value(row.get("height"))
            weight = self._format_cell_value(row.get("weight"))
            college = self._format_cell_value(
                row.get("college") or row.get("college_name")
            )

            self._results_table.add_row([name, position, team, age, height, weight, college])

        self._update_page_controls()

    def _on_results_row_activated(self, row: int, _column: int) -> None:
        """Handle selection of a result row to show player details."""
        if self._current_results_df is None or self._current_page <= 0:
            return

        self._results_table.table.selectRow(row)

        global_index = (self._current_page - 1) * self._page_size + row
        if global_index < 0 or global_index >= self._current_results_df.height:
            return

        row_data = self._current_results_df.row(global_index, named=True)
        if hasattr(row_data, "as_dict"):
            record = row_data.as_dict()  # polars Row
        elif isinstance(row_data, dict):
            record = row_data
        else:
            record = dict(zip(self._current_results_df.columns, row_data))

        player_info: dict[str, Any] = {
            "full_name": record.get("display_name")
            or record.get("full_name")
            or record.get("name")
            or "Unknown Player",
            "position": record.get("position"),
            "team": record.get("team_abbr")
            or record.get("current_team_abbr")
            or record.get("recent_team")
            or record.get("team"),
            "age": record.get("age"),
            "college": record.get("college") or record.get("college_name"),
            "gsis_id": record.get("gsis_id"),
            "nfl_id": record.get("nfl_id"),
            "pfr_id": record.get("pfr_id"),
            "raw": record,
        }

        self.playerSelected.emit(player_info)

    def _update_page_controls(self) -> None:
        """Update pagination controls based on current page and totals."""
        if self._total_pages:
            self._page_display.setText(f"Page {self._current_page}/{self._total_pages}")
        else:
            self._page_display.setText("Page 0/0")

        if self._search_in_progress:
            self._prev_page_button.setEnabled(False)
            self._next_page_button.setEnabled(False)
        else:
            self._prev_page_button.setEnabled(self._current_page > 1)
            self._next_page_button.setEnabled(self._current_page < self._total_pages)

    def _go_to_next_page(self) -> None:
        """Navigate to the next page of results."""
        if self._current_page < self._total_pages:
            self._current_page += 1
            self._load_current_page()

    def _go_to_previous_page(self) -> None:
        """Navigate to the previous page of results."""
        if self._current_page > 1:
            self._current_page -= 1
            self._load_current_page()

    def _on_draft_round_changed(self, tab_key: str, value: str) -> None:
        """Adjust draft position controls based on round selection."""
        position_combo = self._draft_position_filters.get(tab_key)
        if position_combo is None:
            return

        if value.lower() == "undrafted":
            position_combo.setCurrentText("N/A")
            position_combo.setEnabled(False)
        else:
            if position_combo.currentText() == "N/A":
                position_combo.setCurrentIndex(0)
            position_combo.setEnabled(True)

    def _get_value_variant(self) -> str:
        """Return selected contract value variant."""
        return self._value_variant_state
    
    def _clear_table(self) -> None:
        """Clear all results from the table."""
        self._results_table.clear_data()
        self._current_results_df = None
        self._current_page = 0
        self._total_pages = 0
        self._update_page_controls()
        print("[Search] Table cleared")
    
    def _reset_filters(self) -> None:
        """Reset all filters to default values."""
        for age_from, age_to in self._age_filters.values():
            age_from.setCurrentText("20")
            age_to.setCurrentText("45")
        for service_from, service_to in self._service_filters.values():
            service_from.setCurrentText("0")
            service_to.setCurrentText("25")
        for combo in self._position_filters.values():
            combo.setCurrentIndex(0)
        for combo in self._team_filters.values():
            combo.setCurrentIndex(0)
        for combo in self._year_filters.values():
            if self._default_year in self._year_options:
                combo.setCurrentText(self._default_year)
            else:
                combo.setCurrentIndex(0)
        for spin, combo in self._contract_years_filters.values():
            combo.setCurrentIndex(0)
            spin.setValue(spin.maximum())
        for spin, combo in self._contract_value_filters.values():
            combo.setCurrentIndex(0)
            spin.setValue(spin.maximum())
        for spin, combo in self._contract_guaranteed_filters.values():
            combo.setCurrentIndex(0)
            spin.setValue(spin.maximum())
        for spin, combo in self._contract_apy_filters.values():
            combo.setCurrentIndex(0)
            spin.setValue(spin.maximum())
        for spin, combo in self._contract_apy_cap_pct_filters.values():
            combo.setCurrentIndex(0)
            spin.setValue(spin.maximum())
        for combo in self._contract_year_signed_filters.values():
            if self._default_contract_year_signed in self._contract_year_signed_options:
                combo.setCurrentText(self._default_contract_year_signed)
            else:
                combo.setCurrentIndex(0)
        for tab_key, combo in self._draft_round_filters.items():
            combo.setCurrentIndex(0)
            self._on_draft_round_changed(tab_key, combo.currentText())
        for combo in self._draft_position_filters.values():
            combo.setCurrentIndex(0)
            combo.setEnabled(True)
        for combo in self._draft_team_filters.values():
            combo.setCurrentIndex(0)
        self._set_value_variant_state("original")

        print("[Search] Filters reset to defaults")
        self._clear_table()

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        """Handle resize events to update grid layout."""
        super().resizeEvent(event)
        if hasattr(self, "_grid_layout"):
            self._grid_layout.update_layout()
