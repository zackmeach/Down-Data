"""Player search landing page."""

from __future__ import annotations

from typing import Optional

import math
from datetime import datetime

import polars as pl

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
from down_data.ui.widgets import GridCell, GridLayoutManager, FilterPanel, TablePanel

from .base_page import SectionPage


class PlayerSearchPage(SectionPage):
    """Find Player page - search and filter players.
    
    This page is displayed when navigating to Players > Find A Player.
    Navigation bars are handled by the parent ContentPage.
    
    Layout:
    - Filter panel: rows 0-23, columns 0-2 (left sidebar, 3 columns wide)
    - Results table: rows 0-23, columns 3-11 (center/right area, 9 columns wide)
    - Detail panel: TBD (optional right sidebar for preview)
    """

    def __init__(
        self, 
        *, 
        service: PlayerService, 
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(title="Player Search", parent=parent)
        self._service = service
        self._page_size = 100
        self._current_page = 0
        self._total_pages = 0
        self._current_results_df = None  # polars DataFrame storing filtered results
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
        # Draft-related options
        self._draft_round_options = ["Any"] + [str(i) for i in range(1, 8)] + ["Undrafted"]
        self._draft_position_options = ["Any"] + [str(i) for i in range(1, 33)] + ["N/A"]
        self._draft_team_options = self._team_options.copy()
    
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
        age_layout = QHBoxLayout()
        age_layout.setSpacing(8)
        age_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        
        # From dropdown
        age_from = QComboBox()
        age_from.setObjectName("FilterComboBox")
        # Typical NFL age range: 20-45
        for age in range(20, 46):
            age_from.addItem(str(age))
        age_from.setCurrentText("20")
        
        to_label = QLabel("to")
        to_label.setStyleSheet("color: #C6CED6; padding: 0 4px;")
        
        # To dropdown
        age_to = QComboBox()
        age_to.setObjectName("FilterComboBox")
        for age in range(20, 46):
            age_to.addItem(str(age))
        age_to.setCurrentText("45")
        
        age_layout.addWidget(age_from, 1)
        age_layout.addWidget(to_label, 0)
        age_layout.addWidget(age_to, 1)
        
        age_group.setLayout(age_layout)
        # Store references for later access keyed by tab
        self._age_filters[tab_key] = (age_from, age_to)
        return age_group
    
    def _add_service_time_filter(self, tab_key: str) -> QGroupBox:
        """Create service time (years in league) range filter group."""
        service_group = QGroupBox("Service Time (Yrs)")
        service_layout = QHBoxLayout()
        service_layout.setSpacing(8)
        service_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        
        # From dropdown
        service_from = QComboBox()
        service_from.setObjectName("FilterComboBox")
        # Typical NFL career: 0-25 years
        for years in range(0, 26):
            service_from.addItem(str(years))
        service_from.setCurrentText("0")
        
        to_label = QLabel("to")
        to_label.setStyleSheet("color: #C6CED6; padding: 0 4px;")
        
        # To dropdown
        service_to = QComboBox()
        service_to.setObjectName("FilterComboBox")
        for years in range(0, 26):
            service_to.addItem(str(years))
        service_to.setCurrentText("25")
        
        service_layout.addWidget(service_from, 1)
        service_layout.addWidget(to_label, 0)
        service_layout.addWidget(service_to, 1)
        
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
        
        # Table starts empty - user must click SEARCH to populate
    
    def _perform_search(self) -> None:
        """Execute search based on current filter criteria."""
        # Determine if searching offense or defense
        is_offense = self._position_tabs.currentIndex() == 0
        tab_key = "offense" if is_offense else "defense"

        # Get filter values for the active tab
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
            return

        age_min = int(age_from_combo.currentText())
        age_max = int(age_to_combo.currentText())
        service_min = int(service_from_combo.currentText())
        service_max = int(service_to_combo.currentText())
        position_filter = position_combo.currentText()
        team_filter = team_combo.currentText()
        year_filter = year_combo.currentText()
        draft_round_value = self._draft_round_filters[tab_key].currentText()
        draft_position_value = self._draft_position_filters[tab_key].currentText()
        draft_team_value = self._draft_team_filters[tab_key].currentText()
        contract_years_filter = self._contract_years_filters.get(tab_key)
        contract_value_filter = self._contract_value_filters.get(tab_key)
        contract_guaranteed_filter = self._contract_guaranteed_filters.get(tab_key)
        contract_apy_filter = self._contract_apy_filters.get(tab_key)
        contract_apy_cap_pct_filter = self._contract_apy_cap_pct_filters.get(tab_key)
        contract_year_signed_combo = self._contract_year_signed_filters.get(tab_key)
        value_variant = self._get_value_variant()
        
        print(
            "[Search] "
            f"Age: {age_min}-{age_max}, Service: {service_min}-{service_max}, "
            f"Offense: {is_offense}, Position: {position_filter}, Team: {team_filter}, Year: {year_filter}, "
            f"Draft Round: {draft_round_value}, Draft Pick: {draft_position_value}, Draft Team: {draft_team_value}, "
            f"Contract Variant: {value_variant}"
        )
        
        try:
            # Get all players from service
            players_df = self._service.get_all_players()
            
            if players_df is None or players_df.height == 0:
                self._results_table.clear_data()
                return
            
            # Filter by position group (offense vs defense)
            if is_offense:
                # Offense positions: QB, RB, WR, TE, FB, OL positions
                offense_positions = ['QB', 'RB', 'WR', 'TE', 'FB', 'HB', 'T', 'G', 'C', 'OT', 'OG']
                players_df = players_df.filter(
                    players_df['position'].is_in(offense_positions)
                )
            else:
                # Defense positions: DL, LB, DB positions
                defense_positions = ['DE', 'DT', 'NT', 'LB', 'ILB', 'OLB', 'MLB', 'CB', 'S', 'SS', 'FS', 'DB']
                players_df = players_df.filter(
                    players_df['position'].is_in(defense_positions)
                )
            
            # Filter by age if column available
            if "age" in players_df.columns:
                players_df = players_df.filter(
                    pl.col("age").cast(pl.Float64, strict=False).is_between(age_min, age_max, closed="both")
                )
            
            # Filter by service time if applicable column exists
            for service_column in ["years_pro", "experience", "seasons", "seasons_played"]:
                if service_column in players_df.columns:
                    players_df = players_df.filter(
                        pl.col(service_column).cast(pl.Float64, strict=False).is_between(
                            service_min, service_max, closed="both"
                        )
                    )
                    break

            # Filter by specific player position if not Any
            if position_filter != "Any" and "position" in players_df.columns:
                players_df = players_df.filter(
                    pl.col("position").str.to_uppercase() == position_filter.upper()
                )

            # Filter by team if not Any
            if team_filter != "Any":
                team_conditions: list[pl.Expr] = []
                for column in ["team_abbr", "current_team_abbr", "recent_team", "team"]:
                    if column in players_df.columns:
                        team_conditions.append(
                            pl.col(column).str.to_uppercase() == team_filter.upper()
                        )
                if team_conditions:
                    condition = team_conditions[0]
                    for expr in team_conditions[1:]:
                        condition = condition | expr
                    players_df = players_df.filter(condition)

            # Filter by year if not Any (use last_season/year columns when available)
            if year_filter != "Any":
                try:
                    start_year = int(year_filter.split("-")[0])
                    for column in ["last_season", "season", "year"]:
                        if column in players_df.columns:
                            players_df = players_df.filter(
                                pl.col(column).cast(pl.Int64, strict=False) >= start_year
                            )
                            break
                except ValueError:
                    pass

            def apply_threshold(
                df: pl.DataFrame,
                widgets: Optional[tuple[QAbstractSpinBox, QComboBox]],
                column_name: str,
                *,
                cast_type: pl.DataType = pl.Float64,
            ) -> pl.DataFrame:
                if widgets is None or column_name not in df.columns:
                    return df
                spin_widget, operator_combo = widgets
                operator = operator_combo.currentText()
                if operator == "Any":
                    return df
                value = float(spin_widget.value())
                expr = pl.col(column_name).cast(cast_type, strict=False)
                if operator == "<":
                    return df.filter(expr <= value)
                return df.filter(expr >= value)

            players_df = apply_threshold(players_df, contract_years_filter, "years")

            # Contract year signed
            if contract_year_signed_combo is not None and "year_signed" in players_df.columns:
                contract_year_signed_value = contract_year_signed_combo.currentText()
                if contract_year_signed_value != "Any":
                    try:
                        contract_year_int = int(contract_year_signed_value)
                        players_df = players_df.filter(
                            pl.col("year_signed").cast(pl.Int64, strict=False) == contract_year_int
                        )
                    except ValueError:
                        pass

            value_column = (
                "inflated_value"
                if value_variant == "inflated" and "inflated_value" in players_df.columns
                else "value"
            )
            players_df = apply_threshold(players_df, contract_value_filter, value_column)

            guaranteed_column = (
                "inflated_guaranteed"
                if value_variant == "inflated" and "inflated_guaranteed" in players_df.columns
                else "guaranteed"
            )
            players_df = apply_threshold(players_df, contract_guaranteed_filter, guaranteed_column)

            apy_column = (
                "inflated_apy"
                if value_variant == "inflated" and "inflated_apy" in players_df.columns
                else "apy"
            )
            players_df = apply_threshold(players_df, contract_apy_filter, apy_column)

            players_df = apply_threshold(players_df, contract_apy_cap_pct_filter, "apy_cap_pct")

            # Filter by draft round
            draft_round_int: Optional[int] = None
            if draft_round_value != "Any":
                if draft_round_value == "Undrafted":
                    conditions: list[pl.Expr] = []
                    if "draft_round" in players_df.columns:
                        conditions.append(pl.col("draft_round").is_null() | (pl.col("draft_round") == 0))
                    if "draft_pick" in players_df.columns:
                        conditions.append(pl.col("draft_pick").is_null() | (pl.col("draft_pick") == 0))
                    if conditions:
                        condition = conditions[0]
                        for expr in conditions[1:]:
                            condition = condition | expr
                        players_df = players_df.filter(condition)
                else:
                    try:
                        draft_round_int = int(draft_round_value)
                    except ValueError:
                        draft_round_int = None
                    if draft_round_int is not None:
                        if "draft_round" in players_df.columns:
                            players_df = players_df.filter(
                                pl.col("draft_round").cast(pl.Int64, strict=False) == draft_round_int
                            )
                        elif "draft_pick" in players_df.columns:
                            players_df = players_df.filter(
                                (((pl.col("draft_pick").cast(pl.Int64, strict=False) - 1) // 32) + 1)
                                == draft_round_int
                            )

            # Filter by draft pick within round
            if draft_position_value not in ("Any", "N/A"):
                try:
                    draft_pick_in_round = int(draft_position_value)
                except ValueError:
                    draft_pick_in_round = None
                if draft_pick_in_round is not None and "draft_pick" in players_df.columns:
                    if draft_round_int is None and draft_round_value not in ("Any", "Undrafted"):
                        try:
                            draft_round_int = int(draft_round_value)
                        except ValueError:
                            draft_round_int = None
                    if draft_round_int is not None:
                        players_df = players_df.filter(
                            (((pl.col("draft_pick").cast(pl.Int64, strict=False) - 1) % 32) + 1)
                            == draft_pick_in_round
                        )

            # Filter by draft team
            if draft_team_value != "Any":
                draft_team_conditions: list[pl.Expr] = []
                for column in ["draft_team", "draft_team_abbr", "draft_team_code"]:
                    if column in players_df.columns:
                        draft_team_conditions.append(
                            pl.col(column).str.to_uppercase() == draft_team_value.upper()
                        )
                if not draft_team_conditions:
                    for column in ["team_abbr", "current_team_abbr", "recent_team", "team"]:
                        if column in players_df.columns:
                            draft_team_conditions.append(
                                pl.col(column).str.to_uppercase() == draft_team_value.upper()
                            )
                if draft_team_conditions:
                    condition = draft_team_conditions[0]
                    for expr in draft_team_conditions[1:]:
                        condition = condition | expr
                    players_df = players_df.filter(condition)
            
            # Store filtered results for pagination
            self._current_results_df = players_df
            total_rows = players_df.height
            self._total_pages = math.ceil(total_rows / self._page_size) if total_rows > 0 else 0
            self._current_page = 1 if self._total_pages > 0 else 0

            print(f"[Search] Found {total_rows} players across {self._total_pages} pages")

            # Load first page
            self._load_current_page()
            
        except Exception as e:
            print(f"[Search] Error: {e}")
            # Show error in results table
            self._results_table.clear_data()
            self._results_table.add_row(["Error loading players", str(e), "", "", "", "", ""])
            self._total_pages = 0
            self._current_page = 0
            self._update_page_controls()
        self._load_current_page()

    def _load_current_page(self) -> None:
        """Load the rows for the current page into the results table."""
        self._results_table.clear_data()

        if self._current_results_df is None or self._total_pages == 0 or self._current_page == 0:
            self._update_page_controls()
            return

        start = (self._current_page - 1) * self._page_size
        page_df = self._current_results_df.slice(start, self._page_size)

        for row in page_df.iter_rows(named=True):
            name = row.get('display_name') or row.get('full_name') or 'Unknown'
            position = row.get('position') or ''
            team = row.get('team_abbr') or row.get('current_team_abbr') or ''
            age = str(row.get('age', ''))
            height = row.get('height') or ''
            weight = str(row.get('weight', '')) if row.get('weight') else ''
            college = row.get('college') or ''
            self._results_table.add_row([name, position, team, age, height, weight, college])

        self._update_page_controls()

    def _update_page_controls(self) -> None:
        """Update pagination controls based on current page and totals."""
        if self._total_pages:
            self._page_display.setText(f"Page {self._current_page}/{self._total_pages}")
        else:
            self._page_display.setText("Page 0/0")

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
