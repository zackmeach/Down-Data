"""Data exploration proof-of-concept landing page."""

from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import partial
from typing import Any, Dict, Iterable, List, Optional

from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QSortFilterProxyModel,
    QSize,
    Qt,
    QTimer,
)
from PySide6.QtGui import QColor, QIcon, QKeySequence, QPixmap
from PySide6.QtWidgets import (
    QAction,
    QApplication,
    QCompleter,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QStackedLayout,
    QStackedWidget,
    QStyle,
    QTabWidget,
    QTableView,
    QToolBar,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from .base import BasePage


@dataclass
class Entity:
    identifier: str
    name: str
    entity_type: str
    description: str
    thumbnail_color: Optional[str]


@dataclass
class DatasetColumn:
    name: str
    dtype: str  # text, number, date


@dataclass
class DatasetDefinition:
    identifier: str
    title: str
    description: str
    row_estimate: int
    last_updated: str
    source: str
    coverage: str
    mode: str  # virtualized or paginated
    columns: List[DatasetColumn]
    rows: List[Dict[str, Any]]
    page_size: int = 50
    simulate_error: bool = False


ENTITY_TYPES = ["Player", "Team", "Season", "Coach", "Game"]


def _build_entities() -> List[Entity]:
    roster = [
        Entity("player_mahoms", "Patrick Mahomes", "Player", "QB • Kansas City Chiefs", "#ff6f61"),
        Entity("player_kelce", "Travis Kelce", "Player", "TE • Kansas City Chiefs", "#5b8def"),
        Entity("team_kc", "Kansas City Chiefs", "Team", "AFC West", "#e31837"),
        Entity("team_det", "Detroit Lions", "Team", "NFC North", "#0076b6"),
        Entity("season_2023", "NFL Season 2023", "Season", "Regular & Postseason", None),
        Entity("coach_reid", "Andy Reid", "Coach", "Head Coach • Kansas City Chiefs", "#f4c542"),
        Entity("game_sb58", "Super Bowl LVIII", "Game", "Chiefs vs. 49ers", None),
        Entity("player_stbrown", "Amon-Ra St. Brown", "Player", "WR • Detroit Lions", "#1c4fa1"),
        Entity("player_gibbs", "Jahmyr Gibbs", "Player", "RB • Detroit Lions", "#00a6d2"),
        Entity("team_sfo", "San Francisco 49ers", "Team", "NFC West", "#aa0000"),
    ]

    # Add synthetic players to demonstrate infinite scrolling / virtualization.
    start_year = 2000
    for idx in range(40):
        name = f"Prospect {idx + 1:02d}"
        roster.append(
            Entity(
                f"player_prospect_{idx}",
                name,
                "Player",
                f"Draft class {start_year + idx % 20}",
                None,
            )
        )

    return roster


def _random_row(base_date: date, opponent_pool: Iterable[str]) -> Dict[str, Any]:
    opponent = random.choice(tuple(opponent_pool))
    result = random.choice(["W", "L"])
    diff = random.randint(1, 24)
    score = f"{random.randint(14, 45)}-{random.randint(10, 38)}"
    pass_yards = random.randint(120, 420)
    rush_yards = random.randint(-5, 120)
    pass_td = random.randint(0, 6)

    return {
        "Game Date": base_date.isoformat(),
        "Opponent": opponent,
        "Result": f"{result} ({diff})",
        "Score": score,
        "Pass Yds": pass_yards,
        "Rush Yds": rush_yards,
        "Pass TD": pass_td,
    }


def _build_rows(num: int) -> List[Dict[str, Any]]:
    opponents = ["BUF", "LAC", "DEN", "LV", "PHI", "BAL", "MIA", "CIN"]
    base_day = date.today() - timedelta(days=num)
    return [_random_row(base_day + timedelta(days=i * 7), opponents) for i in range(num)]


def _build_dataset_definitions() -> Dict[str, List[DatasetDefinition]]:
    common_columns = [
        DatasetColumn("Game Date", "date"),
        DatasetColumn("Opponent", "text"),
        DatasetColumn("Result", "text"),
        DatasetColumn("Score", "text"),
        DatasetColumn("Pass Yds", "number"),
        DatasetColumn("Rush Yds", "number"),
        DatasetColumn("Pass TD", "number"),
    ]

    advanced_columns = [
        DatasetColumn("Season", "text"),
        DatasetColumn("EPA/play", "number"),
        DatasetColumn("Success %", "number"),
        DatasetColumn("CPOE", "number"),
        DatasetColumn("Air Yards", "number"),
    ]

    salary_columns = [
        DatasetColumn("Year", "number"),
        DatasetColumn("Cap Hit", "number"),
        DatasetColumn("Guaranteed", "number"),
        DatasetColumn("Contract Type", "text"),
    ]

    datasets = {
        "player_mahoms": [
            DatasetDefinition(
                identifier="mahomes_game_logs",
                title="Game Logs",
                description="Every regular and postseason appearance with key passing and rushing production.",
                row_estimate=280,
                last_updated="2024-02-12",
                source="Down Data tracking",
                coverage="2017-2023 Regular & Postseason",
                mode="virtualized",
                columns=common_columns,
                rows=_build_rows(180),
            ),
            DatasetDefinition(
                identifier="mahomes_splits",
                title="Splits",
                description="Performance by opponent, situation, and personnel grouping.",
                row_estimate=96,
                last_updated="2024-02-01",
                source="Down Data derived",
                coverage="2018-2023 Regular Season",
                mode="paginated",
                columns=advanced_columns,
                rows=[
                    {
                        "Season": str(2018 + i),
                        "EPA/play": round(random.uniform(0.05, 0.24), 3),
                        "Success %": round(random.uniform(45, 60), 1),
                        "CPOE": round(random.uniform(-1.5, 8.0), 2),
                        "Air Yards": random.randint(2600, 3900),
                    }
                    for i in range(6)
                ],
                page_size=3,
            ),
            DatasetDefinition(
                identifier="mahomes_contracts",
                title="Contracts",
                description="Contract history with cap details and guarantee structures.",
                row_estimate=12,
                last_updated="2024-02-10",
                source="Spotrac snapshot",
                coverage="2017-2031",
                mode="paginated",
                columns=salary_columns,
                rows=[
                    {
                        "Year": 2020 + i,
                        "Cap Hit": 5000000 + i * 12000000,
                        "Guaranteed": 2000000 + i * 10000000,
                        "Contract Type": "Extension" if i else "Rookie",
                    }
                    for i in range(8)
                ],
                page_size=4,
            ),
        ],
        "player_kelce": [
            DatasetDefinition(
                identifier="kelce_logs",
                title="Game Logs",
                description="Targets, receptions, yards, and touchdowns each week.",
                row_estimate=200,
                last_updated="2024-02-05",
                source="Next Gen Stats",
                coverage="2013-2023",
                mode="virtualized",
                columns=common_columns,
                rows=_build_rows(160),
            ),
            DatasetDefinition(
                identifier="kelce_metrics",
                title="Advanced Metrics",
                description="Route efficiency, separation, and YAC models.",
                row_estimate=80,
                last_updated="2024-02-08",
                source="Down Data machine learning",
                coverage="2014-2023",
                mode="virtualized",
                columns=advanced_columns,
                rows=[
                    {
                        "Season": str(2014 + i),
                        "EPA/play": round(random.uniform(0.02, 0.19), 3),
                        "Success %": round(random.uniform(43, 68), 1),
                        "CPOE": round(random.uniform(-4.0, 6.2), 2),
                        "Air Yards": random.randint(600, 1700),
                    }
                    for i in range(9)
                ],
            ),
        ],
        "team_kc": [
            DatasetDefinition(
                identifier="kc_team_trends",
                title="Team Trends",
                description="Weekly drive efficiency and opponent-adjusted ratings.",
                row_estimate=320,
                last_updated="2024-02-12",
                source="Down Data analytics",
                coverage="2013-2023",
                mode="virtualized",
                columns=common_columns,
                rows=_build_rows(120),
            ),
            DatasetDefinition(
                identifier="kc_financials",
                title="Contracts",
                description="Active contracts, lengths, and total cap allocations.",
                row_estimate=90,
                last_updated="2024-02-02",
                source="Spotrac aggregation",
                coverage="2024",
                mode="paginated",
                columns=salary_columns,
                rows=[
                    {
                        "Year": 2024,
                        "Cap Hit": 1200000 + i * 800000,
                        "Guaranteed": 400000 + i * 300000,
                        "Contract Type": random.choice(["Veteran", "Rookie", "Extension"]),
                    }
                    for i in range(18)
                ],
                page_size=6,
            ),
        ],
        "team_det": [
            DatasetDefinition(
                identifier="det_scouting",
                title="Advanced Scouting",
                description="Tendency reports and situational breakdowns for the Lions offense.",
                row_estimate=40,
                last_updated="2024-02-15",
                source="Down Data scouting",
                coverage="2023",
                mode="virtualized",
                columns=advanced_columns,
                rows=[
                    {
                        "Season": "2023",
                        "EPA/play": 0.134,
                        "Success %": 52.8,
                        "CPOE": 3.1,
                        "Air Yards": 3021,
                    }
                ],
                simulate_error=True,
            )
        ],
    }

    default_datasets = [
        DatasetDefinition(
            identifier="generic_logs",
            title="Game Logs",
            description="Standardized appearance-level data when detailed tracking is unavailable.",
            row_estimate=64,
            last_updated="2024-01-28",
            source="League base feeds",
            coverage="2019-2023",
            mode="virtualized",
            columns=common_columns,
            rows=_build_rows(64),
        )
    ]

    return datasets, default_datasets


class DatasetTableModel(QAbstractTableModel):
    """Lightweight table model for dataset rows."""

    def __init__(self, columns: List[DatasetColumn], parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._columns = columns
        self._rows: List[Dict[str, Any]] = []

    def set_columns(self, columns: List[DatasetColumn]) -> None:
        self.beginResetModel()
        self._columns = columns
        self._rows = []
        self.endResetModel()

    def set_rows(self, rows: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = rows
        self.endResetModel()

    # Qt Model implementation -------------------------------------------------
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # type: ignore[override]
        if parent.isValid():
            return 0
        return len(self._columns)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # type: ignore[override]
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        column = self._columns[index.column()]
        value = self._rows[index.row()].get(column.name)

        if role == Qt.DisplayRole:
            if value is None:
                return ""
            if column.dtype == "number" and isinstance(value, (int, float)):
                return f"{value:,}" if isinstance(value, int) else f"{value:,.2f}"
            if column.dtype == "date" and isinstance(value, str):
                try:
                    parsed = datetime.fromisoformat(value)
                    return parsed.strftime("%b %d, %Y")
                except ValueError:
                    return value
            return value

        if role == Qt.TextAlignmentRole:
            if column.dtype == "number":
                return Qt.AlignRight | Qt.AlignVCenter
            return Qt.AlignLeft | Qt.AlignVCenter

        return None

    def headerData(  # type: ignore[override]
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
    ) -> Any:
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self._columns):
            return self._columns[section].name
        if orientation == Qt.Vertical:
            return section + 1
        return None

    def column_type(self, column_index: int) -> str:
        if 0 <= column_index < len(self._columns):
            return self._columns[column_index].dtype
        return "text"

    def column_names(self) -> List[str]:
        return [column.name for column in self._columns]


class DatasetFilterProxyModel(QSortFilterProxyModel):
    """Proxy model that supports multi-column filtering for heterogeneous data."""

    def __init__(self, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._filters: Dict[int, Dict[str, Any]] = {}

    def set_filter(self, column: int, operation: str, value: Any, dtype: str) -> None:
        if value in (None, ""):
            self._filters.pop(column, None)
        else:
            self._filters[column] = {"operation": operation, "value": value, "dtype": dtype}
        self.invalidateFilter()

    def clear_filters(self) -> None:
        self._filters.clear()
        self.invalidateFilter()

    # QSortFilterProxyModel overrides -----------------------------------------
    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:  # type: ignore[override]
        if not self._filters:
            return True

        model = self.sourceModel()
        assert isinstance(model, DatasetTableModel)

        for column, payload in self._filters.items():
            index = model.index(source_row, column, source_parent)
            data = model.data(index, Qt.DisplayRole)
            if not self._matches(data, payload):
                return False
        return True

    def _matches(self, value: Any, payload: Dict[str, Any]) -> bool:
        dtype = payload["dtype"]
        op = payload["operation"]
        target = payload["value"]

        if dtype == "number":
            try:
                numeric_value = float(str(value).replace(",", ""))
                numeric_target = float(target)
            except ValueError:
                return False

            if op == "=":
                return math.isclose(numeric_value, numeric_target, rel_tol=1e-4)
            if op == ">":
                return numeric_value > numeric_target
            if op == ">=":
                return numeric_value >= numeric_target
            if op == "<":
                return numeric_value < numeric_target
            if op == "<=":
                return numeric_value <= numeric_target
            return False

        if dtype == "date":
            try:
                value_date = datetime.strptime(str(value), "%b %d, %Y")
            except ValueError:
                try:
                    value_date = datetime.fromisoformat(str(value))
                except ValueError:
                    return False

            try:
                target_date = datetime.fromisoformat(target)
            except ValueError:
                try:
                    target_date = datetime.strptime(target, "%Y-%m-%d")
                except ValueError:
                    return False

            if op == "On":
                return value_date.date() == target_date.date()
            if op == "Before":
                return value_date.date() < target_date.date()
            if op == "After":
                return value_date.date() > target_date.date()
            return False

        # default text comparisons
        haystack = str(value).lower()
        needle = str(target).lower()

        if op == "Contains":
            return needle in haystack
        if op == "Equals":
            return haystack == needle
        if op == "Starts with":
            return haystack.startswith(needle)
        return False


class SearchResultItemWidget(QWidget):
    """Widget used inside the search results list."""

    def __init__(self, entity: Entity, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._entity = entity
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(10)

        if entity.thumbnail_color:
            swatch = QLabel()
            swatch.setFixedSize(32, 32)
            pixmap = QPixmap(32, 32)
            pixmap.fill(QColor(entity.thumbnail_color))
            swatch.setPixmap(pixmap)
            swatch.setObjectName("entityThumbnail")
            layout.addWidget(swatch)

        text_container = QVBoxLayout()
        text_container.setContentsMargins(0, 0, 0, 0)
        text_container.setSpacing(2)

        name_label = QLabel(entity.name)
        name_label.setObjectName("entityName")
        text_container.addWidget(name_label)

        badge_row = QHBoxLayout()
        badge_row.setContentsMargins(0, 0, 0, 0)
        badge_row.setSpacing(6)

        type_badge = QLabel(entity.entity_type.upper())
        type_badge.setObjectName("typeBadge")
        badge_row.addWidget(type_badge)

        descriptor = QLabel(entity.description)
        descriptor.setObjectName("entityDescriptor")
        descriptor.setWordWrap(True)
        badge_row.addWidget(descriptor, 1)

        text_container.addLayout(badge_row)
        layout.addLayout(text_container, 1)


class DatasetListItemWidget(QWidget):
    """Widget representing a dataset option for the selected entity."""

    def __init__(self, dataset: DatasetDefinition, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._dataset = dataset
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(4)

        title_row = QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(6)

        title = QLabel(dataset.title)
        title.setObjectName("datasetTitle")
        title_row.addWidget(title, 1)

        rows_label = QLabel(f"~{dataset.row_estimate:,} rows")
        rows_label.setObjectName("datasetRows")
        title_row.addWidget(rows_label)

        layout.addLayout(title_row)

        description = QLabel(dataset.description)
        description.setWordWrap(True)
        description.setObjectName("datasetDescription")
        layout.addWidget(description)

        meta_row = QLabel(f"Updated {dataset.last_updated}")
        meta_row.setObjectName("datasetMeta")
        layout.addWidget(meta_row)


class FilterBar(QFrame):
    """Interactive filter bar for the dataset table."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setObjectName("filterBar")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(8)

        self.column_combo = QComboBox()
        self.operation_combo = QComboBox()
        self.value_edit = QLineEdit()
        self.value_edit.setPlaceholderText("Value")

        self.apply_button = QPushButton("Apply")
        self.clear_button = QPushButton("Clear filters")

        layout.addWidget(QLabel("Filter column:"))
        layout.addWidget(self.column_combo)
        layout.addWidget(self.operation_combo)
        layout.addWidget(self.value_edit, 1)
        layout.addWidget(self.apply_button)
        layout.addWidget(self.clear_button)

    def configure_for_columns(self, columns: List[DatasetColumn]) -> None:
        self.column_combo.clear()
        for idx, column in enumerate(columns):
            self.column_combo.addItem(column.name, (idx, column.dtype))
        self._update_operation_combo(columns[0].dtype if columns else "text")

    def _update_operation_combo(self, dtype: str) -> None:
        self.operation_combo.clear()
        if dtype == "number":
            for op in [">", ">=", "<", "<=", "="]:
                self.operation_combo.addItem(op)
            self.value_edit.setPlaceholderText("Numeric value")
        elif dtype == "date":
            for op in ["On", "Before", "After"]:
                self.operation_combo.addItem(op)
            self.value_edit.setPlaceholderText("YYYY-MM-DD")
        else:
            for op in ["Contains", "Equals", "Starts with"]:
                self.operation_combo.addItem(op)
            self.value_edit.setPlaceholderText("Text contains…")


class HomePage(BasePage):
    """Single-screen exploration prototype following a strict workflow."""

    title = "Down Data Explorer"
    description = None

    def __init__(self, *, parent: Optional[QWidget] = None) -> None:
        self._entities = _build_entities()
        self._entity_datasets, self._fallback_datasets = _build_dataset_definitions()
        self._search_results: List[Entity] = []
        self._search_page = 0
        self._page_size = 12
        self._current_entity: Optional[Entity] = None
        self._current_dataset: Optional[DatasetDefinition] = None
        self._pending_dataset: Optional[DatasetDefinition] = None
        self._is_compact_mode: Optional[bool] = None
        self._updating_selection = False
        self._pagination_enabled = False
        self._pagination_page = 0

        super().__init__(parent=parent)

    # Layout construction -----------------------------------------------------
    def _init_layout(self) -> None:  # type: ignore[override]
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(16, 16, 16, 16)
        root_layout.setSpacing(12)

        self._build_search_bar(root_layout)

        self.responsive_container = QWidget()
        self.responsive_layout = QStackedLayout(self.responsive_container)
        root_layout.addWidget(self.responsive_container, 1)

        self._build_sidebar_panels()
        self._build_main_canvas()

        self.desktop_container = QWidget()
        desktop_layout = QVBoxLayout(self.desktop_container)
        desktop_layout.setContentsMargins(0, 0, 0, 0)
        desktop_layout.setSpacing(0)

        self.main_splitter = QSplitter(Qt.Horizontal)
        self.main_splitter.setChildrenCollapsible(False)
        self.main_splitter.addWidget(self.sidebar_splitter)
        self.main_splitter.addWidget(self.main_canvas)
        self.main_splitter.setStretchFactor(0, 1)
        self.main_splitter.setStretchFactor(1, 3)
        desktop_layout.addWidget(self.main_splitter)

        self.compact_container = QWidget()
        compact_layout = QVBoxLayout(self.compact_container)
        compact_layout.setContentsMargins(0, 0, 0, 0)
        compact_layout.setSpacing(8)
        compact_layout.addWidget(self.sidebar_tabs)

        self.compact_canvas_holder = QWidget()
        compact_canvas_layout = QVBoxLayout(self.compact_canvas_holder)
        compact_canvas_layout.setContentsMargins(0, 0, 0, 0)
        compact_canvas_layout.setSpacing(0)
        compact_layout.addWidget(self.compact_canvas_holder, 1)

        self.responsive_layout.addWidget(self.desktop_container)
        self.responsive_layout.addWidget(self.compact_container)

        self._enter_desktop_mode(force=True)
        self._update_responsive_layout()
        self._init_search_defaults()

    def _build_search_bar(self, layout: QVBoxLayout) -> None:
        bar = QFrame()
        bar.setObjectName("searchBar")
        bar_layout = QHBoxLayout(bar)
        bar_layout.setContentsMargins(12, 12, 12, 12)
        bar_layout.setSpacing(10)

        prompt = QLabel("Search NFL entities")
        prompt.setObjectName("searchPrompt")
        bar_layout.addWidget(prompt)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search players, teams, seasons…")
        self.search_input.setClearButtonEnabled(False)
        bar_layout.addWidget(self.search_input, 1)

        clear_action = QAction(self.style().standardIcon(QStyle.SP_LineEditClearButton), "Clear", self)
        clear_action.triggered.connect(self._clear_search)
        self.search_input.addAction(clear_action, QLineEdit.TrailingPosition)

        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.setInterval(250)
        self.search_timer.timeout.connect(self._trigger_search)

        self.search_input.textChanged.connect(lambda _: self.search_timer.start())
        self.search_input.returnPressed.connect(self._trigger_search)

        self.search_completer = self._build_completer()
        self.search_input.setCompleter(self.search_completer)
        self.search_completer.activated[str].connect(self._on_completer_selected)

        layout.addWidget(bar)

    def _build_completer(self) -> QCompleter:
        completer = QCompleter([entity.name for entity in self._entities], self)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        return completer

    def _build_sidebar_panels(self) -> None:
        self.search_results_widget = QWidget()
        search_layout = QVBoxLayout(self.search_results_widget)
        search_layout.setContentsMargins(0, 0, 0, 0)
        search_layout.setSpacing(8)

        results_header = QLabel("Search Results")
        results_header.setObjectName("sectionHeader")
        search_layout.addWidget(results_header)

        filter_bar = QHBoxLayout()
        filter_bar.setContentsMargins(8, 0, 8, 0)
        filter_bar.setSpacing(6)

        filter_label = QLabel("Quick filters:")
        filter_bar.addWidget(filter_label)

        self.filter_buttons: Dict[str, QToolButton] = {}
        for entity_type in ENTITY_TYPES:
            button = QToolButton()
            button.setText(entity_type)
            button.setCheckable(True)
            button.setToolButtonStyle(Qt.ToolButtonTextOnly)
            button.clicked.connect(partial(self._on_filter_toggled, entity_type))
            filter_bar.addWidget(button)
            self.filter_buttons[entity_type] = button

        filter_bar.addStretch(1)
        search_layout.addLayout(filter_bar)

        self.search_results_list = QListWidget()
        self.search_results_list.setSelectionMode(QListWidget.SingleSelection)
        self.search_results_list.setUniformItemSizes(False)
        self.search_results_list.setAlternatingRowColors(True)
        self.search_results_list.itemSelectionChanged.connect(self._on_entity_selected)
        self.search_results_list.verticalScrollBar().valueChanged.connect(self._handle_result_scroll)
        search_layout.addWidget(self.search_results_list, 1)

        self.search_results_scroll = self._wrap_in_scroll(self.search_results_widget)

        self.dataset_widget = QWidget()
        dataset_layout = QVBoxLayout(self.dataset_widget)
        dataset_layout.setContentsMargins(0, 0, 0, 0)
        dataset_layout.setSpacing(8)

        dataset_header = QLabel("Data Tables")
        dataset_header.setObjectName("sectionHeader")
        dataset_layout.addWidget(dataset_header)

        self.dataset_list = QListWidget()
        self.dataset_list.setSelectionMode(QListWidget.SingleSelection)
        self.dataset_list.setUniformItemSizes(False)
        self.dataset_list.setAlternatingRowColors(True)
        self.dataset_list.itemSelectionChanged.connect(self._on_dataset_selected)
        dataset_layout.addWidget(self.dataset_list, 1)

        self.dataset_scroll = self._wrap_in_scroll(self.dataset_widget)

        self.sidebar_splitter = QSplitter(Qt.Vertical)
        self.sidebar_splitter.setChildrenCollapsible(False)
        self.sidebar_splitter.addWidget(self.search_results_scroll)
        self.sidebar_splitter.addWidget(self.dataset_scroll)
        self.sidebar_splitter.setStretchFactor(0, 3)
        self.sidebar_splitter.setStretchFactor(1, 2)

        self.sidebar_tabs = QTabWidget()
        self.sidebar_tabs.setTabPosition(QTabWidget.North)

    def _build_main_canvas(self) -> None:
        self.main_canvas = QWidget()
        layout = QVBoxLayout(self.main_canvas)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        breadcrumb_frame = QFrame()
        breadcrumb_layout = QHBoxLayout(breadcrumb_frame)
        breadcrumb_layout.setContentsMargins(0, 0, 0, 0)
        breadcrumb_layout.setSpacing(8)

        self.entity_chip = QLabel("No entity selected")
        self.entity_chip.setObjectName("entityChip")
        self.dataset_chip = QLabel("No dataset selected")
        self.dataset_chip.setObjectName("datasetChip")
        breadcrumb_layout.addWidget(self.entity_chip)
        breadcrumb_layout.addWidget(self.dataset_chip)
        breadcrumb_layout.addStretch(1)
        layout.addWidget(breadcrumb_frame)

        self.action_toolbar = QToolBar()
        self.action_toolbar.setToolButtonStyle(Qt.ToolButtonTextUnderIcon)
        self.action_toolbar.setIconSize(QSize(18, 18))

        export_csv_action = QAction(QIcon.fromTheme("document-save"), "Export CSV", self)
        export_csv_action.triggered.connect(lambda: self._export_table("csv"))
        self.action_toolbar.addAction(export_csv_action)

        export_parquet_action = QAction(QIcon.fromTheme("document-save-as"), "Export Parquet", self)
        export_parquet_action.triggered.connect(lambda: self._export_table("parquet"))
        self.action_toolbar.addAction(export_parquet_action)

        copy_action = QAction(QIcon.fromTheme("edit-copy"), "Copy", self)
        copy_action.setShortcut(QKeySequence.Copy)
        copy_action.triggered.connect(self._copy_selected_rows)
        self.action_toolbar.addAction(copy_action)

        reset_action = QAction(QIcon.fromTheme("view-refresh"), "Reset", self)
        reset_action.triggered.connect(self._reset_table_view)
        self.action_toolbar.addAction(reset_action)

        self.column_menu = QMenu("Columns", self)
        self._column_actions: List[QAction] = []
        column_button = QToolButton()
        column_button.setText("Columns")
        column_button.setPopupMode(QToolButton.InstantPopup)
        column_button.setMenu(self.column_menu)
        column_action = self.action_toolbar.addWidget(column_button)
        column_action.setVisible(True)

        layout.addWidget(self.action_toolbar)

        self.filter_bar = FilterBar()
        self.filter_bar.apply_button.clicked.connect(self._apply_filter)
        self.filter_bar.clear_button.clicked.connect(self._clear_filters)
        self.filter_bar.column_combo.currentIndexChanged.connect(self._on_filter_column_changed)
        layout.addWidget(self.filter_bar)

        self.table_model = DatasetTableModel(columns=[])
        self.proxy_model = DatasetFilterProxyModel()
        self.proxy_model.setSourceModel(self.table_model)

        self.table_view = QTableView()
        self.table_view.setModel(self.proxy_model)
        self.table_view.setSortingEnabled(True)
        self.table_view.horizontalHeader().setStretchLastSection(False)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.ExtendedSelection)
        self.table_view.verticalHeader().setVisible(False)

        self.pagination_bar = QFrame()
        pagination_layout = QHBoxLayout(self.pagination_bar)
        pagination_layout.setContentsMargins(8, 4, 8, 4)
        pagination_layout.setSpacing(6)

        self.prev_page_button = QPushButton("Previous")
        self.next_page_button = QPushButton("Next")
        self.page_label = QLabel("Page 1 of 1")

        self.prev_page_button.clicked.connect(lambda: self._change_page(-1))
        self.next_page_button.clicked.connect(lambda: self._change_page(1))

        pagination_layout.addWidget(self.prev_page_button)
        pagination_layout.addWidget(self.next_page_button)
        pagination_layout.addWidget(self.page_label)
        pagination_layout.addStretch(1)

        self.pagination_bar.hide()

        self.table_stack = QStackedWidget()
        self.empty_state = self._build_state_widget(
            "Select a player, team, or season to begin.",
            "Search, pick an entity, and then choose a dataset to explore.",
        )
        self.loading_state = self._build_state_widget("Loading dataset…", "Fetching rows and metadata.")
        self.error_state = self._build_state_widget("We hit a snag.", "Retry to attempt loading this dataset again.")

        self.retry_button = QPushButton("Retry")
        retry_layout = QVBoxLayout()
        retry_layout.setContentsMargins(0, 12, 0, 0)
        retry_layout.addWidget(self.retry_button, alignment=Qt.AlignHCenter)
        error_container = QWidget()
        error_container.setLayout(QVBoxLayout())
        error_container.layout().setContentsMargins(0, 0, 0, 0)
        error_container.layout().addWidget(self.error_state)
        error_container.layout().addLayout(retry_layout)

        self.retry_button.clicked.connect(self._retry_dataset)

        self.table_container = QWidget()
        table_layout = QVBoxLayout(self.table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(0)
        table_layout.addWidget(self.table_view)
        table_layout.addWidget(self.pagination_bar)

        self.table_stack.addWidget(self.empty_state)
        self.table_stack.addWidget(self.loading_state)
        self.table_stack.addWidget(error_container)
        self.table_stack.addWidget(self.table_container)
        self.table_stack.setCurrentWidget(self.empty_state)

        layout.addWidget(self.table_stack, 1)

        self.metadata_bar = QFrame()
        metadata_layout = QHBoxLayout(self.metadata_bar)
        metadata_layout.setContentsMargins(8, 6, 8, 6)
        metadata_layout.setSpacing(6)
        self.metadata_label = QLabel("Dataset source and coverage will appear here once selected.")
        metadata_layout.addWidget(self.metadata_label)
        layout.addWidget(self.metadata_bar)

    def _build_state_widget(self, headline: str, subtitle: str) -> QWidget:
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 40, 0, 40)
        layout.setSpacing(12)
        headline_label = QLabel(headline)
        headline_label.setAlignment(Qt.AlignHCenter)
        headline_label.setObjectName("stateHeadline")
        subtitle_label = QLabel(subtitle)
        subtitle_label.setAlignment(Qt.AlignHCenter)
        subtitle_label.setWordWrap(True)
        subtitle_label.setObjectName("stateSubtitle")
        layout.addWidget(headline_label)
        layout.addWidget(subtitle_label)
        return container

    def _wrap_in_scroll(self, widget: QWidget) -> QScrollArea:
        scroll = QScrollArea()
        scroll.setWidget(widget)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        return scroll

    # Responsive mode --------------------------------------------------------
    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._update_responsive_layout()

    def _update_responsive_layout(self) -> None:
        width = self.width()
        if width >= 1440:
            self._enter_desktop_mode()
        else:
            self._enter_compact_mode()

    def _enter_desktop_mode(self, *, force: bool = False) -> None:
        if not force and self._is_compact_mode is False:
            return

        self._is_compact_mode = False
        # Detach widgets from compact mode
        index = self.sidebar_tabs.indexOf(self.search_results_scroll)
        if index != -1:
            self.sidebar_tabs.removeTab(index)
        index = self.sidebar_tabs.indexOf(self.dataset_scroll)
        if index != -1:
            self.sidebar_tabs.removeTab(index)

        self.search_results_scroll.setParent(None)
        self.dataset_scroll.setParent(None)

        if self.sidebar_splitter.indexOf(self.search_results_scroll) == -1:
            self.sidebar_splitter.insertWidget(0, self.search_results_scroll)
        if self.sidebar_splitter.indexOf(self.dataset_scroll) == -1:
            self.sidebar_splitter.insertWidget(1, self.dataset_scroll)

        self.main_canvas.setParent(None)
        if self.main_splitter.indexOf(self.main_canvas) == -1:
            self.main_splitter.addWidget(self.main_canvas)

        self.responsive_layout.setCurrentWidget(self.desktop_container)

    def _enter_compact_mode(self) -> None:
        if self._is_compact_mode is True:
            return
        self._is_compact_mode = True

        self.search_results_scroll.setParent(None)
        self.dataset_scroll.setParent(None)
        if self.sidebar_tabs.indexOf(self.search_results_scroll) == -1:
            self.sidebar_tabs.addTab(self.search_results_scroll, "Search Results")
        if self.sidebar_tabs.indexOf(self.dataset_scroll) == -1:
            self.sidebar_tabs.addTab(self.dataset_scroll, "Data Tables")

        self.main_canvas.setParent(None)
        compact_layout: QVBoxLayout = self.compact_canvas_holder.layout()  # type: ignore[assignment]
        compact_layout.addWidget(self.main_canvas)

        self.responsive_layout.setCurrentWidget(self.compact_container)

    # Search behaviour -------------------------------------------------------
    def _init_search_defaults(self) -> None:
        self._active_filters: set[str] = set()
        self._perform_search(reset=True)

    def _clear_search(self) -> None:
        self.search_input.clear()
        self.search_input.setFocus()
        self._perform_search(reset=True)

    def _on_completer_selected(self, text: str) -> None:
        self.search_input.setText(text)
        self._trigger_search()

    def _trigger_search(self) -> None:
        self.search_timer.stop()
        self._perform_search(reset=True)

    def _perform_search(self, *, reset: bool) -> None:
        query = self.search_input.text().strip().lower()
        filtered = [entity for entity in self._entities if self._matches_query(entity, query)]

        if self._active_filters:
            filtered = [entity for entity in filtered if entity.entity_type in self._active_filters]

        filtered.sort(key=lambda entity: (entity.entity_type, entity.name))
        self._search_results = filtered
        self._search_page = 0
        self._populate_search_results(reset=True)
        self._refresh_completer()

    def _matches_query(self, entity: Entity, query: str) -> bool:
        if not query:
            return True
        haystack = f"{entity.name} {entity.description}".lower()
        return query in haystack

    def _populate_search_results(self, reset: bool = False) -> None:
        if reset:
            self.search_results_list.clear()

        start = self._search_page * self._page_size
        end = start + self._page_size
        chunk = self._search_results[start:end]
        for entity in chunk:
            item = QListWidgetItem()
            item.setData(Qt.UserRole, entity)
            widget = SearchResultItemWidget(entity)
            item.setSizeHint(widget.sizeHint())
            self.search_results_list.addItem(item)
            self.search_results_list.setItemWidget(item, widget)

        if not self._search_results:
            placeholder = QListWidgetItem("No results. Adjust your filters or query.")
            placeholder.setFlags(Qt.NoItemFlags)
            self.search_results_list.addItem(placeholder)

    def _handle_result_scroll(self, value: int) -> None:
        scrollbar = self.search_results_list.verticalScrollBar()
        if value >= scrollbar.maximum() - 4:
            if (self._search_page + 1) * self._page_size < len(self._search_results):
                self._search_page += 1
                self._populate_search_results()

    def _on_filter_toggled(self, entity_type: str, checked: bool) -> None:
        if checked:
            self._active_filters.add(entity_type)
        else:
            self._active_filters.discard(entity_type)
        self._perform_search(reset=True)

    def _refresh_completer(self) -> None:
        completer_model = [entity.name for entity in self._search_results[:40]]
        self.search_completer.model().setStringList(completer_model)

    def _on_entity_selected(self) -> None:
        if self._updating_selection:
            return
        item = self.search_results_list.currentItem()
        if not item:
            return
        entity = item.data(Qt.UserRole)
        if not isinstance(entity, Entity):
            return

        self._current_entity = entity
        self.entity_chip.setText(f"{entity.name} · {entity.entity_type}")
        self.dataset_chip.setText("Select a dataset")
        self._populate_dataset_options(entity)

    # Dataset selection ------------------------------------------------------
    def _populate_dataset_options(self, entity: Entity) -> None:
        self.dataset_list.clear()

        datasets = self._entity_datasets.get(entity.identifier, self._fallback_datasets)
        for dataset in datasets:
            item = QListWidgetItem()
            item.setData(Qt.UserRole, dataset)
            widget = DatasetListItemWidget(dataset)
            item.setSizeHint(widget.sizeHint())
            self.dataset_list.addItem(item)
            self.dataset_list.setItemWidget(item, widget)

        if datasets:
            self.dataset_list.setCurrentRow(0)

    def _on_dataset_selected(self) -> None:
        if self._updating_selection:
            return
        item = self.dataset_list.currentItem()
        if not item:
            return
        dataset = item.data(Qt.UserRole)
        if not isinstance(dataset, DatasetDefinition):
            return

        self._load_dataset(dataset)

    def _load_dataset(self, dataset: DatasetDefinition) -> None:
        self._current_dataset = dataset
        self._pending_dataset = dataset
        self.dataset_chip.setText(dataset.title)
        self.metadata_label.setText(f"Source: {dataset.source} • Coverage: {dataset.coverage}")
        self._set_table_state("loading")

        QTimer.singleShot(420, self._finish_dataset_load)

    def _finish_dataset_load(self) -> None:
        dataset = self._pending_dataset
        if not dataset:
            return

        if dataset.simulate_error:
            self._set_table_state("error")
            return

        if not dataset.rows:
            self._set_table_state("empty")
            return

        self.table_model.set_columns(dataset.columns)
        self.filter_bar.configure_for_columns(dataset.columns)
        self.filter_bar.column_combo.setCurrentIndex(0)
        self._on_filter_column_changed(0)
        self._build_column_menu(dataset.columns)

        self._pagination_enabled = dataset.mode == "paginated"
        if self._pagination_enabled:
            self._pagination_page = 0
            self._paginated_rows = dataset.rows
            self._apply_pagination()
        else:
            self.pagination_bar.hide()
            self.table_model.set_rows(dataset.rows)
            self.table_stack.setCurrentWidget(self.table_container)

    def _apply_pagination(self) -> None:
        dataset = self._current_dataset
        if not dataset:
            return

        start = self._pagination_page * dataset.page_size
        end = start + dataset.page_size
        current_rows = dataset.rows[start:end]
        self.table_model.set_rows(current_rows)
        total_pages = max(1, math.ceil(len(dataset.rows) / dataset.page_size))
        self.page_label.setText(f"Page {self._pagination_page + 1} of {total_pages}")
        self.prev_page_button.setEnabled(self._pagination_page > 0)
        self.next_page_button.setEnabled(self._pagination_page < total_pages - 1)
        self.pagination_bar.show()
        self.table_stack.setCurrentWidget(self.table_container)

    def _change_page(self, delta: int) -> None:
        dataset = self._current_dataset
        if not dataset:
            return
        total_pages = max(1, math.ceil(len(dataset.rows) / dataset.page_size))
        new_page = min(max(self._pagination_page + delta, 0), total_pages - 1)
        if new_page == self._pagination_page:
            return
        self._pagination_page = new_page
        self._apply_pagination()

    def _build_column_menu(self, columns: List[DatasetColumn]) -> None:
        self.column_menu.clear()
        self._column_actions = []
        for idx, column in enumerate(columns):
            action = QAction(column.name, self.column_menu)
            action.setCheckable(True)
            action.setChecked(True)
            action.toggled.connect(lambda checked, column_index=idx: self._toggle_column_visibility(checked, column_index))
            self.column_menu.addAction(action)
            self._column_actions.append(action)

    def _toggle_column_visibility(self, checked: bool, column_index: int) -> None:
        self.table_view.setColumnHidden(column_index, not checked)

    # Table interaction ------------------------------------------------------
    def _apply_filter(self) -> None:
        if self.table_model.columnCount() == 0:
            return
        data = self.filter_bar.column_combo.currentData()
        if not data:
            return
        column_index, dtype = data
        operation = self.filter_bar.operation_combo.currentText()
        value = self.filter_bar.value_edit.text().strip()
        self.proxy_model.set_filter(column_index, operation, value, dtype)
        self.table_stack.setCurrentWidget(self.table_container)

    def _clear_filters(self) -> None:
        self.filter_bar.value_edit.clear()
        self.proxy_model.clear_filters()
        self.table_stack.setCurrentWidget(self.table_container)

    def _on_filter_column_changed(self, index: int) -> None:
        data = self.filter_bar.column_combo.itemData(index)
        if not data:
            return
        _, dtype = data
        self.filter_bar._update_operation_combo(dtype)

    def _copy_selected_rows(self) -> None:
        selection = self.table_view.selectionModel()
        if not selection or not selection.selectedRows():
            QMessageBox.information(self, "Copy", "Select one or more rows to copy.")
            return

        rows_text = []
        headers = self.table_model.column_names()
        rows_text.append(",".join(headers))
        for proxy_index in selection.selectedRows():
            source_index = self.proxy_model.mapToSource(proxy_index)
            row_values = []
            for col in range(self.table_model.columnCount()):
                cell_index = self.table_model.index(source_index.row(), col)
                row_values.append(str(self.table_model.data(cell_index, Qt.DisplayRole)))
            rows_text.append(",".join(row_values))

        clipboard = QApplication.clipboard()
        clipboard.setText("\n".join(rows_text))
        QMessageBox.information(self, "Copy", "Copied selected rows to the clipboard.")

    def _export_table(self, fmt: str) -> None:
        dataset = self._current_dataset
        if not dataset:
            QMessageBox.information(self, "Export", "Select a dataset before exporting.")
            return

        # Compose file name suggestion
        suffix = "csv" if fmt == "csv" else "parquet"
        filename = f"{dataset.identifier}.{suffix}"

        if fmt == "csv":
            path = self._write_csv(filename)
            if path:
                QMessageBox.information(self, "Export", f"Exported rows to {path}.")
        else:
            # Proof-of-concept: we simulate Parquet export by confirming the action.
            QMessageBox.information(
                self,
                "Export",
                f"Parquet export simulated for {filename}. Integrate with pyarrow/pandas in production.",
            )

    def _write_csv(self, filename: str) -> Optional[str]:
        path = f"/tmp/{filename}"
        try:
            with open(path, "w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(self.table_model.column_names())
                for row in range(self.table_model.rowCount()):
                    writer.writerow(
                        [
                            self.table_model.data(self.table_model.index(row, col), Qt.DisplayRole)
                            for col in range(self.table_model.columnCount())
                        ]
                    )
        except OSError:
            QMessageBox.warning(self, "Export", "Failed to export CSV to the temporary directory.")
            return None
        return path

    def _reset_table_view(self) -> None:
        self.proxy_model.clear_filters()
        self.filter_bar.value_edit.clear()
        self.table_view.horizontalHeader().setSortIndicator(-1, Qt.AscendingOrder)
        for col in range(self.table_model.columnCount()):
            self.table_view.setColumnHidden(col, False)
        for action in self._column_actions:
            action.blockSignals(True)
            action.setChecked(True)
            action.blockSignals(False)
        self.pagination_bar.hide()
        if self._current_dataset:
            if self._pagination_enabled:
                self._apply_pagination()
            else:
                self.table_model.set_rows(self._current_dataset.rows)
        self.table_stack.setCurrentWidget(self.table_container)

    def _set_table_state(self, state: str) -> None:
        if state == "loading":
            self.table_stack.setCurrentWidget(self.loading_state)
        elif state == "error":
            self.table_stack.setCurrentIndex(2)
        elif state == "empty":
            self.table_stack.setCurrentWidget(self.empty_state)
        else:
            self.table_stack.setCurrentWidget(self.table_container)

    def _retry_dataset(self) -> None:
        if self._pending_dataset:
            self._pending_dataset.simulate_error = False
            self._load_dataset(self._pending_dataset)


__all__ = ["HomePage"]

