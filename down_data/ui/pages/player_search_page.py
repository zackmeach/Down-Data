"""Player search landing page."""

from __future__ import annotations

from typing import List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from down_data.backend.player_service import PlayerService, PlayerSummary

from .base_page import SectionPage


class PlayerSearchPage(SectionPage):
    """Initial screen that will eventually mirror the OOTP "find player" view."""

    def __init__(self, *, service: PlayerService, parent: Optional[QWidget] = None) -> None:
        super().__init__(title="Player Search", parent=parent)
        self._service = service
        self._results: List[PlayerSummary] = []

        self._build_header()
        self._build_filters()
        self._build_results()
        self._build_selection_placeholder()

    # UI construction helpers -------------------------------------------------

    def _build_header(self) -> None:
        header = QLabel("Find NFL players and dive into their profiles.", self)
        header.setObjectName("pageTitle")
        header.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header.setStyleSheet("font-size: 24px; font-weight: 600;")
        self.root_layout.addWidget(header)

        description = QLabel(
            "This is the starting point for the OOTP-style explorer. "
            "Use the filters below to find players. Future iterations will "
            "replace the placeholder panels with the designs captured in the "
            "`ootp-images` reference screenshots.",
            self,
        )
        description.setWordWrap(True)
        description.setStyleSheet("color: #555; font-size: 14px;")
        self.root_layout.addWidget(description)

    def _build_filters(self) -> None:
        filters_box = QGroupBox("Search filters", self)
        filters_layout = QFormLayout(filters_box)
        filters_layout.setLabelAlignment(Qt.AlignLeft)

        self._name_input = QLineEdit(filters_box)
        self._name_input.setPlaceholderText("Patrick Mahomes")
        filters_layout.addRow("Name", self._name_input)

        self._team_input = QLineEdit(filters_box)
        self._team_input.setPlaceholderText("KC")
        filters_layout.addRow("Team", self._team_input)

        self._position_input = QLineEdit(filters_box)
        self._position_input.setPlaceholderText("QB")
        filters_layout.addRow("Position", self._position_input)

        buttons_row = QHBoxLayout()
        self._search_button = QPushButton("Search", filters_box)
        self._search_button.setDefault(True)
        self._clear_button = QPushButton("Clear", filters_box)
        buttons_row.addWidget(self._search_button)
        buttons_row.addWidget(self._clear_button)
        filters_layout.addRow("", buttons_row)

        self.root_layout.addWidget(filters_box)

        self._search_button.clicked.connect(self._run_search)
        self._clear_button.clicked.connect(self._reset_filters)

    def _build_results(self) -> None:
        results_box = QGroupBox("Results", self)
        results_layout = QVBoxLayout(results_box)

        self._results_table = QTableWidget(results_box)
        self._results_table.setColumnCount(4)
        self._results_table.setHorizontalHeaderLabels(["Name", "Team", "Position", "Draft Year"])
        self._results_table.horizontalHeader().setStretchLastSection(True)
        self._results_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._results_table.setSelectionMode(QTableWidget.SingleSelection)
        self._results_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._results_table.verticalHeader().setVisible(False)

        results_layout.addWidget(self._results_table)

        helper = QLabel(
            "Selecting a player will eventually load the profile, contract, stats, "
            "and history views in the tabs listed on the left navigation.",
            results_box,
        )
        helper.setWordWrap(True)
        helper.setStyleSheet("color: #666; font-size: 12px;")
        results_layout.addWidget(helper)

        self.root_layout.addWidget(results_box)

        self._results_table.itemSelectionChanged.connect(self._handle_selection_changed)

    def _build_selection_placeholder(self) -> None:
        placeholder = QGroupBox("Selected player", self)
        layout = QVBoxLayout(placeholder)

        self._selection_label = QLabel(
            "No player selected yet. Search above and pick a result to populate "
            "the detail pages.",
            placeholder,
        )
        self._selection_label.setWordWrap(True)
        self._selection_label.setStyleSheet(
            "color: #445; font-style: italic; background: rgba(0,0,0,0.03); padding: 12px;"
        )
        layout.addWidget(self._selection_label)

        self.root_layout.addWidget(placeholder)
        self.root_layout.addStretch(1)

    # Event handlers ----------------------------------------------------------

    def _reset_filters(self) -> None:
        self._name_input.clear()
        self._team_input.clear()
        self._position_input.clear()
        self._results_table.setRowCount(0)
        self._results.clear()
        self._selection_label.setText(
            "No player selected yet. Search above and pick a result to populate the detail pages."
        )

    def _run_search(self) -> None:
        name = self._name_input.text().strip()
        team = self._team_input.text().strip() or None
        position = self._position_input.text().strip() or None

        try:
            results = self._service.search_players(name=name, team=team, position=position)
        except Exception as exc:  # pragma: no cover - UI level error handling
            QMessageBox.critical(self, "Search failed", f"Unable to search players: {exc}")
            return

        self._populate_results(results)

    def _populate_results(self, results: List[PlayerSummary]) -> None:
        self._results = results
        self._results_table.setRowCount(len(results))
        for row_index, summary in enumerate(results):
            profile = summary.profile
            cells = [
                profile.full_name,
                summary.team or "-",
                summary.position or "-",
                str(profile.draft_year or "-"),
            ]
            for col_index, value in enumerate(cells):
                item = QTableWidgetItem(value)
                self._results_table.setItem(row_index, col_index, item)
        if results:
            self._selection_label.setText("Select a player to load their information into the other screens.")
        else:
            self._selection_label.setText("No players found. Adjust the filters and search again.")

    def _handle_selection_changed(self) -> None:
        indexes = self._results_table.selectionModel().selectedRows()
        if not indexes:
            return
        selected_index = indexes[0].row()
        if selected_index >= len(self._results):
            return
        summary = self._results[selected_index]
        profile = summary.profile
        message = (
            f"Selected: <b>{profile.full_name}</b><br>"
            f"Team: {summary.team or 'N/A'} &nbsp;&nbsp;"
            f"Position: {summary.position or 'N/A'}<br>"
            "Use the navigation to open other detail pages."
        )
        self._selection_label.setText(message)
