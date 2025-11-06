"""Main application window hosting all feature pages."""

from __future__ import annotations

from typing import Dict

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QSizePolicy,
    QStackedWidget,
    QWidget,
)

from down_data.backend import PlayerService

from .pages.player_search_page import PlayerSearchPage
from .pages.placeholder_page import PlaceholderPage

class MainWindow(QMainWindow):
    """Application shell with navigation and a stacked content area."""

    def __init__(self, service: PlayerService | None = None) -> None:
        super().__init__()
        self.setWindowTitle("Down-Data Player Explorer")
        self.resize(1200, 800)

        self._service = service or PlayerService()
        self._pages: Dict[str, QWidget] = {}

        central = QWidget(self)
        layout = QHBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)

        self._nav = QListWidget(central)
        self._nav.setFixedWidth(220)
        self._nav.setSpacing(2)
        self._nav.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        self._nav.setAlternatingRowColors(True)
        self._nav.setSelectionMode(QListWidget.SingleSelection)

        self._stack = QStackedWidget(central)
        self._stack.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        layout.addWidget(self._nav)
        layout.addWidget(self._stack, stretch=1)

        self.setCentralWidget(central)

        self._build_pages()
        self._nav.currentRowChanged.connect(self._stack.setCurrentIndex)
        self._nav.setCurrentRow(0)

    def _build_pages(self) -> None:
        pages = [
            ("Player Search", PlayerSearchPage(service=self._service)),
            ("Player Profile - Summary", PlaceholderPage("Player profile summary layout placeholder")),
            ("Player Profile - Contract", PlaceholderPage("Contract details placeholder")),
            ("Player Profile - Injury History", PlaceholderPage("Injury history placeholder")),
            ("Player History - Accomplishments", PlaceholderPage("Player history & accolades placeholder")),
            ("Player Stats - Overview", PlaceholderPage("Stats overview placeholder")),
            ("Player Stats - Game Log", PlaceholderPage("Game log placeholder")),
            ("Player Stats - Streaks", PlaceholderPage("Streaks placeholder")),
        ]

        for title, widget in pages:
            item = QListWidgetItem(title)
            item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            self._nav.addItem(item)
            self._stack.addWidget(widget)
            self._pages[title] = widget

    def page(self, title: str) -> QWidget:
        return self._pages[title]
