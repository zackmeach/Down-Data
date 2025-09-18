"""Main window shell for the Down Data desktop experience."""

from __future__ import annotations

from typing import Dict, List, Tuple

from PySide6.QtCore import QEvent, Qt, QTimer
from PySide6.QtWidgets import (
    QHBoxLayout,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QStackedWidget,
    QWidget,
)

# The data layer lives at the project root to make it accessible from scripts
# as well as the UI code.
from data import DataPipeline
from .aspect_ratio_widget import AspectRatioWidget
from .pages import DataBrowserPage, HomePage, PlayerWatchPage


class MainWindow(QMainWindow):
    """Hosts the multi-page navigation shell."""

    def __init__(self, pipeline: DataPipeline, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.pipeline = pipeline
        self.setWindowTitle("Down Data")
        self.setMinimumSize(1280, 720)
        self._pages: Dict[str, QWidget] = {}
        self._stack = QStackedWidget()
        self._nav_list = QListWidget()
        self._setup_ui()
        self._nav_list.currentRowChanged.connect(self._stack.setCurrentIndex)
        QTimer.singleShot(0, self.showMaximized)

    def _setup_ui(self) -> None:
        content = QWidget()
        layout = QHBoxLayout(content)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(24)

        self._nav_list.setFixedWidth(220)
        self._nav_list.setObjectName("navigationList")
        self._stack.setObjectName("pageStack")

        for page in self._create_pages():
            self._add_page(page)

        self._nav_list.setCurrentRow(0)

        layout.addWidget(self._nav_list)
        layout.addWidget(self._stack, stretch=1)

        aspect = AspectRatioWidget(content, ratio=16 / 9)
        aspect.setObjectName("aspectContainer")
        self.setCentralWidget(aspect)

    def _create_pages(self) -> List[Tuple[str, QWidget]]:
        home = HomePage()
        browser = DataBrowserPage()
        watch = PlayerWatchPage()
        return [
            ("Home", home),
            ("Data Browser", browser),
            ("Player Watch", watch),
        ]

    def _add_page(self, entry: Tuple[str, QWidget]) -> None:
        label, widget = entry
        item = QListWidgetItem(label)
        item.setData(Qt.UserRole, widget)
        self._nav_list.addItem(item)
        self._stack.addWidget(widget)
        self._pages[label] = widget

    def changeEvent(self, event: QEvent) -> None:  # type: ignore[override]
        super().changeEvent(event)
        if event.type() == QEvent.WindowStateChange and not self.isMaximized():
            QTimer.singleShot(0, self.showMaximized)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        if not self.isMaximized():
            QTimer.singleShot(0, self.showMaximized)
        super().resizeEvent(event)


__all__ = ["MainWindow"]
