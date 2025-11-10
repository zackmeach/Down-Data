"""Main application window hosting all feature pages."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QResizeEvent
from PySide6.QtWidgets import QMainWindow, QWidget

from down_data.backend import PlayerService

from .pages.content_page import ContentPage

class MainWindow(QMainWindow):
    """Application shell with navigation and a stacked content area."""

    def __init__(self, service: PlayerService | None = None) -> None:
        super().__init__()
        self.setWindowTitle("Down-Data Player Explorer")
        # Prefer a 16:9 base size and start maximized; enforce 16:9 on manual resizes
        self.resize(1600, 900)
        self.setMinimumSize(1280, 720)
        self.setWindowState(Qt.WindowMaximized)

        self._aspect_ratio = 16 / 9
        self._enforcing_aspect = False

        self._service = service or PlayerService()
        self._pages: dict[str, QWidget] = {}
        self._build_pages()

        # Set the ContentPage as the central widget (handles all navigation)
        self.setCentralWidget(self._pages["Content"])

    def resizeEvent(self, event: QResizeEvent) -> None:  # type: ignore[override]
        """Keep the window near a 16:9 aspect ratio when manually resized."""
        if self._enforcing_aspect:
            return super().resizeEvent(event)
        # Do not interfere while maximized (window manager controls geometry)
        if self.isMaximized():
            return super().resizeEvent(event)

        self._enforcing_aspect = True
        try:
            width = self.width()
            height = self.height()
            desired_by_width = int(round(width / self._aspect_ratio))
            desired_by_height = int(round(height * self._aspect_ratio))

            # Choose the adjustment with minimal change
            if abs(desired_by_width - height) <= abs(desired_by_height - width):
                self.resize(width, max(self.minimumHeight(), desired_by_width))
            else:
                self.resize(max(self.minimumWidth(), desired_by_height), height)
        finally:
            self._enforcing_aspect = False
        return super().resizeEvent(event)

    def _build_pages(self) -> None:
        # Create the main content page with hierarchical navigation
        self._pages["Content"] = ContentPage(service=self._service)

    def page(self, title: str) -> QWidget:
        return self._pages[title]
