"""Base widget for all application pages."""

from __future__ import annotations

from PySide6.QtWidgets import QVBoxLayout, QWidget


class SectionPage(QWidget):
    """Common page base that provides a vertical layout helper."""

    def __init__(self, *, title: str | None = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._title = title
        self._root_layout = QVBoxLayout(self)
        self._root_layout.setContentsMargins(24, 24, 24, 24)
        self._root_layout.setSpacing(16)

    @property
    def root_layout(self) -> QVBoxLayout:
        return self._root_layout

    @property
    def title(self) -> str | None:
        return self._title
