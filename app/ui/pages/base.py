"""Common base class for application pages."""

from __future__ import annotations

from typing import Optional

from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget


class BasePage(QWidget):
    """Convenience subclass that standardises page structure."""

    title: str = ""
    description: Optional[str] = None

    def __init__(self, *, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._header_label = QLabel(self.title)
        self._description_label = QLabel(self.description or "")
        self._description_label.setWordWrap(True)
        self._init_layout()

    def _init_layout(self) -> None:
        layout = QVBoxLayout(self)
        self._header_label.setObjectName("pageHeader")
        layout.addWidget(self._header_label)
        if self.description:
            layout.addWidget(self._description_label)
        layout.addStretch(1)


__all__ = ["BasePage"]
