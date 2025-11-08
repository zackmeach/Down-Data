"""Generic placeholder page used until dedicated layouts are built."""

from __future__ import annotations

from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QWidget

from .base_page import SectionPage


class PlaceholderPage(SectionPage):
    def __init__(self, message: str, *, title: str | None = None, parent: Optional[QWidget] = None) -> None:
        super().__init__(title=title, parent=parent)
        label = QLabel(message, self)
        label.setWordWrap(True)
        label.setAlignment(Qt.AlignCenter)
        label.setProperty("placeholder", True)
        label.setStyleSheet(
            "font-size: 18px; color: #445; background-color: rgba(0, 0, 0, 0.02);"
            "border: 1px dashed #778; padding: 32px;"
        )
        self.root_layout.addStretch(1)
        self.root_layout.addWidget(label, alignment=Qt.AlignCenter)
        self.root_layout.addStretch(1)
