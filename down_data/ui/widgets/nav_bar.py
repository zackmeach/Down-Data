"""Top navigation bar widget spanning the grid's first row."""

from __future__ import annotations

from typing import Iterable

from PySide6.QtWidgets import QButtonGroup, QFrame, QHBoxLayout, QPushButton, QWidget


class NavBar(QFrame):
    """Simple horizontal navigation bar with clickable items."""

    def __init__(
        self,
        items: Iterable[str] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("NavBar")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        default_items = ["FILE", "GAME", "Z. MEACHAM", "MLB"]
        self._group = QButtonGroup(self)
        self._group.setExclusive(True)

        for idx, text in enumerate(list(items) if items is not None else default_items):
            btn = QPushButton(text.upper(), self)
            btn.setObjectName("NavItem")
            btn.setCheckable(True)
            btn.setAutoDefault(False)
            btn.setFlat(True)
            layout.addWidget(btn)
            self._group.addButton(btn, idx)
            if idx == 0:
                btn.setChecked(True)

        layout.addStretch(1)


