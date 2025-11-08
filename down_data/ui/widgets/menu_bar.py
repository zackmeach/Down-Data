"""Secondary menu bar with exclusive segmented items."""

from __future__ import annotations

from typing import Iterable, Optional

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QButtonGroup, QFrame, QHBoxLayout, QPushButton, QWidget


class MenuBar(QFrame):
    """Horizontal segmented menu bar with exclusive selection."""

    # Signal emitted when selection changes: (index: int, text: str)
    selectionChanged = Signal(int, str)

    def __init__(
        self,
        items: Optional[Iterable[str]] = None,
        *,
        parent: QWidget | None = None,
        default_index: int = 0,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("MenuBar")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self._group = QButtonGroup(self)
        self._group.setExclusive(True)
        self._group.idClicked.connect(self._on_button_clicked)

        self._items = list(items) if items is not None else []
        for idx, text in enumerate(self._items):
            btn = QPushButton(text.upper(), self)
            btn.setObjectName("MenuItem")
            btn.setCheckable(True)
            btn.setAutoDefault(False)
            btn.setFlat(True)
            layout.addWidget(btn)
            self._group.addButton(btn, idx)

        # Select default
        if self._group.buttons():
            chosen = default_index if 0 <= default_index < len(self._group.buttons()) else 0
            self._group.buttons()[chosen].setChecked(True)

        layout.addStretch(1)

    def _on_button_clicked(self, button_id: int) -> None:
        """Handle button click and emit signal."""
        if 0 <= button_id < len(self._items):
            self.selectionChanged.emit(button_id, self._items[button_id])

    def set_items(self, items: Iterable[str], default_index: int = 0) -> None:
        """Replace all menu items."""
        # Clear existing buttons
        for btn in self._group.buttons():
            self._group.removeButton(btn)
            btn.deleteLater()

        # Add new buttons
        self._items = list(items)
        layout = self.layout()
        for idx, text in enumerate(self._items):
            btn = QPushButton(text.upper(), self)
            btn.setObjectName("MenuItem")
            btn.setCheckable(True)
            btn.setAutoDefault(False)
            btn.setFlat(True)
            layout.insertWidget(idx, btn)
            self._group.addButton(btn, idx)

        # Select default
        if self._group.buttons():
            chosen = default_index if 0 <= default_index < len(self._group.buttons()) else 0
            self._group.buttons()[chosen].setChecked(True)
            self._on_button_clicked(chosen)  # Emit signal for initial selection


