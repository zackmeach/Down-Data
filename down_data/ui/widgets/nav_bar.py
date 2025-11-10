"""Top navigation bar widget spanning the grid's first row."""

from __future__ import annotations

from collections.abc import Iterable

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QButtonGroup, QFrame, QHBoxLayout, QPushButton, QWidget


class NavBar(QFrame):
    """Simple horizontal navigation bar with clickable items."""

    backRequested = Signal()
    homeRequested = Signal()
    forwardRequested = Signal()

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

        self._back_button = QPushButton("←", self)
        self._back_button.setObjectName("NavHistoryButton")
        self._back_button.setFlat(True)
        self._back_button.setEnabled(False)
        self._back_button.setToolTip("Back")
        self._back_button.clicked.connect(self.backRequested.emit)
        layout.addWidget(self._back_button)

        self._home_button = QPushButton("⌂", self)
        self._home_button.setObjectName("NavHistoryButton")
        self._home_button.setFlat(True)
        self._home_button.setToolTip("Home")
        self._home_button.clicked.connect(self.homeRequested.emit)
        layout.addWidget(self._home_button)

        self._forward_button = QPushButton("→", self)
        self._forward_button.setObjectName("NavHistoryButton")
        self._forward_button.setFlat(True)
        self._forward_button.setEnabled(False)
        self._forward_button.setToolTip("Forward")
        self._forward_button.clicked.connect(self.forwardRequested.emit)
        layout.addWidget(self._forward_button)

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

    def set_history_enabled(self, *, can_go_back: bool, can_go_forward: bool) -> None:
        """Enable or disable navigation history buttons."""

        self._back_button.setEnabled(can_go_back)
        self._forward_button.setEnabled(can_go_forward)


