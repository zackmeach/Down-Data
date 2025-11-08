"""Context bar widget: logo, context title, schedule control, Continue button."""

from __future__ import annotations

from typing import Iterable, Optional

from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QComboBox, QFrame, QHBoxLayout, QLabel, QPushButton, QWidget


class ContextBar(QFrame):
    """Top context bar mirroring OOTP's secondary bar."""

    def __init__(
        self,
        *,
        title: str = "FIND A PLAYER",
        schedule_options: Optional[Iterable[str]] = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("ContextBar")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        # Left: league/logo placeholder
        self._logo = QLabel(self)
        self._logo.setObjectName("ContextLogo")
        self._logo.setFixedSize(28, 28)
        self._logo.setScaledContents(True)
        layout.addWidget(self._logo)

        # Center: title
        self._title = QLabel(title, self)
        self._title.setObjectName("ContextTitle")
        layout.addWidget(self._title, 1)

        # Right: schedule control + Continue button
        self._schedule = QComboBox(self)
        self._schedule.setObjectName("ContextSchedule")
        for item in (list(schedule_options) if schedule_options is not None else ["Today", "Tomorrow", "This Week"]):
            self._schedule.addItem(item)
        layout.addWidget(self._schedule)

        self._continue = QPushButton("CONTINUE", self)
        self._continue.setObjectName("ContinueButton")
        layout.addWidget(self._continue)

    # API ------------------------------------------------------------------
    def set_logo_pixmap(self, pixmap: QPixmap | None) -> None:
        if pixmap is None:
            self._logo.clear()
            return
        self._logo.setPixmap(pixmap)

    def set_title(self, text: str) -> None:
        self._title.setText(text)

    def set_schedule_options(self, options: Iterable[str], *, select_index: int = 0) -> None:
        self._schedule.clear()
        for item in options:
            self._schedule.addItem(str(item))
        if 0 <= select_index < self._schedule.count():
            self._schedule.setCurrentIndex(select_index)


