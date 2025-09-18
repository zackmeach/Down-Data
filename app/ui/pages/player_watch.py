"""Page dedicated to tracking favourite players."""

from __future__ import annotations

from PySide6.QtWidgets import QLabel, QListWidget, QPushButton, QVBoxLayout

from .base import BasePage


class PlayerWatchPage(BasePage):
    title = "Player Watch"
    description = (
        "Create a shortlist of players and monitor their recent performances."
    )

    def _init_layout(self) -> None:
        layout = QVBoxLayout(self)
        header = QLabel(self.title)
        header.setObjectName("pageHeader")
        blurb = QLabel(self.description)
        blurb.setWordWrap(True)
        self._list = QListWidget()
        self._list.setObjectName("playerWatchList")
        add_button = QPushButton("Add player…")
        add_button.setObjectName("playerWatchAdd")

        layout.addWidget(header)
        layout.addWidget(blurb)
        layout.addWidget(self._list, stretch=1)
        layout.addWidget(add_button)


__all__ = ["PlayerWatchPage"]
