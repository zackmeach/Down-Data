"""Landing page for the application."""

from __future__ import annotations

from PySide6.QtWidgets import QLabel, QPushButton, QVBoxLayout

from .base import BasePage


class HomePage(BasePage):
    title = "Welcome to Down Data"
    description = (
        "Kick off your NFL research workflow by ingesting data, exploring analytics, "
        "and tracking players you care about."
    )

    def _init_layout(self) -> None:
        layout = QVBoxLayout(self)
        header = QLabel(self.title)
        header.setObjectName("pageHeader")
        tagline = QLabel(self.description)
        tagline.setWordWrap(True)
        layout.addWidget(header)
        layout.addWidget(tagline)

        cta = QPushButton("Start ingesting data")
        cta.setObjectName("primaryCta")
        layout.addWidget(cta)
        layout.addStretch(1)


__all__ = ["HomePage"]
