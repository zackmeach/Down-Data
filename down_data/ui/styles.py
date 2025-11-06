"""Shared styling helpers for the Qt widgets."""

from __future__ import annotations

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication


PRIMARY_COLOUR = QColor(30, 45, 60)
ACCENT_COLOUR = QColor(0, 120, 215)
BACKGROUND_COLOUR = QColor(245, 247, 250)
TEXT_COLOUR = QColor(30, 30, 30)


def apply_app_palette(app: QApplication) -> None:
    """Apply a light theme palette across the application."""

    palette = QPalette()
    palette.setColor(QPalette.Window, BACKGROUND_COLOUR)
    palette.setColor(QPalette.WindowText, TEXT_COLOUR)
    palette.setColor(QPalette.Base, QColor(255, 255, 255))
    palette.setColor(QPalette.AlternateBase, QColor(240, 240, 240))
    palette.setColor(QPalette.ToolTipBase, QColor(255, 255, 220))
    palette.setColor(QPalette.ToolTipText, TEXT_COLOUR)
    palette.setColor(QPalette.Text, TEXT_COLOUR)
    palette.setColor(QPalette.Button, QColor(240, 240, 240))
    palette.setColor(QPalette.ButtonText, TEXT_COLOUR)
    palette.setColor(QPalette.Highlight, ACCENT_COLOUR)
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))

    app.setPalette(palette)
