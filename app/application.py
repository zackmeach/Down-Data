"""Utilities for instantiating the QApplication used by Down Data."""

from __future__ import annotations

import sys
from typing import Iterable, Optional

from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import QApplication


def _configure_high_dpi(application: QApplication) -> None:
    """Enable high DPI scaling so the UI looks crisp on modern displays."""

    QGuiApplication.setHighDpiScaleFactorRoundingPolicy(
        QGuiApplication.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    application.setAttribute(QApplication.AA_EnableHighDpiScaling)
    application.setAttribute(QApplication.AA_UseHighDpiPixmaps)


def create_app(argv: Optional[Iterable[str]] = None) -> QApplication:
    """Create and configure the :class:`QApplication` instance for the app.

    Parameters
    ----------
    argv:
        Optional iterable of command line arguments. When ``None`` the arguments
        from :data:`sys.argv` are used.
    """

    args = list(argv) if argv is not None else sys.argv
    app = QApplication(args)
    app.setApplicationDisplayName("Down Data")
    app.setApplicationName("Down Data")
    app.setApplicationVersion("0.1.0")

    _configure_high_dpi(app)

    return app


__all__ = ["create_app"]
