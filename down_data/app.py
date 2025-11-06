"""Application bootstrap for the Down-Data desktop client."""

from __future__ import annotations

import sys
from typing import Optional

from PySide6.QtWidgets import QApplication

from .ui.main_window import MainWindow
from .ui.styles import apply_app_palette


def create_qt_app(existing: Optional[QApplication] = None) -> QApplication:
    """Create (or return) the Qt application instance.

    PySide requires a single QApplication instance. Providing this helper makes it
    easier to unit-test widgets without needing to spawn a new application each
    time, and it also means future entry-points (CLI, tests, etc.) can reuse the
    same bootstrap logic.
    """

    if existing is not None:
        return existing
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    return app


def run_app() -> int:
    """Launch the Qt application and return the exit code."""

    app = create_qt_app()
    apply_app_palette(app)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    sys.exit(run_app())
