"""User interface components for the Down-Data desktop app.

This module exposes a lazy import wrapper so that environments without the
Qt runtime (e.g., headless CI running only backend tests) can still import the
package without pulling in PySide6 immediately.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - hints only
    from .main_window import MainWindow

__all__ = ["MainWindow"]


def __getattr__(name: str):
    if name == "MainWindow":
        from .main_window import MainWindow  # local import to avoid hard dependency at module import

        return MainWindow
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
