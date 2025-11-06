"""Down-Data application package."""

from __future__ import annotations

from typing import Any

__all__ = ["run_app"]


def run_app(*args: Any, **kwargs: Any) -> int:
    """Lazy import wrapper so non-UI modules can be imported without PySide6."""

    from .app import run_app as _run_app

    return _run_app(*args, **kwargs)
