"""Utility widget that enforces a fixed aspect ratio for its child."""

from __future__ import annotations

from typing import Optional

from PySide6.QtCore import QSize
from PySide6.QtWidgets import QWidget


class AspectRatioWidget(QWidget):
    """Letterboxes its child widget to maintain a target aspect ratio."""

    def __init__(self, child: QWidget, ratio: float = 16 / 9, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._child = child
        self._ratio = ratio
        child.setParent(self)
        child.show()

    def resizeEvent(self, event):  # type: ignore[override]
        size: QSize = event.size()
        width = size.width()
        height = size.height()
        if width <= 0 or height <= 0:
            return

        target_width = width
        target_height = int(width / self._ratio)

        if target_height > height:
            target_height = height
            target_width = int(height * self._ratio)

        x_offset = (width - target_width) // 2
        y_offset = (height - target_height) // 2
        self._child.setGeometry(x_offset, y_offset, target_width, target_height)

    def child(self) -> QWidget:
        return self._child


__all__ = ["AspectRatioWidget"]
