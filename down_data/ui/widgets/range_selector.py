"""Reusable range selector widget used in filter panels."""

from __future__ import annotations

from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QWidget,
)


class RangeSelector(QWidget):
    """Row with two input widgets separated by a label (e.g. 'to')."""

    def __init__(
        self,
        left_widget: QWidget,
        right_widget: QWidget,
        *,
        label_text: str = "to",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("RangeSelector")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        left_cell = self._wrap_widget(left_widget)
        right_cell = self._wrap_widget(right_widget)

        label = QLabel(label_text, self)
        label.setObjectName("RangeSelectorLabel")

        layout.addWidget(left_cell)
        layout.addWidget(label)
        layout.addWidget(right_cell)
        layout.addStretch(1)

    @staticmethod
    def _wrap_widget(widget: QWidget) -> QFrame:
        """Wrap the provided widget in a styled frame."""

        frame = QFrame()
        frame.setObjectName("RangeSelectorCell")
        frame_layout = QHBoxLayout(frame)
        frame_layout.setContentsMargins(10, 4, 10, 4)
        frame_layout.setSpacing(0)

        widget.setParent(frame)
        widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        frame_layout.addWidget(widget)

        return frame

