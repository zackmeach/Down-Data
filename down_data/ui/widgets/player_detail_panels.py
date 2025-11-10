"""Widgets used on the player detail page panels."""

from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)


@dataclass(frozen=True)
class RatingBreakdown:
    """Container describing a rating with its current/potential values."""

    label: str
    current: int
    potential: int
    subratings: tuple["RatingBreakdown", ...] = ()


class PersonalDetailsWidget(QWidget):
    """Display a vertical list of personal details for a player."""

    def __init__(self, fields: list[str], *, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._fields = fields
        self._value_labels: dict[str, QLabel] = {}

        layout = QGridLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(8)

        for row, field in enumerate(fields):
            title_label = QLabel(field.upper(), self)
            title_label.setObjectName("DetailHeadingLabel")
            title_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)

            value_label = QLabel("—", self)
            value_label.setObjectName("DetailValueLabel")
            value_label.setWordWrap(True)
            value_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)

            layout.addWidget(title_label, row, 0, alignment=Qt.AlignTop | Qt.AlignLeft)
            layout.addWidget(value_label, row, 1, alignment=Qt.AlignTop | Qt.AlignLeft)

            self._value_labels[field] = value_label

        layout.setColumnStretch(0, 0)
        layout.setColumnStretch(1, 1)

    def set_details(self, values: list[tuple[str, str]]) -> None:
        """Update the displayed values."""

        provided = {key: value for key, value in values}
        for field in self._fields:
            value = provided.get(field, "—")
            label = self._value_labels.get(field)
            if label is not None:
                label.setText(value if value else "—")


class _RatingBar(QWidget):
    """Custom horizontal bar visualising current and potential ratings."""

    def __init__(self, *, max_rating: int = 80, compact: bool = False, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._max_rating = max(max_rating, 1)
        self._current = 0
        self._potential = 0
        self._compact = compact

        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setMinimumHeight(12 if compact else 20)

    def sizeHint(self) -> QSize:  # type: ignore[override]
        base_height = 12 if self._compact else 20
        return QSize(120, base_height)

    def set_values(self, current: int, potential: int) -> None:
        self._current = max(0, min(current, self._max_rating))
        self._potential = max(self._current, min(potential, self._max_rating))
        self.update()

    @staticmethod
    def _rating_color(rating: int) -> QColor:
        if rating >= 70:
            return QColor("#3C8DFF")  # elite -> blue
        if rating >= 55:
            return QColor("#4CAF50")  # above average -> green
        if rating >= 45:
            return QColor("#D4C23D")  # average -> yellow
        if rating >= 35:
            return QColor("#E6862E")  # below average -> orange
        return QColor("#D9534F")  # poor -> red

    def paintEvent(self, event) -> None:  # type: ignore[override]
        rect = self.rect()
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        background_color = QColor("#1B1F24")
        painter.fillRect(rect, background_color)

        border_pen = QPen(QColor("#2F3B45"))
        border_pen.setWidth(1)
        painter.setPen(border_pen)
        painter.drawRect(rect.adjusted(0, 0, -1, -1))

        width = rect.width()
        height = rect.height()

        if self._potential > 0:
            potential_width = int(width * (self._potential / self._max_rating))
            potential_rect = rect.adjusted(1, 1, -1, -1)
            potential_rect.setWidth(max(potential_width - 2, 0))
            painter.fillRect(potential_rect, QColor("#2E3A46"))

        if self._current > 0:
            current_width = int(width * (self._current / self._max_rating))
            current_rect = rect.adjusted(1, 1, -1, -1)
            current_rect.setWidth(max(current_width - 2, 0))
            painter.fillRect(current_rect, self._rating_color(self._current))

        painter.end()


class BasicRatingsWidget(QWidget):
    """Panel widget rendering rating rows with optional subratings."""

    def __init__(self, *, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(16)
        self._placeholder = QLabel("RATINGS UNAVAILABLE", self)
        self._placeholder.setAlignment(Qt.AlignCenter)
        self._placeholder.setStyleSheet("color: #7A8894; letter-spacing: 1px;")

    def _clear_rows(self) -> None:
        while self._layout.count():
            item = self._layout.takeAt(0)
            widget = item.widget()
            if widget is None:
                continue
            if widget is self._placeholder:
                widget.setParent(None)
            else:
                widget.deleteLater()

    def set_ratings(self, ratings: list[RatingBreakdown]) -> None:
        """Render the supplied rating breakdowns."""

        self._clear_rows()
        if not ratings:
            self._layout.addWidget(self._placeholder, alignment=Qt.AlignCenter)
            self._placeholder.show()
            return

        self._placeholder.hide()
        for rating in ratings:
            self._layout.addWidget(self._create_rating_group(rating))

        self._layout.addStretch(1)

    def _create_rating_group(self, rating: RatingBreakdown) -> QWidget:
        container = QWidget(self)
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(8)

        header = QWidget(container)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)

        label = QLabel(rating.label.upper(), header)
        label.setObjectName("RatingLabel")
        value = QLabel(f"{rating.current} / {rating.potential}", header)
        value.setObjectName("RatingValueLabel")

        header_layout.addWidget(label, alignment=Qt.AlignLeft)
        header_layout.addStretch(1)
        header_layout.addWidget(value, alignment=Qt.AlignRight)

        bar = _RatingBar(parent=container)
        bar.set_values(rating.current, rating.potential)

        container_layout.addWidget(header)
        container_layout.addWidget(bar)

        for subrating in rating.subratings:
            container_layout.addWidget(self._create_subrating_widget(subrating))

        return container

    def _create_subrating_widget(self, subrating: RatingBreakdown) -> QWidget:
        row = QWidget(self)
        row_layout = QVBoxLayout(row)
        row_layout.setContentsMargins(16, 0, 0, 0)
        row_layout.setSpacing(4)

        header = QWidget(row)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)

        label = QLabel(subrating.label, header)
        label.setObjectName("SubRatingLabel")
        value = QLabel(f"{subrating.current} / {subrating.potential}", header)
        value.setObjectName("SubRatingValueLabel")

        header_layout.addWidget(label, alignment=Qt.AlignLeft)
        header_layout.addStretch(1)
        header_layout.addWidget(value, alignment=Qt.AlignRight)

        bar = _RatingBar(compact=True, parent=row)
        bar.set_values(subrating.current, subrating.potential)

        row_layout.addWidget(header)
        row_layout.addWidget(bar)

        return row

