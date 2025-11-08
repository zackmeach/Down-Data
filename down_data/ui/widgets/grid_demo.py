"""Demo widgets for testing the grid layout system."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPainter
from PySide6.QtWidgets import QWidget


class GridDemoBox(QWidget):
    """A simple colored box widget for testing grid layout.
    
    This widget displays a colored rectangle with a border to visualize
    how elements fit within the grid system.
    """
    
    def __init__(
        self,
        *,
        color: QColor = QColor(100, 150, 200),
        border_color: QColor = QColor(50, 75, 100),
        border_width: int = 2,
        label: str = "",
        parent: QWidget | None = None,
    ):
        """Initialize a demo box.
        
        Args:
            color: Fill color for the box
            border_color: Border color
            border_width: Width of the border in pixels
            label: Optional label text (currently not displayed)
            parent: Parent widget
        """
        super().__init__(parent)
        self.color = color
        self.border_color = border_color
        self.border_width = border_width
        self.label = label
        
        # Make the widget visible
        self.setAttribute(Qt.WA_StyledBackground, True)
    
    def paintEvent(self, event) -> None:  # type: ignore[override]
        """Draw the demo box."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # Fill the box
        painter.fillRect(self.rect(), self.color)
        
        # Draw border
        if self.border_width > 0:
            painter.setPen(self.border_color)
            for i in range(self.border_width):
                painter.drawRect(
                    i, i,
                    self.width() - 2 * i - 1,
                    self.height() - 2 * i - 1
                )


def create_demo_boxes() -> list[tuple[GridDemoBox, str]]:
    """Create a set of demo boxes with different colors for testing.
    
    Returns:
        List of tuples (widget, description) for each demo box
    """
    boxes = [
        # Top left - small box (like the green one at col 1-2, row 1-3)
        (
            GridDemoBox(
                color=QColor(76, 175, 80, 180),  # Green
                border_color=QColor(56, 142, 60),
                border_width=3,
                label="A"
            ),
            "col=1, row=1, span=2x3"
        ),
        # Top center - horizontal span (like green at col 4-8, row 4)
        (
            GridDemoBox(
                color=QColor(76, 175, 80, 180),  # Green
                border_color=QColor(56, 142, 60),
                border_width=3,
                label="B"
            ),
            "col=4, row=4, span=5x1"
        ),
        # Right side - tall box (like green at col 11, row 1-7)
        (
            GridDemoBox(
                color=QColor(76, 175, 80, 180),  # Green
                border_color=QColor(56, 142, 60),
                border_width=3,
                label="C"
            ),
            "col=11, row=1, span=1x7"
        ),
        # Bottom left - large block (like green at col 1-5, row=10-14)
        (
            GridDemoBox(
                color=QColor(76, 175, 80, 180),  # Green
                border_color=QColor(56, 142, 60),
                border_width=3,
                label="D"
            ),
            "col=1, row=10, span=5x5"
        ),
        # Bottom right - wide block (like green at col 8-11, row=11-16)
        (
            GridDemoBox(
                color=QColor(76, 175, 80, 180),  # Green
                border_color=QColor(56, 142, 60),
                border_width=3,
                label="E"
            ),
            "col=8, row=11, span=4x6"
        ),
    ]
    
    return boxes

