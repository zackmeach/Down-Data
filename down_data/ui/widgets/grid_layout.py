"""Grid layout manager for positioning widgets in a responsive grid system."""

from __future__ import annotations

from PySide6.QtCore import QRect
from PySide6.QtWidgets import QWidget


class GridCell:
    """Represents a position in the grid with optional span."""
    
    def __init__(self, col: int, row: int, col_span: int = 1, row_span: int = 1):
        """Initialize a grid cell position.
        
        Args:
            col: Starting column (0-indexed)
            row: Starting row (0-indexed)
            col_span: Number of columns to span (default: 1)
            row_span: Number of rows to span (default: 1)
        """
        self.col = col
        self.row = row
        self.col_span = col_span
        self.row_span = row_span


class GridLayoutManager:
    """Manages widget positioning in a responsive grid layout.
    
    This manager computes grid cell positions based on a reference design size
    and proportionally scales margins and gutters. Widgets can be placed at
    specific grid cells or span multiple cells.
    
    Reference points:
    - At width = 2650 px → left/right margins = 16 px, column gutter = 10 px
    - At height = 1392 px → row gutter = 16 px, top margin = 0 px, bottom margin = 24 px
    - 12 columns, 24 rows (configurable)
    """
    
    def __init__(
        self,
        parent: QWidget,
        *,
        columns: int = 12,
        rows: int = 24,
        ref_width: int = 2650,
        ref_height: int = 1392,
        base_margin_lr: int = 16,
        base_margin_top: int = 0,
        base_margin_bottom: int = 24,
        base_gutter_col: int = 10,
        base_gutter_row: int = 16,
    ):
        """Initialize the grid layout manager.
        
        Args:
            parent: Parent widget that contains the grid
            columns: Number of columns in the grid
            rows: Number of rows in the grid
            ref_width: Reference width for scaling calculations
            ref_height: Reference height for scaling calculations
            base_margin_lr: Base left/right margin at reference width
            base_margin_top: Base top margin at reference height
            base_margin_bottom: Base bottom margin at reference height
            base_gutter_col: Base column gutter at reference width
            base_gutter_row: Base row gutter at reference height
        """
        self.parent = parent
        self.columns = columns
        self.rows = rows
        
        # Reference design and base measurements
        self.ref_width = ref_width
        self.ref_height = ref_height
        self.base_margin_lr = base_margin_lr
        self.base_margin_top = base_margin_top
        self.base_margin_bottom = base_margin_bottom
        self.base_gutter_col = base_gutter_col
        self.base_gutter_row = base_gutter_row
        
        # Track widgets managed by this layout
        self._managed_widgets: dict[QWidget, GridCell] = {}
    
    def add_widget(self, widget: QWidget, cell: GridCell) -> None:
        """Add a widget to the grid at the specified cell.
        
        Args:
            widget: Widget to add to the grid
            cell: Grid cell position and span
        """
        widget.setParent(self.parent)
        self._managed_widgets[widget] = cell
        self._update_widget_geometry(widget, cell)
    
    def remove_widget(self, widget: QWidget) -> None:
        """Remove a widget from the grid.
        
        Args:
            widget: Widget to remove
        """
        if widget in self._managed_widgets:
            del self._managed_widgets[widget]
    
    def update_layout(self) -> None:
        """Update all widget positions based on current parent size."""
        for widget, cell in self._managed_widgets.items():
            self._update_widget_geometry(widget, cell)
    
    def get_cell_rect(self, cell: GridCell) -> QRect:
        """Get the rectangle for a grid cell in parent coordinates.
        
        Args:
            cell: Grid cell position and span
            
        Returns:
            QRect representing the cell bounds
        """
        width = self.parent.width()
        height = self.parent.height()
        metrics = self._compute_metrics(width, height)
        
        # Validate cell position
        if cell.col < 0 or cell.col >= self.columns:
            raise ValueError(f"Column {cell.col} out of range [0, {self.columns})")
        if cell.row < 0 or cell.row >= self.rows:
            raise ValueError(f"Row {cell.row} out of range [0, {self.rows})")
        if cell.col + cell.col_span > self.columns:
            raise ValueError(f"Column span {cell.col_span} exceeds grid at column {cell.col}")
        if cell.row + cell.row_span > self.rows:
            raise ValueError(f"Row span {cell.row_span} exceeds grid at row {cell.row}")
        
        # Calculate cell position
        col_w = metrics["col_w"]
        row_h = metrics["row_h"]
        gutter_col = metrics["gutter_col"]
        gutter_row = metrics["gutter_row"]
        margin_lr = metrics["margin_lr"]
        margin_top = metrics["margin_top"]
        
        # Calculate x position and width
        x = margin_lr + (cell.col * (col_w + gutter_col))
        w = (cell.col_span * col_w) + ((cell.col_span - 1) * gutter_col)
        
        # Calculate y position and height
        y = margin_top + (cell.row * (row_h + gutter_row))
        h = (cell.row_span * row_h) + ((cell.row_span - 1) * gutter_row)
        
        return QRect(int(x), int(y), int(w), int(h))
    
    def _update_widget_geometry(self, widget: QWidget, cell: GridCell) -> None:
        """Update a widget's geometry to match its grid cell."""
        rect = self.get_cell_rect(cell)
        widget.setGeometry(rect)
    
    def _compute_metrics(self, width: int, height: int) -> dict:
        """Compute scaled margins, gutters, and cell sizes for the current size.
        
        This matches the GridOverlay's calculation logic to ensure consistency.
        """
        # Proportional scaling from the reference size
        margin_lr = max(1, int(round(self.base_margin_lr * (width / self.ref_width))))
        gutter_col = max(1, int(round(self.base_gutter_col * (width / self.ref_width))))
        gutter_row = max(1, int(round(self.base_gutter_row * (height / self.ref_height))))
        
        # Horizontal sizing
        available_w = max(0, width - (2 * margin_lr) - ((self.columns - 1) * gutter_col))
        col_w = available_w / self.columns if self.columns > 0 else 0.0
        
        # Vertical sizing
        available_h = max(0, height - ((self.rows - 1) * gutter_row))
        row_h = available_h / self.rows if self.rows > 0 else 0.0
        margin_top = max(0, int(round(self.base_margin_top * (height / self.ref_height))))
        margin_bottom = max(1, int(round(self.base_margin_bottom * (height / self.ref_height))))
        
        return {
            "margin_lr": margin_lr,
            "margin_top": margin_top,
            "margin_bottom": margin_bottom,
            "gutter_col": gutter_col,
            "gutter_row": gutter_row,
            "col_w": col_w,
            "row_h": row_h,
        }

