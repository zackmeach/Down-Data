"""Table panel widget: panel with embedded table for displaying data."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHeaderView, QTableWidget, QTableWidgetItem, QWidget

from .panel import ContentPanel


class TablePanel(ContentPanel):
    """Reusable panel with an embedded read-only table.
    
    Use this for displaying tabular data with consistent styling.
    Supports customization via parameters rather than subclassing.
    
    Example:
        # Create a simple stats table
        table = TablePanel(
            title="PLAYER STATS",
            columns=["Name", "Team", "Position", "Yards", "TDs"],
            parent=self
        )
        
        # Add data rows
        table.add_row(["Patrick Mahomes", "KC", "QB", "4,839", "37"])
        table.add_row(["Josh Allen", "BUF", "QB", "4,283", "35"])
        
        # Or set all data at once
        table.set_data([
            ["Patrick Mahomes", "KC", "QB", "4,839", "37"],
            ["Josh Allen", "BUF", "QB", "4,283", "35"],
        ])
    """

    def __init__(
        self,
        *,
        title: str | None = None,
        columns: list[str] | None = None,
        sortable: bool = True,
        alternating_rows: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        """Initialize table panel.
        
        Args:
            title: Panel title (displayed at top)
            columns: Column headers (list of strings)
            sortable: Enable column sorting by clicking headers
            alternating_rows: Use alternating row colors for readability
            parent: Parent widget
        """
        super().__init__(title=title, parent=parent)
        
        # Create table widget
        self._table = QTableWidget(self)
        self._table.setObjectName("DataTable")
        
        # Configure table
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)  # Read-only
        self._table.setSelectionBehavior(QTableWidget.SelectRows)  # Select full rows
        self._table.setSelectionMode(QTableWidget.SingleSelection)  # Single row at a time
        self._table.setSortingEnabled(sortable)
        self._table.setAlternatingRowColors(alternating_rows)
        
        # Set columns if provided
        if columns:
            self.set_columns(columns)
        
        # Add table to panel's content layout
        self.content_layout.addWidget(self._table)
    
    # Data Management API -------------------------------------------------------
    
    def set_columns(self, columns: list[str]) -> None:
        """Set column headers.
        
        Args:
            columns: List of column header strings
        """
        self._table.setColumnCount(len(columns))
        self._table.setHorizontalHeaderLabels(columns)
        
        # Auto-resize columns to content
        header = self._table.horizontalHeader()
        header.setStretchLastSection(True)  # Last column stretches
        for i in range(len(columns) - 1):
            header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
    
    def add_row(self, data: list[str]) -> None:
        """Add a single row of data.
        
        Args:
            data: List of cell values (strings)
        """
        row_position = self._table.rowCount()
        self._table.insertRow(row_position)
        
        for col, value in enumerate(data):
            item = QTableWidgetItem(str(value))
            
            # Right-align numeric columns (simple heuristic)
            if self._is_numeric(value):
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            
            self._table.setItem(row_position, col, item)
    
    def set_data(self, rows: list[list[str]]) -> None:
        """Set all table data at once (replaces existing data).
        
        Args:
            rows: List of rows, where each row is a list of cell values
        """
        self.clear_data()
        for row in rows:
            self.add_row(row)
    
    def clear_data(self) -> None:
        """Remove all rows from the table (keeps column headers)."""
        self._table.setRowCount(0)
    
    # Utility Methods -----------------------------------------------------------
    
    def _is_numeric(self, value: str) -> bool:
        """Check if a value looks numeric (for right-alignment)."""
        # Remove common formatting characters
        clean = str(value).replace(",", "").replace("$", "").replace("%", "").strip()
        try:
            float(clean)
            return True
        except (ValueError, AttributeError):
            return False
    
    def set_column_widths(self, widths: list[int]) -> None:
        """Set explicit column widths in pixels.
        
        Args:
            widths: List of widths (one per column)
        """
        for col, width in enumerate(widths):
            self._table.setColumnWidth(col, width)
    
    def resize_columns_to_contents(self) -> None:
        """Auto-resize all columns to fit their content."""
        self._table.resizeColumnsToContents()
    
    # Access to underlying table ------------------------------------------------
    
    @property
    def table(self) -> QTableWidget:
        """Access the underlying QTableWidget for advanced customization."""
        return self._table


# Convenience factory functions for common table types -------------------------

def create_stats_table(
    *,
    title: str,
    stat_columns: list[str],
    parent: QWidget | None = None,
) -> TablePanel:
    """Create a stats table with common configuration.
    
    Args:
        title: Table title
        stat_columns: Column headers (e.g., ["Player", "Team", "Yards", "TDs"])
        parent: Parent widget
    
    Returns:
        Configured TablePanel
    """
    return TablePanel(
        title=title,
        columns=stat_columns,
        sortable=True,
        alternating_rows=True,
        parent=parent,
    )


def create_roster_table(
    *,
    title: str = "ROSTER",
    parent: QWidget | None = None,
) -> TablePanel:
    """Create a roster table with standard NFL columns.
    
    Args:
        title: Table title
        parent: Parent widget
    
    Returns:
        Configured TablePanel
    """
    columns = ["#", "Name", "Position", "Height", "Weight", "Age", "Exp"]
    return TablePanel(
        title=title,
        columns=columns,
        sortable=True,
        alternating_rows=True,
        parent=parent,
    )


def create_results_table(
    *,
    title: str = "MATCHING PLAYERS",
    parent: QWidget | None = None,
) -> TablePanel:
    """Create a search results table.
    
    Args:
        title: Table title
        parent: Parent widget
    
    Returns:
        Configured TablePanel
    """
    columns = ["Name", "Position", "Team", "Age", "College"]
    return TablePanel(
        title=title,
        columns=columns,
        sortable=True,
        alternating_rows=True,
        parent=parent,
    )

