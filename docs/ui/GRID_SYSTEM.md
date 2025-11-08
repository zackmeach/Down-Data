# Grid Layout System

## Overview

The Down-Data application uses a **12-column by 24-row responsive grid system** for laying out UI elements. This system ensures that all elements scale proportionally with window size while maintaining consistent spacing and alignment.

## Components

### 1. GridLayoutManager
The main class for positioning widgets in the grid.

```python
from down_data.ui.widgets import GridLayoutManager, GridCell

# Create a grid layout manager
grid = GridLayoutManager(
    parent=my_widget,
    columns=12,
    rows=24
)
```

### 2. GridCell
Represents a position in the grid with optional spanning.

```python
# Single cell
cell = GridCell(col=0, row=0)

# Spanning multiple cells
cell = GridCell(col=1, row=2, col_span=3, row_span=2)
```

## Reference Design

The grid system is calibrated to a reference design size:
- **Reference Width**: 2650px
- **Reference Height**: 1392px (16:9 aspect ratio)
- **Columns**: 12
- **Rows**: 24

### Margins and Gutters

All margins and gutters scale proportionally with window size:

**At Reference Width (2650px):**
- Left/Right Margins: 16px
- Column Gutter: 10px

**At Reference Height (1392px):**
- Row Gutter: 16px
- Top Margin: 0px
- Bottom Margin: 24px

## Usage

### Adding Widgets to the Grid

```python
from down_data.ui.widgets import GridLayoutManager, GridCell
from PySide6.QtWidgets import QPushButton

# Create grid layout
grid = GridLayoutManager(parent=self, columns=12, rows=24)

# Create a widget
button = QPushButton("Click Me", parent=self)

# Position it in the grid (column 2, row 3, spanning 3 columns and 1 row)
cell = GridCell(col=2, row=3, col_span=3, row_span=1)
grid.add_widget(button, cell)
```

### Handling Window Resize

Update the grid layout when the parent widget resizes:

```python
def resizeEvent(self, event):
    super().resizeEvent(event)
    if hasattr(self, '_grid_layout'):
        self._grid_layout.update_layout()
```

### Removing Widgets

```python
grid.remove_widget(my_widget)
```

## Grid Coordinates

- **Columns**: 0-11 (12 columns total)
- **Rows**: 0-23 (24 rows total)
- Both columns and rows are **0-indexed**

### Example Positions

```python
# Top-left corner
GridCell(col=0, row=0)

# Center (approximately)
GridCell(col=5, row=11, col_span=2, row_span=2)

# Full width, single row
GridCell(col=0, row=5, col_span=12, row_span=1)

# Right column, tall element
GridCell(col=11, row=0, col_span=1, row_span=10)
```

## Common Patterns

### Header Bar (Full Width)
```python
header = GridCell(col=0, row=0, col_span=12, row_span=1)
```

### Sidebar (Left)
```python
sidebar = GridCell(col=0, row=1, col_span=2, row_span=23)
```

### Main Content Area
```python
content = GridCell(col=2, row=1, col_span=10, row_span=23)
```

### 3-Column Layout
```python
left = GridCell(col=0, row=0, col_span=4, row_span=24)
center = GridCell(col=4, row=0, col_span=4, row_span=24)
right = GridCell(col=8, row=0, col_span=4, row_span=24)
```

## Demo Boxes

The system includes `GridDemoBox` widgets for testing layouts:

```python
from down_data.ui.widgets import GridDemoBox, GridCell
from PySide6.QtGui import QColor

# Create a colored demo box
demo_box = GridDemoBox(
    color=QColor(100, 150, 200),
    border_color=QColor(50, 75, 100),
    border_width=2,
    label="Test"
)

# Add it to the grid
grid.add_widget(demo_box, GridCell(col=1, row=1, col_span=2, row_span=2))
```

## Communicating Layout Changes

When requesting layout changes, use this format:

```
"Place the search box at column 2, row 3, spanning 8 columns and 1 row"
```

Or:

```
"Move the player list to GridCell(col=0, row=5, col_span=12, row_span=18)"
```

## Best Practices

1. **Maintain Proportions**: Use the grid to maintain consistent proportions across different screen sizes
2. **Align to Grid**: Always align UI elements to grid boundaries for visual consistency
3. **Group Related Elements**: Use spanning to group related UI elements together
4. **Test at Different Sizes**: Resize the window to ensure elements scale properly
5. **Avoid Overlapping**: Don't place widgets in overlapping grid cells

## Debugging

### Quick Toggle (Ctrl+G)

The PlayerSearchPage includes a built-in grid debug overlay that can be toggled:

- Press **Ctrl+G** to show/hide the grid lines
- Grid lines display as cyan lines with red dashed gutters
- Margins are shown as bright yellow lines

### Manual Grid Overlay

To add a grid overlay to other pages during development:

```python
from down_data.ui.widgets import GridOverlay

# Add overlay for debugging (shows grid lines)
overlay = GridOverlay(columns=12, rows=24, parent=self)
overlay.set_mode("both")  # Shows both rows and columns
overlay.setGeometry(self.rect())
overlay.raise_()
```

Available overlay modes:
- `"both"`: Show all grid lines
- `"rows"`: Show only horizontal row lines
- `"columns"`: Show only vertical column lines  
- `"margins"`: Show only margin boundaries with pink boxes

Remember to remove or disable the overlay in production!

### Debug Workflow

1. Launch the app
2. Press **Ctrl+G** to enable grid overlay
3. Resize the window to see how elements scale
4. Use the grid lines to visually verify element placement
5. Press **Ctrl+G** again to disable when done

