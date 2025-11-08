# Panel Architecture

## Overview

The panel system provides a structured approach to creating visually distinct content areas in the OOTP-inspired UI. Panels are **darker** than the main background to create depth and visual hierarchy.

## Visual Hierarchy

```
Main Background (#2B2B2B - lighter gray)
  └─ Panel (#1E1E1E - darker gray)
      └─ Content (widgets inside panel)
```

This **inverted** approach (panels darker than background) creates the "recessed" look seen in OOTP, where panels appear to sink into the interface.

## Panel Class Structure

### Base Class: `Panel`

The foundational panel class with customizable object names for styling.

```python
from down_data.ui.widgets import Panel

# Basic panel
panel = Panel(title="MY PANEL", parent=self)

# Custom styled panel
panel = Panel(title="CUSTOM", parent=self, object_name="CustomPanel")
```

**Features:**
- Optional title label (auto-uppercase, styled)
- Configurable object name for CSS targeting
- Built-in layout with consistent margins/spacing
- `content_layout` property for adding child widgets

### Specialized Subclasses

Pre-configured panel types for common use cases:

#### 1. **FilterPanel** (left sidebar)
```python
from down_data.ui.widgets import FilterPanel

filter_panel = FilterPanel(title="FIND PLAYER", parent=self)
filter_panel.content_layout.addWidget(search_input)
filter_panel.content_layout.addWidget(filters_section)
```

**Use for:** Search controls, filter criteria, options

#### 2. **ContentPanel** (center area)
```python
from down_data.ui.widgets import ContentPanel

content_panel = ContentPanel(title="MATCHING PLAYERS", parent=self)
content_panel.content_layout.addWidget(results_table)
```

**Use for:** Main data display, tables, forms, primary content

#### 3. **DetailPanel** (right sidebar)
```python
from down_data.ui.widgets import DetailPanel

detail_panel = DetailPanel(title="PLAYER PREVIEW", parent=self)
detail_panel.content_layout.addWidget(preview_card)
```

**Use for:** Previews, summaries, contextual information

## Inheritance vs. Composition

### Approach Used: **Composition with Specialization**

We use a **hybrid approach**:

1. **Base `Panel` class** provides core functionality
2. **Specialized subclasses** (`FilterPanel`, `ContentPanel`, `DetailPanel`) set object names for styling
3. **Styling via QSS** targets object names for visual customization

### Why This Approach?

✅ **Advantages:**
- Simple inheritance hierarchy (only one level)
- Easy to add new panel types (just subclass and set object name)
- Styling centralized in QSS (no Python color code)
- Flexible: can use base `Panel` with custom object names
- Type safety: `FilterPanel` is clearly a filter panel

❌ **Alternatives Considered:**

**Option A: Deep inheritance with hardcoded styles**
```python
class FilterPanel(Panel):
    def __init__(self):
        super().__init__()
        self.setStyleSheet("background: #1E1E1E; border: 1px solid #404040;")
```
- ❌ Styles scattered across Python files
- ❌ Hard to maintain consistent theme
- ❌ Can't easily change colors globally

**Option B: Composition with wrapper classes**
```python
class FilterPanel:
    def __init__(self):
        self._panel = Panel()
        self._panel.setObjectName("FilterPanel")
```
- ❌ More boilerplate code
- ❌ Loses `isinstance(panel, Panel)` checks
- ❌ Need to expose all Panel methods

**Option C: Factory functions**
```python
def create_filter_panel(title):
    panel = Panel(title=title)
    panel.setObjectName("FilterPanel")
    return panel
```
- ❌ No type hints for specialized panels
- ❌ Less discoverable (not in class hierarchy)
- ✅ Would work, but less Pythonic

## Styling System

### QSS Targeting

Panels are styled via object names in `styles.py`:

```css
/* Base panel */
QFrame#Panel {
    background-color: #1E1E1E;
    border: 1px solid #404040;
    border-radius: 2px;
}

/* Filter panel (can override base) */
QFrame#FilterPanel {
    background-color: #1E1E1E;
    border: 1px solid #404040;
    border-radius: 2px;
}

/* Panel titles */
QLabel#PanelTitle {
    color: #C6CED6;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.5px;
}
```

### Creating Custom Panel Styles

**Option 1: Subclass with custom object name**
```python
class MySpecialPanel(Panel):
    def __init__(self, *, title=None, parent=None):
        super().__init__(title=title, parent=parent, object_name="MySpecialPanel")
```

Then add to `styles.py`:
```css
QFrame#MySpecialPanel {
    background-color: #2A2A2A;  /* Different shade */
    border: 2px solid #5A5A5A;  /* Thicker border */
}
```

**Option 2: Use base Panel with custom object name**
```python
panel = Panel(title="CUSTOM", parent=self, object_name="CustomPanel")
```

## Usage Patterns

### Pattern 1: Simple Panel
```python
panel = Panel(title="SECTION NAME", parent=self)
panel.content_layout.addWidget(my_widget)
```

### Pattern 2: Specialized Panel
```python
filter_panel = FilterPanel(title="FILTERS", parent=self)
filter_panel.content_layout.addWidget(search_box)
filter_panel.content_layout.addWidget(dropdown)
```

### Pattern 3: Panel with Grid Layout
```python
filter_panel = FilterPanel(title="FIND PLAYER", parent=self)

# Position in grid
grid_layout.add_widget(
    filter_panel,
    GridCell(col=1, row=6, col_span=3, row_span=18)
)

# Add content to panel
filter_panel.content_layout.addWidget(search_controls)
```

### Pattern 4: Nested Sections
```python
panel = ContentPanel(title="PLAYER STATS", parent=self)

# Add sections inside panel
offense_section = QGroupBox("OFFENSE")
offense_layout = QVBoxLayout()
offense_section.setLayout(offense_layout)
panel.content_layout.addWidget(offense_section)

defense_section = QGroupBox("DEFENSE")
defense_layout = QVBoxLayout()
defense_section.setLayout(defense_layout)
panel.content_layout.addWidget(defense_section)
```

## Best Practices

### DO:
✅ Use specialized panel types (`FilterPanel`, `ContentPanel`, `DetailPanel`)  
✅ Set meaningful titles for all panels  
✅ Use `content_layout` to add widgets  
✅ Position panels using GridLayoutManager  
✅ Create custom panel subclasses for reusable panel types  
✅ Style panels via QSS object names  

### DON'T:
❌ Hardcode colors in Python (use QSS)  
❌ Create deep inheritance hierarchies  
❌ Mix panel types (don't put filter controls in ContentPanel)  
❌ Forget to set object names for custom styling  
❌ Use panels for single widgets (just add widget directly)  
❌ Nest panels more than 2 levels deep  

## Color Reference

```python
# Main background (lighter)
BACKGROUND_MAIN = "#2B2B2B"

# Panel background (darker - creates depth)
PANEL_BG = "#1E1E1E"

# Panel border
PANEL_BORDER = "#404040"

# Panel title text
PANEL_TITLE_COLOR = "#C6CED6"
```

## Example: Complete Filter Panel

```python
from down_data.ui.widgets import FilterPanel, GridCell, GridLayoutManager
from PySide6.QtWidgets import QLineEdit, QComboBox, QPushButton, QVBoxLayout

class MyPage(SectionPage):
    def __init__(self, *, service, parent=None):
        super().__init__(title="My Page", parent=parent)
        
        # Create grid
        self._grid = GridLayoutManager(parent=self, columns=12, rows=24)
        
        # Create filter panel
        self._filter_panel = FilterPanel(title="SEARCH FILTERS", parent=self)
        
        # Add controls to panel
        search = QLineEdit()
        search.setPlaceholderText("Search...")
        self._filter_panel.content_layout.addWidget(search)
        
        position = QComboBox()
        position.addItems(["All", "QB", "RB", "WR"])
        self._filter_panel.content_layout.addWidget(position)
        
        search_btn = QPushButton("SEARCH")
        self._filter_panel.content_layout.addWidget(search_btn)
        
        # Position in grid (left sidebar)
        self._grid.add_widget(
            self._filter_panel,
            GridCell(col=1, row=6, col_span=3, row_span=18)
        )
```

## Summary

The panel system provides:
- **Clear visual hierarchy** through darker panels on lighter background
- **Flexible customization** via inheritance and object names
- **Consistent styling** through centralized QSS
- **Type safety** with specialized panel classes
- **Reusability** through base Panel class

This architecture balances simplicity (shallow inheritance) with flexibility (custom object names and QSS styling).

