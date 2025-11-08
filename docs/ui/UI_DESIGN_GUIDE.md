# UI Design Guide: Panel-Based Layout System

## Overview

This guide defines the terminology, patterns, and best practices for building UI components in the Down-Data application, based on the OOTP Baseball interface design.

## Core Concept: Panel-Based Layout

The OOTP interface uses a **panel-based** design where content is organized into distinct rectangular regions with clear visual boundaries. Each panel serves a specific purpose and contains related UI elements.

### Visual Characteristics
- **Dark background** (#2B2B2B) with slightly lighter panels (#252525, #2F2F2F)
- **Subtle borders** (#404040, 1px) to define panel boundaries
- **Consistent spacing** (8-16px padding inside panels, 8-12px gaps between panels)
- **Hierarchical depth** through background color variations

## Terminology

### 1. **Panel** (QFrame with styling)
A rectangular container with a background and border that groups related content.

**Types:**
- **Filter Panel**: Left sidebar containing search/filter controls
- **Content Panel**: Main area displaying data (tables, forms, details)
- **Detail Panel**: Right sidebar showing summary or contextual information
- **Section Panel**: Subdivisions within a larger panel (e.g., "RATINGS", "STATS")

**Qt Implementation:**
```python
from PySide6.QtWidgets import QFrame, QVBoxLayout

class Panel(QFrame):
    """Base panel widget with OOTP styling."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("Panel")
        self.setFrameShape(QFrame.StyledPanel)
        # Styling applied via QSS
```

**QSS Styling:**
```css
QFrame#Panel {
    background-color: #252525;
    border: 1px solid #404040;
    border-radius: 2px;
}
```

### 2. **Section** (QGroupBox or labeled QFrame)
A titled subdivision within a panel, using a header label and contained content.

**Example from OOTP:** "FIND PLAYER" section with filters, "RATINGS" section with sliders

**Qt Implementation:**
```python
from PySide6.QtWidgets import QGroupBox

section = QGroupBox("RATINGS")
section.setObjectName("Section")
```

**QSS Styling:**
```css
QGroupBox#Section {
    background-color: transparent;
    border: 1px solid #404040;
    border-radius: 2px;
    margin-top: 12px;
    padding-top: 8px;
    font-weight: 600;
    color: #C6CED6;
}

QGroupBox#Section::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 8px;
    color: #FFFFFF;
}
```

### 3. **Filter Panel** (left sidebar in Find Player)
A scrollable panel containing search criteria, filters, and action buttons.

**Components:**
- Search input fields
- Dropdown filters (QComboBox)
- Range selectors (QSpinBox pairs)
- Checkboxes for boolean options
- Sliders for rating filters
- Action buttons (RESET FILTERS, SEARCH)

**Layout Pattern:**
```python
from PySide6.QtWidgets import QScrollArea, QVBoxLayout, QWidget

filter_panel = QFrame()
filter_panel.setObjectName("FilterPanel")

scroll = QScrollArea()
scroll.setWidgetResizable(True)
scroll.setWidget(filter_panel)

layout = QVBoxLayout(filter_panel)
layout.setSpacing(12)
layout.setContentsMargins(16, 16, 16, 16)

# Add filter sections
layout.addWidget(create_search_section())
layout.addWidget(create_age_section())
layout.addWidget(create_ratings_section())
layout.addStretch()  # Push buttons to bottom
layout.addWidget(create_action_buttons())
```

### 4. **Content Panel** (center/main area)
The primary display area showing tables, forms, or detailed information.

**Common Patterns:**
- **Data Table**: QTableView with sortable columns
- **Form Layout**: QFormLayout with label-value pairs
- **Detail View**: Mixed layout with headers, stats, and sub-sections

**Example:**
```python
content_panel = QFrame()
content_panel.setObjectName("ContentPanel")

layout = QVBoxLayout(content_panel)
layout.setContentsMargins(16, 16, 16, 16)

# Header
header = QLabel("MATCHING PLAYERS")
header.setObjectName("PanelHeader")
layout.addWidget(header)

# Search bar
search = QLineEdit()
search.setPlaceholderText("SEARCH")
layout.addWidget(search)

# Results table
table = QTableView()
layout.addWidget(table)
```

### 5. **Splitter** (QSplitter)
Resizable divider between panels, allowing users to adjust panel widths.

**Usage:**
```python
from PySide6.QtWidgets import QSplitter
from PySide6.QtCore import Qt

splitter = QSplitter(Qt.Horizontal)
splitter.addWidget(filter_panel)
splitter.addWidget(content_panel)
splitter.addWidget(detail_panel)

# Set initial sizes (proportional)
splitter.setSizes([300, 800, 250])  # Left: 300px, Center: 800px, Right: 250px
```

## Layout Patterns

### Pattern 1: Three-Column Layout (Filter | Content | Detail)

**Used in:** Find Player screen

```
┌─────────────┬──────────────────────────┬─────────────┐
│   FILTER    │       CONTENT            │   DETAIL    │
│   PANEL     │       PANEL              │   PANEL     │
│             │                          │             │
│ [Search]    │ MATCHING PLAYERS         │ [Preview]   │
│ [Filters]   │ ┌──────────────────────┐ │ [Stats]     │
│ [Options]   │ │  Results Table       │ │             │
│             │ │                      │ │             │
│ [Reset]     │ │                      │ │             │
│ [Search]    │ └──────────────────────┘ │             │
└─────────────┴──────────────────────────┴─────────────┘
```

**Implementation:**
```python
def create_three_column_layout(self):
    splitter = QSplitter(Qt.Horizontal)
    
    # Left: Filter panel (25%)
    filter_panel = self.create_filter_panel()
    splitter.addWidget(filter_panel)
    
    # Center: Content panel (60%)
    content_panel = self.create_content_panel()
    splitter.addWidget(content_panel)
    
    # Right: Detail panel (15%)
    detail_panel = self.create_detail_panel()
    splitter.addWidget(detail_panel)
    
    splitter.setSizes([300, 900, 250])
    return splitter
```

### Pattern 2: Two-Column Layout (Sidebar | Content)

**Used in:** Player profile pages

```
┌─────────────┬──────────────────────────────────────┐
│  SIDEBAR    │         MAIN CONTENT                 │
│             │                                      │
│ [Photo]     │ ┌──────────────────────────────────┐ │
│             │ │  Stats Table                     │ │
│ Personal    │ │                                  │ │
│ Details     │ └──────────────────────────────────┘ │
│             │                                      │
│ Status      │ ┌──────────────────────────────────┐ │
│             │ │  Career History                  │ │
│             │ │                                  │ │
│             │ └──────────────────────────────────┘ │
└─────────────┴──────────────────────────────────────┘
```

### Pattern 3: Stacked Sections (vertical panels)

**Used in:** Forms, detailed views

```
┌─────────────────────────────────────────────────┐
│  SECTION 1: PERSONAL DETAILS                    │
│  ┌───────────────────────────────────────────┐  │
│  │ Name:     John Smith                      │  │
│  │ Age:      28                              │  │
│  │ Position: QB                              │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────┐
│  SECTION 2: CURRENT SEASON                      │
│  ┌───────────────────────────────────────────┐  │
│  │ Games:    16                              │  │
│  │ Yards:    4,500                           │  │
│  │ TDs:      35                              │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

## Component Guidelines

### Tables (QTableView)

**Styling:**
- Alternating row colors (#2B2B2B / #303030)
- Gridlines (#404040, 1px)
- Header background (#1E1E1E)
- Selection highlight (#2A8CA5 with 30% opacity)

**Best Practices:**
- Use QAbstractTableModel for large datasets (>100 rows)
- Enable sorting on all columns
- Right-align numeric columns
- Add tooltips for truncated text

### Forms (QFormLayout)

**Pattern:**
```python
form = QFormLayout()
form.setLabelAlignment(Qt.AlignRight | Qt.AlignVCenter)
form.setSpacing(8)

form.addRow("Name:", QLabel("John Smith"))
form.addRow("Position:", QLabel("QB"))
form.addRow("Team:", QLabel("Kansas City Chiefs"))
```

### Buttons

**Types:**
- **Primary Action**: Green background (#00FF00), black text - for main actions (SEARCH, CONTINUE)
- **Secondary Action**: Gray background (#404040), white text - for auxiliary actions (RESET, CANCEL)
- **Danger Action**: Red background (#F44336), white text - for destructive actions (DELETE)

### Input Fields

**Styling:**
- Background: #404040
- Border: 1px solid #555555
- Focus border: 1px solid #2A8CA5
- Placeholder text: #808080

## Spacing System

Use consistent spacing based on 4px increments:

- **4px**: Tight spacing (between related labels)
- **8px**: Default spacing (between form fields, buttons)
- **12px**: Section spacing (between filter groups)
- **16px**: Panel padding (inside panels)
- **24px**: Large gaps (between major sections)

## Color Palette Reference

```python
# Backgrounds
BACKGROUND_MAIN = "#2B2B2B"      # Main window background
PANEL_BG = "#252525"              # Panel background
PANEL_ALT_BG = "#2F2F2F"          # Alternate panel background
INPUT_BG = "#404040"              # Input fields, dropdowns

# Borders
BORDER_COLOR = "#404040"          # Standard borders
BORDER_LIGHT = "#555555"          # Lighter borders (inputs)

# Text
TEXT_PRIMARY = "#FFFFFF"          # Primary text
TEXT_SECONDARY = "#C6CED6"        # Secondary text
TEXT_DISABLED = "#808080"         # Disabled text

# Accents
ACCENT_PRIMARY = "#2A8CA5"        # Teal (selections, highlights)
ACCENT_SUCCESS = "#4CAF50"        # Green (success, positive)
ACCENT_WARNING = "#FF9800"        # Orange (warnings)
ACCENT_ERROR = "#F44336"          # Red (errors, negative)
ACCENT_CONTINUE = "#00FF00"       # Bright green (CONTINUE button)
```

## Creating a New Page

### Step-by-Step Process

1. **Inherit from SectionPage**
```python
from down_data.ui.pages.base_page import SectionPage

class MyNewPage(SectionPage):
    def __init__(self, *, service, parent=None):
        super().__init__(title="My New Page", parent=parent)
        self._service = service
        self._build_ui()
```

2. **Choose a Layout Pattern**
- Three-column for search/filter pages
- Two-column for detail pages
- Stacked sections for forms

3. **Create Panels**
```python
def _build_ui(self):
    # Main container
    container = QWidget()
    layout = QHBoxLayout(container)
    
    # Left panel
    left_panel = self._create_filter_panel()
    layout.addWidget(left_panel, 1)  # Stretch factor 1
    
    # Right panel
    right_panel = self._create_content_panel()
    layout.addWidget(right_panel, 3)  # Stretch factor 3
    
    self.root_layout.addWidget(container)
```

4. **Style Panels**
```python
def _create_filter_panel(self):
    panel = QFrame()
    panel.setObjectName("Panel")
    
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(16, 16, 16, 16)
    layout.setSpacing(12)
    
    # Add content...
    
    return panel
```

5. **Add to Navigation**
- Update `NAVIGATION_MAP` in `content_page.py`
- Instantiate in `_build_content_area()`

## Best Practices

### DO:
✓ Use panels to group related content  
✓ Maintain consistent spacing (4px increments)  
✓ Apply object names for QSS targeting  
✓ Use QSplitter for resizable layouts  
✓ Right-align numeric data in tables  
✓ Add tooltips for complex UI elements  
✓ Use QFormLayout for label-value pairs  
✓ Keep panels scrollable if content might overflow  

### DON'T:
✗ Mix layout patterns on the same page  
✗ Use arbitrary spacing values  
✗ Create deeply nested layouts (>3 levels)  
✗ Hardcode colors (use QSS or constants)  
✗ Forget to set object names for styling  
✗ Make panels too wide (>1200px without splitters)  
✗ Use QTableWidget for large datasets (use QTableView + model)  

## Example: Complete Filter Panel

```python
def create_filter_panel(self):
    """Create a complete filter panel following OOTP style."""
    panel = QFrame()
    panel.setObjectName("FilterPanel")
    
    # Make it scrollable
    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(16, 16, 16, 16)
    layout.setSpacing(12)
    
    # Search section
    search_section = QGroupBox("SEARCH")
    search_layout = QVBoxLayout()
    self.search_input = QLineEdit()
    self.search_input.setPlaceholderText("Enter player name...")
    search_layout.addWidget(self.search_input)
    search_section.setLayout(search_layout)
    layout.addWidget(search_section)
    
    # Position section
    position_section = QGroupBox("POSITION")
    position_layout = QVBoxLayout()
    self.position_combo = QComboBox()
    self.position_combo.addItems(["All", "QB", "RB", "WR", "TE"])
    position_layout.addWidget(self.position_combo)
    position_section.setLayout(position_layout)
    layout.addWidget(position_section)
    
    # Age section
    age_section = QGroupBox("AGE RANGE")
    age_layout = QHBoxLayout()
    self.age_min = QSpinBox()
    self.age_min.setRange(0, 50)
    self.age_max = QSpinBox()
    self.age_max.setRange(0, 50)
    self.age_max.setValue(50)
    age_layout.addWidget(QLabel("From:"))
    age_layout.addWidget(self.age_min)
    age_layout.addWidget(QLabel("To:"))
    age_layout.addWidget(self.age_max)
    age_section.setLayout(age_layout)
    layout.addWidget(age_section)
    
    # Spacer to push buttons to bottom
    layout.addStretch()
    
    # Action buttons
    button_layout = QHBoxLayout()
    reset_btn = QPushButton("RESET FILTERS")
    search_btn = QPushButton("SEARCH")
    search_btn.setObjectName("PrimaryButton")
    button_layout.addWidget(reset_btn)
    button_layout.addWidget(search_btn)
    layout.addLayout(button_layout)
    
    scroll.setWidget(panel)
    return scroll
```

## Summary

The OOTP-inspired panel-based design provides:
- **Clear visual hierarchy** through panel boundaries
- **Flexible layouts** using splitters and nested panels
- **Consistent styling** via QSS and object names
- **Reusable patterns** for common UI scenarios

Use this guide as a reference when building new pages to maintain consistency across the application.

