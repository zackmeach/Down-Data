# Component Reusability Guide

## The Question: Custom Component vs. Parameterized Component?

When you need similar UI elements with different data/dimensions/titles, should you:
- **A)** Create unique UI elements for each use case?
- **B)** Create a single reusable component with parameters?

**Answer: Almost always B (parameterized component)**

## Why Parameterized Components Win

### ✅ Advantages

1. **DRY Principle** - Don't Repeat Yourself
   - Write the logic once, use it everywhere
   - Bug fixes apply to all instances automatically

2. **Consistent Behavior**
   - All tables sort the same way
   - All tables have the same keyboard shortcuts
   - All tables look identical (unless intentionally customized)

3. **Easy Maintenance**
   - Change styling in one place
   - Add features to all tables at once
   - Refactor without hunting down duplicates

4. **Smaller Codebase**
   - Less code to read and understand
   - Faster compile/load times
   - Easier onboarding for new developers

5. **Flexible Customization**
   - Parameters for common variations
   - Override methods for special cases
   - Compose with other components

### ❌ When Custom Components Make Sense

Only create separate components when:

1. **Fundamentally Different Behavior**
   - One table is editable, another is read-only with special interactions
   - Different keyboard shortcuts or mouse behaviors
   - Complex state management that differs significantly

2. **Performance Critical**
   - One table needs virtual scrolling for 100k rows
   - Another needs real-time updates every 100ms
   - Optimization would complicate the shared component

3. **Completely Different Structure**
   - One is a simple table, another is a tree-table hybrid
   - Visual layout is fundamentally different
   - Sharing code would require excessive conditionals

## Our Approach: TablePanel

We created `TablePanel` - a reusable component for read-only tables.

### Basic Usage

```python
from down_data.ui.widgets import TablePanel

# Create a table
table = TablePanel(
    title="PLAYER STATS",
    columns=["Name", "Team", "Yards", "TDs"],
    parent=self
)

# Add data
table.add_row(["Patrick Mahomes", "KC", "4,839", "37"])
table.add_row(["Josh Allen", "BUF", "4,283", "35"])

# Or set all at once
table.set_data([
    ["Patrick Mahomes", "KC", "4,839", "37"],
    ["Josh Allen", "BUF", "4,283", "35"],
])
```

### Customization via Parameters

```python
# Disable sorting
table = TablePanel(
    title="LOCKED ORDER",
    columns=["Rank", "Player", "Score"],
    sortable=False,  # Can't reorder
    parent=self
)

# No alternating rows
table = TablePanel(
    title="SIMPLE TABLE",
    columns=["A", "B", "C"],
    alternating_rows=False,  # Uniform background
    parent=self
)
```

### Factory Functions for Common Cases

```python
from down_data.ui.widgets import create_stats_table, create_roster_table

# Pre-configured stats table
stats = create_stats_table(
    title="SEASON STATS",
    stat_columns=["Player", "Yards", "TDs", "Rating"],
    parent=self
)

# Pre-configured roster table (columns already set)
roster = create_roster_table(title="ACTIVE ROSTER", parent=self)
roster.add_row(["15", "Patrick Mahomes", "QB", "6-3", "230", "29", "8"])
```

### Advanced Customization

For special cases, access the underlying table:

```python
table = TablePanel(title="CUSTOM", columns=["A", "B"], parent=self)

# Access underlying QTableWidget
table.table.setColumnWidth(0, 200)  # Set specific width
table.table.setSelectionMode(QTableWidget.MultiSelection)  # Multi-select

# Or use built-in helper methods
table.set_column_widths([200, 150, 300])
table.resize_columns_to_contents()
```

## Decision Tree

```
Do you need a table with data?
│
├─ YES → Is the behavior fundamentally different from existing tables?
│        │
│        ├─ NO → Use TablePanel with parameters ✅
│        │
│        └─ YES → Is the difference just configuration?
│                 │
│                 ├─ YES → Add parameter to TablePanel ✅
│                 │
│                 └─ NO → Create custom component ⚠️
│                          (Document why it's different)
│
└─ NO → Use appropriate existing component
```

## Real Examples

### ❌ BAD: Creating Separate Components

```python
# DON'T DO THIS
class PlayerStatsTable(ContentPanel):
    def __init__(self, parent=None):
        super().__init__(title="PLAYER STATS", parent=parent)
        self._table = QTableWidget()
        self._table.setColumnCount(4)
        # ... 50 lines of setup ...

class TeamStatsTable(ContentPanel):
    def __init__(self, parent=None):
        super().__init__(title="TEAM STATS", parent=parent)
        self._table = QTableWidget()
        self._table.setColumnCount(5)
        # ... 50 lines of nearly identical setup ...

class GameLogTable(ContentPanel):
    def __init__(self, parent=None):
        super().__init__(title="GAME LOG", parent=parent)
        self._table = QTableWidget()
        self._table.setColumnCount(6)
        # ... 50 lines of nearly identical setup ...
```

**Problems:**
- 150+ lines of duplicated code
- Bug fix requires changing 3 files
- Inconsistent behavior between tables
- Hard to add new table types

### ✅ GOOD: Parameterized Component

```python
# DO THIS
from down_data.ui.widgets import TablePanel

# Player stats
player_stats = TablePanel(
    title="PLAYER STATS",
    columns=["Name", "Team", "Yards", "TDs"],
    parent=self
)

# Team stats
team_stats = TablePanel(
    title="TEAM STATS",
    columns=["Team", "Wins", "Losses", "Points", "Yards"],
    parent=self
)

# Game log
game_log = TablePanel(
    title="GAME LOG",
    columns=["Date", "Opponent", "Result", "Yards", "TDs", "Rating"],
    parent=self
)
```

**Benefits:**
- 15 lines instead of 150+
- All tables share the same tested code
- Consistent behavior everywhere
- Easy to add new tables

## When to Extend TablePanel

If you find yourself needing the same customization repeatedly:

### Option 1: Add Parameter

```python
# Add to TablePanel.__init__
def __init__(
    self,
    *,
    title: Optional[str] = None,
    columns: Optional[List[str]] = None,
    sortable: bool = True,
    alternating_rows: bool = True,
    show_row_numbers: bool = False,  # NEW PARAMETER
    parent: QWidget | None = None,
):
    # ... existing code ...
    
    if show_row_numbers:
        self._table.verticalHeader().setVisible(True)
    else:
        self._table.verticalHeader().setVisible(False)
```

### Option 2: Create Subclass

```python
class EditableTablePanel(TablePanel):
    """Table panel that allows editing."""
    
    def __init__(self, *, title=None, columns=None, parent=None):
        super().__init__(title=title, columns=columns, parent=parent)
        
        # Override read-only setting
        self.table.setEditTriggers(QTableWidget.DoubleClicked)
        
    def get_edited_data(self) -> List[List[str]]:
        """Get all data including edits."""
        # Custom method for editable tables
        ...
```

### Option 3: Factory Function

```python
def create_editable_stats_table(*, title, columns, parent=None):
    """Create an editable stats table."""
    table = TablePanel(title=title, columns=columns, parent=parent)
    table.table.setEditTriggers(QTableWidget.DoubleClicked)
    return table
```

## Summary

**Default to parameterized components:**
- `TablePanel` for tables
- `Panel` for generic containers
- `FilterPanel` for filter sidebars

**Create custom components only when:**
- Behavior is fundamentally different
- Performance requires specialization
- Structure is completely different

**The test:**
> "If I fix a bug in one, should it fix in all?"
> - YES → Use parameterized component
> - NO → Consider custom component

## Your Specific Case: Read-Only Tables

For your read-only tables with different dimensions/data/titles:

```python
# ✅ Use TablePanel everywhere
from down_data.ui.widgets import TablePanel

# Small table
small = TablePanel(
    title="TOP 5 PLAYERS",
    columns=["Rank", "Name", "Score"],
    parent=self
)

# Wide table
wide = TablePanel(
    title="DETAILED STATS",
    columns=["Name", "Team", "G", "Att", "Comp", "Yds", "TD", "INT", "Rate"],
    parent=self
)

# Tall table
tall = TablePanel(
    title="ALL PLAYERS",
    columns=["Name", "Team", "Position"],
    parent=self
)
# Add 500 rows...
```

All three use the same component, just with different:
- Titles (parameter)
- Columns (parameter)
- Data (method calls)
- Dimensions (handled by grid layout)

**No need for custom components!**

