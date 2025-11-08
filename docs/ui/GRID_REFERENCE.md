# Grid Location Reference Guide

## How to Refer to Grid Locations

You have **multiple flexible options** for referring to grid locations:

### Option 1: Column/Row Notation (Recommended for Communication)

Use natural language with column and row numbers:

```
"Put the search box at column 2, row 5"
"Place the button at col 10, row 3"
"Move it to C4R8" (shorthand)
```

**Format:**
- Columns: `col`, `column`, `C` + number (0-11)
- Rows: `row`, `R` + number (0-23)

### Option 2: XY Coordinates

You can think of it like XY coordinates where:
- **X = Column** (0-11, left to right)
- **Y = Row** (0-23, top to bottom)

```
"Place widget at (2, 5)" → column 2, row 5
"Move to X=10, Y=3" → column 10, row 3
```

### Option 3: Grid Cell Objects (In Code)

When I implement it, I use:

```python
GridCell(col=2, row=5)              # Single cell
GridCell(col=2, row=5, col_span=3, row_span=2)  # Spanning multiple cells
```

### Option 4: Range Notation (For Spanning)

When you want something to span multiple cells:

```
"Columns 2-9, Row 5" → starts at col 2, spans to col 9 (8 columns)
"Col 0, Rows 10-15" → starts at row 10, spans to row 15 (6 rows)
"C2-9 R5-7" → spans columns 2-9, rows 5-7
```

I'll translate this to:
```python
GridCell(col=2, row=5, col_span=8, row_span=3)
```

## Grid Layout

```
      C0  C1  C2  C3  C4  C5  C6  C7  C8  C9  C10 C11
    ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
R0  │   │   │   │   │   │   │   │   │   │   │   │   │
R1  │   │   │   │   │   │   │   │   │   │   │   │   │
R2  │   │   │   │   │   │   │   │   │   │   │   │   │
R3  │   │   │   │   │   │   │   │   │   │   │   │   │
R4  │   │   │   │   │   │   │   │   │   │   │   │   │
R5  │   │   │   │   │   │   │   │   │   │   │   │   │
...
R23 │   │   │   │   │   │   │   │   │   │   │   │   │
    └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
```

## Examples of Natural Language Requests

All of these work:

### Single Cell
- "Put the button at C5R10"
- "Place it at column 5, row 10"
- "Move to (5, 10)"
- "Position at col 5, row 10"

### Spanning Cells
- "Search bar at C2-9 R1" → 8 columns wide, 1 row tall
- "Sidebar from columns 0-1, rows 0-23" → 2 columns, full height
- "Make it span columns 3 to 8, row 5" → 6 columns wide
- "Put the panel at C2R5, spanning 4 columns and 3 rows"

### Common Patterns
- "Full width at row 0" → C0-11 R0 (12 columns, 1 row)
- "Left half" → C0-5 (columns 0-5)
- "Right half" → C6-11 (columns 6-11)
- "Top quarter" → R0-5 (rows 0-5)
- "Bottom half" → R12-23 (rows 12-23)
- "Center 8 columns" → C2-9 (columns 2-9)

## Quick Tips

1. **Columns are 0-11** (12 total, left to right)
2. **Rows are 0-23** (24 total, top to bottom)
3. **Use any format that's comfortable** - I'll understand
4. **Think in terms of what feels natural:**
   - Want a header? "Full width, top row" or "C0-11 R0"
   - Want a sidebar? "Columns 0-1, full height" or "C0-1 R0-23"
   - Want centered content? "Columns 2-9, rows 5-18"

## Visual Zones (For Quick Reference)

```
┌─────────────────────────────────────────────┐
│              TOP (R0-2)                     │
├──────┬──────────────────────────────┬───────┤
│      │                              │       │
│ LEFT │         CENTER               │ RIGHT │
│(C0-2)│        (C3-8)                │(C9-11)│
│      │                              │       │
├──────┴──────────────────────────────┴───────┤
│            BOTTOM (R21-23)                  │
└─────────────────────────────────────────────┘
```

**Just tell me where you want things in plain English, and I'll translate it to the grid!** 🎯

