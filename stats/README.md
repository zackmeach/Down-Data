# NFL Statistics Calculation Modules

This directory contains specialized statistical calculation functions for NFL data analysis, organized by position group and statistic type.

## Structure

```
stats/
├── __init__.py           # Package exports and imports
├── passing.py            # QB and passing efficiency metrics
├── rushing.py            # RB and rushing efficiency metrics
├── receiving.py          # WR/TE receiving and route metrics
├── specialteams.py       # Kicking, punting, and return metrics
└── README.md            # This file
```

## Design Philosophy

All functions in this package follow these principles:

1. **Pure functions**: Accept a Polars DataFrame, return a Polars DataFrame
2. **Additive**: Add new columns without removing existing ones
3. **Graceful degradation**: Check for required columns before calculating
4. **No side effects**: No object modification, no global state
5. **Composable**: Can be chained using `.pipe()` or combined

## Usage

### Direct Function Calls

```python
import polars as pl
from player import Player
from stats.passing import calculate_passer_rating, calculate_adjusted_yards_per_attempt

# Fetch player data
player = Player(name="Patrick Mahomes")
stats = player.fetch_stats(seasons=[2023])

# Apply calculations
stats_with_rating = calculate_passer_rating(stats)
stats_enhanced = calculate_adjusted_yards_per_attempt(stats_with_rating)

# Or chain with pipe
result = (stats
    .pipe(calculate_passer_rating)
    .pipe(calculate_adjusted_yards_per_attempt))
```

### Using StatsEngine (Recommended)

```python
from player import Player
from stats_engine import StatsEngine

player = Player(name="Justin Jefferson")
engine = StatsEngine()

# Calculate specific stat
yprr = engine.calculate(player, "yards_per_route_run", seasons=[2023])

# Calculate all relevant stats for position
all_stats = engine.calculate_all(player, seasons=[2023])

# List available stats for position
available = engine.list_available_stats(position="WR")
print(available)
```

## Data Source Requirements

Different statistics require different data sources:

| Data Source | Description | Availability |
|-------------|-------------|--------------|
| `basic` | nflverse player_stats | 1999-present |
| `nextgen` | NFL NextGen Stats | 2016-present |
| `pfr_advanced` | Pro Football Reference Advanced | 2018-2024 (local CSV) |
| `pbp` | Play-by-play data | 1999-present |
| `pff` | Pro Football Focus grades | Requires subscription |

## Adding New Statistics

To add a new statistic:

1. **Create the calculation function** in the appropriate module:

```python
# stats/passing.py
def calculate_my_custom_stat(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate my custom passing metric.
    
    Args:
        df: DataFrame with required columns
    
    Returns:
        DataFrame with added column: my_custom_stat
    """
    if "required_column" not in df.columns:
        return df
    
    return df.with_columns([
        (pl.col("numerator") / pl.col("denominator")).alias("my_custom_stat")
    ])
```

2. **Export it** in `__init__.py`:

```python
from stats.passing import calculate_my_custom_stat

__all__ = [
    # ... existing exports
    "calculate_my_custom_stat",
]
```

3. **Register it** in `stats_engine.py`:

```python
STAT_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ... existing stats
    "my_custom_stat": {
        "module": "stats.passing",
        "function": "calculate_my_custom_stat",
        "data_source": "basic",  # or "nextgen", "pfr_advanced", etc.
        "positions": ["QB"],
    },
}
```

4. **Test it**:

```python
from player import Player
from stats_engine import StatsEngine

player = Player(name="Josh Allen")
engine = StatsEngine()
result = engine.calculate(player, "my_custom_stat", seasons=[2023])
print(result)
```

## Examples by Position

### Quarterback (QB)

```python
from stats.passing import (
    calculate_passer_rating,
    calculate_adjusted_yards_per_attempt,
    calculate_air_yards_metrics,
    calculate_pressure_adjusted_stats,
)

qb = Player(name="Josh Allen")
stats = qb.fetch_stats(seasons=[2023])
pfr_data = qb.fetch_pfr_advanced_stats(seasons=[2023], stat_type="passing")

# Basic efficiency
stats = calculate_passer_rating(stats)
stats = calculate_adjusted_yards_per_attempt(stats)

# Advanced metrics (requires PFR data)
pfr_data = calculate_air_yards_metrics(pfr_data)
pfr_data = calculate_pressure_adjusted_stats(pfr_data)
```

### Running Back (RB)

```python
from stats.rushing import (
    calculate_yards_per_carry,
    calculate_yards_after_contact_rate,
    calculate_rushing_efficiency,
)

rb = Player(name="Christian McCaffrey")
stats = rb.fetch_stats(seasons=[2023])
pfr_data = rb.fetch_pfr_advanced_stats(seasons=[2023], stat_type="rushing")

stats = calculate_yards_per_carry(stats)
pfr_data = calculate_yards_after_contact_rate(pfr_data)
```

### Wide Receiver (WR)

```python
from stats.receiving import (
    calculate_yards_per_route_run,
    calculate_catch_rate_above_expectation,
    calculate_separation_metrics,
)

wr = Player(name="CeeDee Lamb")
nextgen = wr.fetch_nextgen_stats(seasons=[2023], stat_type="receiving")

nextgen = calculate_separation_metrics(nextgen)
nextgen = calculate_catch_rate_above_expectation(nextgen)
```

## Chaining Multiple Calculations

```python
# Functional style with pipe
result = (player.fetch_stats(seasons=[2023])
    .pipe(calculate_yards_per_carry)
    .pipe(calculate_success_rate)
    .pipe(calculate_explosive_play_rate))

# Or apply multiple functions in sequence
from stats.rushing import (
    calculate_yards_per_carry,
    calculate_yards_after_contact_rate,
    calculate_rushing_efficiency,
)

pfr_data = player.fetch_pfr_advanced_stats(seasons=[2023], stat_type="rushing")
result = calculate_yards_per_carry(pfr_data)
result = calculate_yards_after_contact_rate(result)
result = calculate_rushing_efficiency(result)
```

## Best Practices

1. **Check for required columns** before calculating
2. **Handle missing data gracefully** (use `.fill_null()`)
3. **Use descriptive column names** for calculated stats
4. **Document data source requirements** in docstrings
5. **Return original DataFrame if data unavailable** (don't raise errors)
6. **Use Polars expressions** for performance
7. **Keep functions focused** - one metric per function

## Testing

```python
# Test basic calculation
import polars as pl
from stats.passing import calculate_passer_rating

# Create sample data
df = pl.DataFrame({
    "attempts": [30],
    "completions": [20],
    "passing_yards": [250],
    "passing_tds": [2],
    "interceptions": [1],
})

result = calculate_passer_rating(df)
assert "passer_rating" in result.columns
print(result["passer_rating"])
```

