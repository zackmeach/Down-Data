# NFLverse Data Availability Reference

## Overview

This document defines the scope and limitations of data available through the nflverse dataset, which powers this Player Explorer tool.

## Data Type Breakdown

### 1. Player Profile Data
**Availability**: ✅ **All NFL players from all eras**

**Includes**:
- Full name, birth date, college
- Physical attributes (height, weight)
- Draft information (year, round, pick, team)
- Position and position group
- Cross-platform IDs:
  - nflverse/GSIS ID
  - Pro Football Reference (PFR) ID
  - Pro Football Focus (PFF) ID
  - ESPN ID
  - Sportradar ID
  - ESB ID
  - Over The Cap (OTC) ID

**Examples**:
```python
# Historical players - profile data available
player = Player(name="Walter Payton")    # Played 1975-1987
player = Player(name="Dan Marino")       # Played 1983-1999
player = Player(name="Jerry Rice")       # Played 1985-2004

# All profile data accessible via player.info()
print(player.profile.full_name)
print(player.profile.draft_year)
print(player.profile.college)
```

### 2. Weekly/Seasonal Statistics
**Availability**: ⚠️ **Only seasons 1999-2025**

The nflverse dataset only includes detailed weekly player statistics starting from the 1999 NFL season. This is a fundamental limitation of the upstream data source.

**Includes** (for 1999+ seasons):
- **Offensive Stats**: 
  - Passing: yards, TDs, interceptions, completions, attempts, EPA, etc.
  - Rushing: yards, TDs, attempts, fumbles, etc.
  - Receiving: yards, TDs, receptions, targets, target share, air yards, etc.
  - Fantasy points (standard and PPR)
  - Advanced metrics: Expected Points Added (EPA), success rate, YAC, completion % above expectation

- **Defensive Stats**:
  - Tackles (solo, assisted, for loss)
  - Sacks, QB hits, hurries, pressures
  - Interceptions, passes defended
  - Forced fumbles, fumble recoveries
  - Defensive TDs, safeties

- **Special Teams Stats**:
  - Kicking, punting, returns

### 3. NFL NextGen Stats (Advanced Tracking Metrics)
**Availability**: ⚠️ **Only seasons 2016-2025**

NextGen Stats are NFL's official advanced tracking metrics that use player tracking data. These provide deeper insights into player performance that go beyond traditional box score stats.

**Includes** (for 2016+ seasons):
- **Passing NextGen Stats**:
  - Average time to throw
  - Average completed/intended air yards
  - Air yards differential
  - Maximum completed air distance
  - Aggressiveness rating
  - Completion percentage above expectation

- **Rushing NextGen Stats**:
  - Efficiency rating
  - Rush yards over expected
  - Rush yards over expected per attempt
  - Percent of rush attempts against 8+ defenders
  - Average time to line of scrimmage

- **Receiving NextGen Stats**:
  - Average separation from defender
  - Average cushion at snap
  - Catch percentage
  - Average yards after catch (YAC)
  - Expected YAC and YAC above expectation
  - Percent share of team's intended air yards

**What This Means**:

| Player Career Span | Profile Data | Stats Data |
|-------------------|-------------|------------|
| Retired before 1999 (e.g., Dan Marino 1983-1999) | ✅ Full profile | ⚠️ Only 1999 stats available |
| Retired before 1999 (e.g., Walter Payton 1975-1987) | ✅ Full profile | ❌ No stats available |
| Active in 1999+ (e.g., Tom Brady 2000-2023) | ✅ Full profile | ✅ Full career stats |
| Current players (e.g., Patrick Mahomes 2017-present) | ✅ Full profile | ✅ Full career stats |

## API Behavior

### Season Validation

The `Player` class now includes automatic season validation:

```python
from player import Player, SeasonNotAvailableError

player = Player(name="Dan Marino")

# This works - profile data is always available
profile = player.info()

# This raises SeasonNotAvailableError - 1990 is before 1999
try:
    stats = player.fetch_stats(seasons=[1990])
except SeasonNotAvailableError as e:
    print(e)  # Explains the limitation and available range

# This works - 1999 is within available range
stats = player.fetch_stats(seasons=[1999])
```

### Validation Method

You can check season validity before making requests:

```python
from player import Player

# Returns (valid_seasons, invalid_seasons)
valid, invalid = Player.validate_seasons([1990, 1999, 2000, 2023, 2030])

print(valid)    # [1999, 2000, 2023]
print(invalid)  # [1990, 2030]
```

### NextGen Stats Access

Access NFL's official advanced tracking metrics (2016+ only):

```python
from player import Player

player = Player(name="Patrick Mahomes")

# Fetch NextGen passing stats
nextgen_stats = player.fetch_nextgen_stats(seasons=[2023], stat_type="passing")

# Access advanced metrics
print(nextgen_stats["avg_time_to_throw"].mean())  # Average time to throw
print(nextgen_stats["aggressiveness"].mean())      # Aggressiveness rating
print(nextgen_stats["completion_percentage_above_expectation"].mean())

# Auto-detect stat type based on position
stat_type = player.get_nextgen_stat_type()  # Returns "passing" for QBs

# Fetch for running back
rb = Player(name="Derrick Henry")
rush_stats = rb.fetch_nextgen_stats(seasons=[2023], stat_type="rushing")
print(rush_stats["rush_yards_over_expected"].sum())  # Yards over expected
print(rush_stats["efficiency"].mean())                # Efficiency rating

# Fetch for wide receiver
wr = Player(name="Tyreek Hill")
rec_stats = wr.fetch_nextgen_stats(seasons=[2023], stat_type="receiving")
print(rec_stats["avg_separation"].mean())  # Average separation from defender
print(rec_stats["avg_cushion"].mean())     # Average cushion at snap
```

### Constants

The available range is defined as constants you can import:

```python
from player import EARLIEST_SEASON_AVAILABLE, LATEST_SEASON_AVAILABLE, EARLIEST_NEXTGEN_SEASON

print(f"Basic stats available from {EARLIEST_SEASON_AVAILABLE} to {LATEST_SEASON_AVAILABLE}")
# Output: Basic stats available from 1999 to 2025

print(f"NextGen stats available from {EARLIEST_NEXTGEN_SEASON} to {LATEST_SEASON_AVAILABLE}")
# Output: NextGen stats available from 2016 to 2025
```

## CLI User Experience

When using the interactive CLI (`main.py`), users are now:

1. **Informed upfront**: A note displays the available range when requesting stats
2. **Protected from errors**: Invalid seasons trigger clear error messages
3. **Guided with helpful context**: Error messages explain why data is unavailable and what range is supported

Example CLI flow:
```
Player name: Dan Marino
[Player profile displays successfully]

Fetch stats for specific seasons now? [y/n]: y
Note: Stats data is only available from 1999 to 2025

Enter comma separated seasons: 1990
[red]The following seasons are not available in nflverse data: [1990].
Player stats are only available from 1999 to 2025.
Player profile data (name, draft info, etc.) is available for all players,
but weekly/seasonal stats are limited to 1999 onwards.[/red]
```

## Technical Details

### Why 1999?

The nflverse project aggregates data from various sources. Comprehensive, consistent weekly player statistics with modern tracking began to be systematically recorded and made publicly available starting with the 1999 season. Prior seasons either:
- Lack complete week-by-week data
- Have inconsistent stat tracking methodologies
- Are not available in digital formats suitable for the nflverse data structure

### Upstream Data Source

The data comes from the nflverse project's data repository:
- Repository: https://github.com/nflverse/nflverse-data
- File format: Parquet files per season
- URL pattern: `stats_player_week_{YEAR}.parquet`
- Available range: 1999-2025

### Future Updates

As new NFL seasons are played, the `LATEST_SEASON_AVAILABLE` constant should be updated to reflect the current season.

## Recommendations for Historical Player Analysis

If you need statistics for players who retired before 1999:

1. **Use profile data**: Draft position, college, physical attributes are all available
2. **External sources**: Consider Pro Football Reference or other historical stat databases
3. **Career summaries**: Many players have career totals documented on sports reference sites
4. **PFR ID**: This tool provides the `pfr_id` which you can use to construct Pro Football Reference URLs:
   ```python
   player = Player(name="Walter Payton")
   pfr_url = f"https://www.pro-football-reference.com/players/{player.profile.pfr_id[0]}/{player.profile.pfr_id}.htm"
   ```

## Summary

- ✅ **Player profiles**: Available for all NFL players, all eras
- ⚠️ **Basic weekly stats**: Available 1999-2025
  - Traditional box score stats (yards, TDs, turnovers, etc.)
  - EPA, success rate, fantasy points
- ⚠️ **NextGen Stats**: Available 2016-2025
  - NFL's official advanced tracking metrics
  - Passing: time to throw, air yards, completion % above expectation
  - Rushing: yards over expected, efficiency
  - Receiving: separation, cushion, YAC above expectation
- ✅ **Error handling**: Clear messages guide users when requesting unavailable data
- ✅ **Validation**: Built-in methods to check season availability before requests
- ✅ **Position-aware**: Auto-detects appropriate stat types based on player position
- ✅ **Documentation**: Comprehensive guidance in README and this reference document

