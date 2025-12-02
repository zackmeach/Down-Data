# NFL Data Store

This directory contains the structured NFL player database. Unlike the legacy
cache files in `../cache/`, this data store is designed to be:

- **Persistent** – Not temporary; intentionally stored and managed
- **Structured** – Well-defined schemas with known features
- **Updateable** – Can expand date ranges and add new features
- **Efficient** – Static data (birthplace) stored separately from dynamic data (stats)

## Files

| File | Description |
|------|-------------|
| `players.parquet` | Static player info: bio, birthplace, college, draft, physical attributes |
| `player_seasons.parquet` | Season-level statistics: games, snaps, passing/rushing/receiving/defensive stats |
| `player_impacts.parquet` | EPA/WPA metrics by player-season from play-by-play analysis |
| `metadata.json` | Schema version, date ranges, update timestamps, error log |

## Building the Data Store

```bash
# Full build (1999-2024, may take 30+ minutes)
python scripts/build_nfl_datastore.py

# Update with new season data
python scripts/build_nfl_datastore.py --seasons 2024 2025

# Quick build without bio scraping (faster)
python scripts/build_nfl_datastore.py --skip-bio

# Quick build without impacts (fastest)
python scripts/build_nfl_datastore.py --skip-bio --skip-impacts

# Force complete rebuild
python scripts/build_nfl_datastore.py --force

# Check status
python scripts/build_nfl_datastore.py --status
```

## Schema Details

### players.parquet

Static/slowly-changing player attributes. Bio fields are fetched once and
don't need to be re-fetched yearly.

Key columns:
- `player_id` (gsis_id) - Primary key
- `pfr_id`, `espn_id`, etc. - Cross-reference IDs
- `full_name`, `birth_date`, `college`
- `birth_city`, `birth_state`, `birth_country` - Static bio
- `handedness` - Throwing/batting hand
- `draft_year`, `draft_round`, `draft_pick`, `draft_team`
- `position`, `position_group`
- `height`, `weight`

### player_seasons.parquet

Season-level statistics. Each row represents one player's performance in one season.

Key columns:
- `player_id`, `season` - Composite key
- `team`, `position`, `games_played`
- `offense_snaps`, `defense_snaps`, `special_teams_snaps`, `snaps_total`
- Passing: `pass_completions`, `pass_attempts`, `passing_yards`, `passing_tds`, `passing_ints`
- Rushing: `rushing_attempts`, `rushing_yards`, `rushing_tds`
- Receiving: `receiving_targets`, `receiving_receptions`, `receiving_yards`, `receiving_tds`
- Defensive: `def_tackles_solo`, `def_sacks`, `def_interceptions`, `def_tds`, etc.
- Special teams: kicking, punting stats

### player_impacts.parquet

EPA (Expected Points Added) and WPA (Win Probability Added) metrics computed
from play-by-play data.

Key columns:
- `player_id`, `season` - Composite key
- QB: `qb_epa`, `qb_wpa`
- Skill: `skill_epa`, `skill_wpa`, `skill_rush_20_plus`, `skill_rec_20_plus`
- Defense: `def_epa`, `def_wpa`
- O-Line: `ol_epa`, `ol_wpa`
- Kicker: `kicker_epa`, `kicker_wpa`
- Punter: `punter_epa`, `punter_wpa`

## Using the Data Store

```python
from down_data.data.nfl_datastore import NFLDataStore, get_default_store

# Get the default store instance
store = get_default_store()

# Query a player
player = store.get_player("00-0033873")  # Patrick Mahomes

# Get season stats
seasons = store.get_player_seasons("00-0033873", seasons=[2023, 2024])

# Get combined summary data
summary = store.get_player_summary("00-0033873")

# Or use the repository layer
from down_data.backend import NFLDataRepository

repo = NFLDataRepository()
summary = repo.get_player_summary("00-0033873")
bio = repo.get_player_bio("00-0033873")
```

## Error Handling

The data store tracks errors during refresh operations in `metadata.json`.
Check the status to see if there are unresolved errors:

```bash
python scripts/build_nfl_datastore.py --status
```

Errors are logged with timestamps and can be reviewed/resolved in the metadata file.
