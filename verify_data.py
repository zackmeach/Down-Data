import polars as pl
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Load data
impacts = pl.read_parquet('data/nflverse/player_impacts.parquet')
seasons = pl.read_parquet('data/nflverse/player_seasons.parquet')
players = pl.read_parquet('data/nflverse/players.parquet')

# Find Josh Allen QB
allen = players.filter(pl.col('display_name').str.contains('Josh Allen') & (pl.col('position') == 'QB'))
if allen.height > 0:
    pid = allen[0, 'player_id']
    print('=== Josh Allen (QB) ===')
    print(f'Player ID: {pid}')
    
    # Get seasons
    allen_seasons = seasons.filter(pl.col('player_id') == pid)
    print('\n=== Seasons ===')
    for row in allen_seasons.sort('season', descending=True).iter_rows(named=True):
        print(f"Season {row['season']}: GP={row['games_played']}, Snaps={row['snaps_total']}, PassYds={int(row['passing_yards'] or 0)}, PassTD={int(row['passing_tds'] or 0)}, INT={int(row['passing_ints'] or 0)}, RushYds={int(row['rushing_yards'] or 0)}")
    
    # Get impacts
    allen_impacts = impacts.filter(pl.col('player_id') == pid)
    print('\n=== Impacts ===')
    for row in allen_impacts.sort('season', descending=True).iter_rows(named=True):
        wpa = f"{row['qb_wpa']:.3f}" if row['qb_wpa'] is not None else 'None'
        epa = f"{row['qb_epa']:.1f}" if row['qb_epa'] is not None else 'None'
        print(f"Season {row['season']}: qb_epa={epa}, qb_wpa={wpa}")

# Check Cam Akers
print('\n\n=== Cam Akers (RB) ===')
akers = players.filter(pl.col('display_name').str.contains('Cam Akers'))
if akers.height > 0:
    pid = akers[0, 'player_id']
    print(f'Player ID: {pid}')
    akers_seasons = seasons.filter(pl.col('player_id') == pid)
    print('\n=== Seasons ===')
    for row in akers_seasons.sort('season', descending=True).iter_rows(named=True):
        print(f"Season {row['season']}: Team={row['team']}, GP={row['games_played']}, Snaps={row['snaps_total']}, RushYds={int(row['rushing_yards'] or 0)}")
    
    akers_impacts = impacts.filter(pl.col('player_id') == pid)
    print('\n=== Impacts ===')
    for row in akers_impacts.sort('season', descending=True).iter_rows(named=True):
        rush_20 = int(row.get('skill_rush_20_plus') or 0)
        rec_20 = int(row.get('skill_rec_20_plus') or 0)
        first_dn = int(row.get('skill_rec_first_downs') or 0)
        print(f"Season {row['season']}: rush_20+={rush_20}, rec_20+={rec_20}, first_downs={first_dn}")

# Check Rashod Bateman
print('\n\n=== Rashod Bateman (WR) ===')
bateman = players.filter(pl.col('display_name').str.contains('Rashod Bateman'))
if bateman.height > 0:
    pid = bateman[0, 'player_id']
    print(f'Player ID: {pid}')
    bateman_seasons = seasons.filter(pl.col('player_id') == pid)
    print('\n=== Seasons ===')
    for row in bateman_seasons.sort('season', descending=True).iter_rows(named=True):
        print(f"Season {row['season']}: Team={row['team']}, GP={row['games_played']}, Snaps={row['snaps_total']}, RecYds={int(row['receiving_yards'] or 0)}")
    
    bateman_impacts = impacts.filter(pl.col('player_id') == pid)
    print('\n=== Impacts ===')
    for row in bateman_impacts.sort('season', descending=True).iter_rows(named=True):
        rec_20 = int(row.get('skill_rec_20_plus') or 0)
        first_dn = int(row.get('skill_rec_first_downs') or 0)
        print(f"Season {row['season']}: rec_20+={rec_20}, first_downs={first_dn}")

